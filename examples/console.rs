//! Console-based actor management REPL with MetaActor supervision.
//!
//! Architecture:
//!   main ──spawn──> MetaActor ──spawn──> CommandActor
//!                       │                    │
//!                       │  do_send / notify  │
//!                       └────────────────────┘
//!                       │
//!                   ctx.spawn
//!                       ▼
//!                  WorkerActor(s)
//!
//! MetaActor is the root: it auto-spawns CommandActor on startup and
//! manages all worker lifecycle. Communication uses fire-and-forget
//! (do_send / notify) to avoid same-worker deadlocks. Responses come
//! back asynchronously via CmdResponse messages.
//!
//! Commands:
//!   spawn <name>     Create a worker
//!   stop <id>        Stop a worker
//!   send <id> <msg>  Send message to a worker
//!   status           Show all workers
//!   list             List all workers
//!   help             Show help
//!   quit             Shutdown

use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use actinium_actor::{
    Actor, ActorId, Addr, Context, Handler, Message, Runtime, StateHandler, StateMachine,
};

// ── Worker States ───────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkerState {
    Idle,
    Receiving,
    Processing,
}

// ── Worker Actor ────────────────────────────────────────────

#[derive(Debug, Clone)]
struct WorkerInfo {
    name: String,
    msg_count: usize,
    last_msg: String,
    current_state: String,
}

type WorkerSM = StateMachine<WorkerActor, WorkerMessage, WorkerState>;

struct WorkerActor {
    info: Arc<Mutex<WorkerInfo>>,
    log: Arc<Mutex<Vec<String>>>,
}

impl Actor for WorkerActor {
    fn started(&mut self, ctx: &mut Context) {
        let name = self.info.lock().unwrap().name.clone();
        self.log.lock().unwrap().push(format!("[{}] Worker '{}' started", ctx.id(), name));
    }
    fn stopped(&mut self, ctx: &mut Context) {
        let name = self.info.lock().unwrap().name.clone();
        self.log.lock().unwrap().push(format!("[{}] Worker '{}' stopped", ctx.id(), name));
    }
}

struct WorkerMessage { text: String }

impl Message for WorkerMessage { type Result = (); }

impl StateHandler<WorkerMessage, WorkerState> for WorkerActor {
    fn initial_state() -> WorkerState { WorkerState::Idle }
    fn state_sequence() -> Vec<WorkerState> {
        vec![WorkerState::Idle, WorkerState::Receiving, WorkerState::Processing]
    }
    fn handle_in_state(
        &mut self, _idx: usize, state: &WorkerState, msg: WorkerMessage, ctx: &mut Context,
    ) {
        let mut info = self.info.lock().unwrap();
        info.msg_count += 1;
        info.last_msg = msg.text.clone();
        info.current_state = format!("{:?}", state);
        self.log.lock().unwrap().push(format!(
            "[{}] '{}' received in {:?} (#{}): {}",
            ctx.id(), info.name, state, info.msg_count, msg.text
        ));
    }
}

// ── MetaActor ───────────────────────────────────────────────

struct ManagedWorker {
    addr: Addr<WorkerSM>,
    info: Arc<Mutex<WorkerInfo>>,
    name: String,
}

/// Fire-and-forget commands from CommandActor to MetaActor.
enum MetaCommand {
    CreateWorker { name: String },
    DestroyWorker { id: ActorId },
    SendToWorker { id: ActorId, text: String },
    GetStatus,
    GetList,
}

impl Message for MetaCommand { type Result = (); }

struct MetaActor {
    workers: HashMap<ActorId, ManagedWorker>,
    cmd_addr: Option<Addr<CommandActor>>,
    /// Shared holder so main thread can get CommandActor's Addr after bootstrap.
    cmd_addr_holder: Arc<Mutex<Option<Addr<CommandActor>>>>,
    log: Arc<Mutex<Vec<String>>>,
}

impl Actor for MetaActor {
    fn started(&mut self, ctx: &mut Context) {
        self.log.lock().unwrap().push("[MetaActor] started".into());

        // Spawn CommandActor — MetaActor is responsible for its lifecycle
        let response_cell: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let cmd = CommandActor {
            meta: None,
            log: self.log.clone(),
            last_response: response_cell.clone(),
        };
        let cmd_addr = ctx.spawn(cmd);

        // Tell CommandActor about MetaActor
        ctx.notify(&cmd_addr, InitCommand { meta_addr: cmd_addr.id() });
        // Actually, we need to give CommandActor MetaActor's Addr.
        // Since we can't clone our own Addr in started, use a different approach:
        // CommandActor gets MetaActor's Addr via the shared holder.
        // MetaActor stores CommandActor's Addr for direct notify.

        self.cmd_addr = Some(cmd_addr.clone());

        // Share CommandActor's Addr with main thread
        *self.cmd_addr_holder.lock().unwrap() = Some(cmd_addr);

        println!("=== Actinium-Actor Console ===");
        println!("[MetaActor] spawned CommandActor");
        println!("Type 'help' for available commands.\n");
    }

    fn stopped(&mut self, _ctx: &mut Context) {
        self.log.lock().unwrap().push("[MetaActor] stopped".into());
    }
}

impl Handler<MetaCommand> for MetaActor {
    fn handle(&mut self, cmd: MetaCommand, ctx: &mut Context) {
        match cmd {
            MetaCommand::CreateWorker { name } => {
                let info = Arc::new(Mutex::new(WorkerInfo {
                    name: name.clone(), msg_count: 0, last_msg: String::new(),
                    current_state: "Idle".into(),
                }));
                let sm = StateMachine::new(WorkerActor { info: info.clone(), log: self.log.clone() });
                let addr = ctx.spawn(sm);
                let id = addr.id();
                self.workers.insert(id, ManagedWorker { addr, info, name: name.clone() });
                self.notify_cmd(ctx, CmdResponse::Spawned { id, name });
            }
            MetaCommand::DestroyWorker { id } => {
                if let Some(w) = self.workers.remove(&id) {
                    let info = w.info.lock().unwrap();
                    self.notify_cmd(ctx, CmdResponse::Stopped {
                        id, name: w.name.clone(),
                        msg_count: info.msg_count, state: info.current_state.clone(),
                    });
                } else {
                    self.notify_cmd(ctx, CmdResponse::Error(format!("Worker {} not found", id)));
                }
            }
            MetaCommand::GetStatus => {
                let snapshots: Vec<WorkerSnapshot> = self.workers.iter().map(|(id, w)| {
                    let info = w.info.lock().unwrap();
                    WorkerSnapshot {
                        id: *id, name: w.name.clone(), msg_count: info.msg_count,
                        state: info.current_state.clone(), last_msg: info.last_msg.clone(),
                    }
                }).collect();
                self.notify_cmd(ctx, CmdResponse::Status(snapshots));
            }
            MetaCommand::GetList => {
                let list: Vec<_> = self.workers.iter()
                    .map(|(id, w)| (*id, w.name.clone())).collect();
                self.notify_cmd(ctx, CmdResponse::List(list));
            }
            MetaCommand::SendToWorker { id, text } => {
                if let Some(w) = self.workers.get(&id) {
                    let _ = w.addr.do_send(WorkerMessage { text });
                    self.notify_cmd(ctx, CmdResponse::MessageSent { id, name: w.name.clone() });
                } else {
                    self.notify_cmd(ctx, CmdResponse::Error(format!("Worker {} not found", id)));
                }
            }
        }
    }
}

impl MetaActor {
    fn notify_cmd(&self, ctx: &mut Context, response: CmdResponse) {
        if let Some(ref cmd_addr) = self.cmd_addr {
            ctx.notify(cmd_addr, response);
        }
    }
}

// ── CommandActor ────────────────────────────────────────────

#[derive(Debug, Clone)]
struct WorkerSnapshot {
    id: ActorId, name: String, msg_count: usize, state: String, last_msg: String,
}

/// Async response from MetaActor back to CommandActor.
enum CmdResponse {
    Spawned { id: ActorId, name: String },
    Stopped { id: ActorId, name: String, msg_count: usize, state: String },
    Status(Vec<WorkerSnapshot>),
    List(Vec<(ActorId, String)>),
    MessageSent { id: ActorId, name: String },
    Error(String),
}

impl Message for CmdResponse { type Result = (); }

/// Bootstrap: MetaActor sends this to CommandActor with its own Addr.
struct InitCommand { meta_addr: ActorId }

impl Message for InitCommand { type Result = (); }

/// Commands from stdin thread.
enum ConsoleCommand {
    Exec { input: String },
    Poll,
    SetMeta { meta_addr: Addr<MetaActor> },
    Quit,
}

impl Message for ConsoleCommand { type Result = ConsoleResult; }

enum ConsoleResult { Ok(String), Quit }

struct CommandActor {
    meta: Option<Addr<MetaActor>>,
    log: Arc<Mutex<Vec<String>>>,
    last_response: Arc<Mutex<Option<String>>>,
}

impl Actor for CommandActor {
    fn started(&mut self, ctx: &mut Context) {
        self.log.lock().unwrap().push(format!("[CommandActor] started ({})", ctx.id()));
    }
    fn stopped(&mut self, ctx: &mut Context) {
        self.log.lock().unwrap().push(format!("[CommandActor] stopped ({})", ctx.id()));
    }
}

impl Handler<InitCommand> for CommandActor {
    fn handle(&mut self, msg: InitCommand, _ctx: &mut Context) {
        self.log.lock().unwrap().push(format!(
            "[CommandActor] meta addr: {}", msg.meta_addr
        ));
    }
}

impl Handler<CmdResponse> for CommandActor {
    fn handle(&mut self, msg: CmdResponse, _ctx: &mut Context) {
        let text = match msg {
            CmdResponse::Spawned { id, name } =>
                format!("Spawned '{}' with ID {}", name, id),
            CmdResponse::Stopped { id, name, msg_count, state } =>
                format!("Stopped '{}' ({}) — {} msg(s), last state: {}", name, id, msg_count, state),
            CmdResponse::Status(snapshots) => {
                if snapshots.is_empty() {
                    "No actors running.".into()
                } else {
                    let lines: Vec<String> = snapshots.iter().map(|s|
                        format!("  {} '{}' — {} msg(s), state: {}, last: \"{}\"",
                            s.id, s.name, s.msg_count, s.state, s.last_msg)
                    ).collect();
                    format!("{} actor(s) running:\n{}", snapshots.len(), lines.join("\n"))
                }
            }
            CmdResponse::List(list) => {
                if list.is_empty() {
                    "No actors.".into()
                } else {
                    let ids: Vec<String> = list.iter()
                        .map(|(id, name)| format!("{} ({})", name, id)).collect();
                    format!("Actors: {}", ids.join(", "))
                }
            }
            CmdResponse::MessageSent { id, name } =>
                format!("Message sent to '{}' ({})", name, id),
            CmdResponse::Error(e) => format!("Error: {}", e),
        };
        *self.last_response.lock().unwrap() = Some(text);
    }
}

impl Handler<ConsoleCommand> for CommandActor {
    fn handle(&mut self, cmd: ConsoleCommand, ctx: &mut Context) -> ConsoleResult {
        match cmd {
            ConsoleCommand::Exec { input } => {
                let parts: Vec<&str> = input.splitn(3, ' ').collect();
                let command = parts[0].to_lowercase();

                match command.as_str() {
                    "help" => {
                        *self.last_response.lock().unwrap() = Some(
                            "spawn <name> | stop <id> | send <id> <msg> | status | list | help | quit".into()
                        );
                    }
                    "spawn" => {
                        if parts.len() >= 2 && !parts[1].is_empty() {
                            if let Some(ref meta) = self.meta {
                                ctx.notify(meta, MetaCommand::CreateWorker {
                                    name: parts[1].to_string(),
                                });
                            }
                        } else {
                            *self.last_response.lock().unwrap() = Some("Usage: spawn <name>".into());
                        }
                    }
                    "stop" => {
                        if parts.len() >= 2 {
                            if let Ok(raw_id) = parts[1].parse::<u64>() {
                                if let Some(ref meta) = self.meta {
                                    ctx.notify(meta, MetaCommand::DestroyWorker {
                                        id: ActorId::from_raw(raw_id),
                                    });
                                }
                            } else {
                                *self.last_response.lock().unwrap() = Some("Invalid ID".into());
                            }
                        } else {
                            *self.last_response.lock().unwrap() = Some("Usage: stop <id>".into());
                        }
                    }
                    "send" => {
                        if parts.len() >= 3 {
                            if let Ok(raw_id) = parts[1].parse::<u64>() {
                                if let Some(ref meta) = self.meta {
                                    ctx.notify(meta, MetaCommand::SendToWorker {
                                        id: ActorId::from_raw(raw_id),
                                        text: parts[2].to_string(),
                                    });
                                }
                            } else {
                                *self.last_response.lock().unwrap() = Some("Invalid ID".into());
                            }
                        } else {
                            *self.last_response.lock().unwrap() = Some("Usage: send <id> <msg>".into());
                        }
                    }
                    "status" => {
                        if let Some(ref meta) = self.meta {
                            ctx.notify(meta, MetaCommand::GetStatus);
                        }
                    }
                    "list" => {
                        if let Some(ref meta) = self.meta {
                            ctx.notify(meta, MetaCommand::GetList);
                        }
                    }
                    _ => {
                        *self.last_response.lock().unwrap() =
                            Some(format!("Unknown: '{}'. Type 'help'.", command));
                    }
                }
                ConsoleResult::Ok("ok".into())
            }
            ConsoleCommand::Poll => {
                let text = self.last_response.lock().unwrap().take()
                    .unwrap_or_else(|| "".into());
                ConsoleResult::Ok(text)
            }
            ConsoleCommand::SetMeta { meta_addr } => {
                self.meta = Some(meta_addr);
                ConsoleResult::Ok("meta set".into())
            }
            ConsoleCommand::Quit => ConsoleResult::Quit,
        }
    }
}

// ── Main ────────────────────────────────────────────────────

fn main() {
    let rt = Runtime::with_threads(2);
    let log = Arc::new(Mutex::new(Vec::new()));

    // Shared holder: MetaActor writes CommandActor's Addr here during started()
    let cmd_addr_holder: Arc<Mutex<Option<Addr<CommandActor>>>> = Arc::new(Mutex::new(None));
    let cmd_holder_for_meta = cmd_addr_holder.clone();

    // 1. Spawn MetaActor — it will auto-spawn CommandActor in started()
    let meta = MetaActor {
        workers: HashMap::new(),
        cmd_addr: None,
        cmd_addr_holder: cmd_holder_for_meta,
        log: log.clone(),
    };
    let meta_addr = rt.spawn(meta);

    // 2. Wait for MetaActor to spawn CommandActor and share its Addr
    let cmd_addr: Addr<CommandActor> = loop {
        if let Some(addr) = cmd_addr_holder.lock().unwrap().take() {
            break addr;
        }
        thread::sleep(Duration::from_millis(5));
    };

    // 3. Give CommandActor MetaActor's Addr (needed for notify calls)
    cmd_addr.send(ConsoleCommand::SetMeta { meta_addr: meta_addr.clone() }).ok();

    // 4. Stdin thread sends commands to CommandActor
    let cmd_clone = cmd_addr.clone();
    let stdin_handle = thread::spawn(move || {
        let stdin = io::stdin();
        let reader = io::BufReader::new(stdin.lock());

        print_prompt();
        for line in reader.lines() {
            let line = match line { Ok(l) => l, Err(_) => break };
            let trimmed = line.trim();
            if trimmed.is_empty() { print_prompt(); continue; }

            if trimmed == "quit" || trimmed == "exit" {
                let _ = cmd_clone.do_send(ConsoleCommand::Quit);
                println!("  Shutting down...");
                break;
            }

            // Send command (async — returns immediately)
            cmd_clone.send(ConsoleCommand::Exec { input: trimmed.to_string() }).ok();

            // Poll for the async response from MetaActor
            let mut attempts = 0;
            loop {
                thread::sleep(Duration::from_millis(5));
                match cmd_clone.send(ConsoleCommand::Poll) {
                    Ok(ConsoleResult::Ok(text)) if !text.is_empty() => {
                        println!("  {}", text);
                        break;
                    }
                    _ => {}
                }
                attempts += 1;
                if attempts > 40 {
                    println!("  (timeout)");
                    break;
                }
            }

            print_prompt();
        }
        drop(cmd_clone);
    });

    stdin_handle.join().unwrap();
    drop(cmd_addr);
    drop(meta_addr);
    drop(rt);

    println!("\n--- Event Log ---");
    for entry in log.lock().unwrap().iter() {
        println!("  {}", entry);
    }
    println!("Goodbye.");
}

fn print_prompt() {
    print!("\n> ");
    io::stdout().flush().ok();
}
