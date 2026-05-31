//! Console-based actor management REPL with MetaActor supervision.
//!
//! Architecture:
//!   stdin → CommandActor → MetaActor → WorkerActor(s)
//!
//! The MetaActor is automatically started on startup and manages all
//! worker lifecycle (create/destroy). The CommandActor delegates
//! spawn/stop operations to the MetaActor via message interfaces.
//!
//! Each worker cycles through states:
//!   Idle → Receiving → Processing → Idle → ...
//!
//! Commands:
//!   spawn <name>     Create a new worker actor
//!   stop <id>        Stop an actor by ID
//!   send <id> <msg>  Send a message to an actor
//!   status           Show all actors and their states
//!   list             List all actors
//!   help             Show this help
//!   quit             Shutdown

use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};
use std::thread;

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

// ── Worker Info ─────────────────────────────────────────────

#[derive(Debug, Clone)]
struct WorkerInfo {
    name: String,
    msg_count: usize,
    last_msg: String,
    current_state: String,
}

// ── Worker Actor ────────────────────────────────────────────

type WorkerSM = StateMachine<WorkerActor, WorkerMessage, WorkerState>;

struct WorkerActor {
    info: Arc<Mutex<WorkerInfo>>,
    log: Arc<Mutex<Vec<String>>>,
}

impl Actor for WorkerActor {
    fn started(&mut self, ctx: &mut Context) {
        let name = self.info.lock().unwrap().name.clone();
        self.log
            .lock()
            .unwrap()
            .push(format!("[{}] Worker '{}' started", ctx.id(), name));
    }
    fn stopped(&mut self, ctx: &mut Context) {
        let name = self.info.lock().unwrap().name.clone();
        self.log
            .lock()
            .unwrap()
            .push(format!("[{}] Worker '{}' stopped", ctx.id(), name));
    }
}

struct WorkerMessage {
    text: String,
}

impl Message for WorkerMessage {
    type Result = ();
}

impl StateHandler<WorkerMessage, WorkerState> for WorkerActor {
    fn initial_state() -> WorkerState {
        WorkerState::Idle
    }
    fn state_sequence() -> Vec<WorkerState> {
        vec![WorkerState::Idle, WorkerState::Receiving, WorkerState::Processing]
    }
    fn handle_in_state(
        &mut self,
        _idx: usize,
        state: &WorkerState,
        msg: WorkerMessage,
        ctx: &mut Context,
    ) {
        let mut info = self.info.lock().unwrap();
        info.msg_count += 1;
        info.last_msg = msg.text.clone();
        info.current_state = format!("{:?}", state);
        self.log.lock().unwrap().push(format!(
            "[{}] '{}' received in state {:?} (#{}): {}",
            ctx.id(),
            info.name,
            state,
            info.msg_count,
            msg.text
        ));
    }
}

// ── MetaActor ───────────────────────────────────────────────

/// A managed worker entry tracked by the MetaActor.
struct ManagedWorker {
    addr: Addr<WorkerSM>,
    info: Arc<Mutex<WorkerInfo>>,
    name: String,
}

/// Commands sent from CommandActor to MetaActor.
enum MetaCommand {
    CreateWorker { name: String },
    DestroyWorker { id: ActorId },
    GetWorkers,
    GetWorkerStatus,
    SendToWorker { id: ActorId, text: String },
}

impl Message for MetaCommand {
    type Result = MetaResult;
}

/// Results returned from MetaActor to CommandActor.
enum MetaResult {
    Created { id: ActorId, name: String },
    Destroyed { id: ActorId, name: String, msg_count: usize, state: String },
    WorkerList(Vec<(ActorId, String)>),
    WorkerStatus(Vec<WorkerSnapshot>),
    MessageSent { id: ActorId, name: String },
    Err(String),
}

#[derive(Debug, Clone)]
struct WorkerSnapshot {
    id: ActorId,
    name: String,
    msg_count: usize,
    state: String,
    last_msg: String,
}

struct MetaActor {
    workers: HashMap<ActorId, ManagedWorker>,
    log: Arc<Mutex<Vec<String>>>,
}

impl Actor for MetaActor {
    fn started(&mut self, _ctx: &mut Context) {
        self.log.lock().unwrap().push("[MetaActor] started".into());
    }
    fn stopped(&mut self, _ctx: &mut Context) {
        self.log.lock().unwrap().push("[MetaActor] stopped".into());
    }
}

impl Handler<MetaCommand> for MetaActor {
    fn handle(&mut self, cmd: MetaCommand, ctx: &mut Context) -> MetaResult {
        match cmd {
            MetaCommand::CreateWorker { name } => {
                let info = Arc::new(Mutex::new(WorkerInfo {
                    name: name.clone(),
                    msg_count: 0,
                    last_msg: String::new(),
                    current_state: "Idle".into(),
                }));
                let worker = WorkerActor {
                    info: info.clone(),
                    log: self.log.clone(),
                };
                let sm = StateMachine::new(worker);
                let addr = ctx.spawn(sm);
                let id = addr.id();
                self.workers.insert(id, ManagedWorker { addr, info, name: name.clone() });
                MetaResult::Created { id, name }
            }
            MetaCommand::DestroyWorker { id } => {
                if let Some(worker) = self.workers.remove(&id) {
                    let info = worker.info.lock().unwrap();
                    MetaResult::Destroyed {
                        id,
                        name: worker.name.clone(),
                        msg_count: info.msg_count,
                        state: info.current_state.clone(),
                    }
                } else {
                    MetaResult::Err(format!("Worker {} not found", id))
                }
            }
            MetaCommand::GetWorkers => {
                let list: Vec<_> = self
                    .workers
                    .iter()
                    .map(|(id, w)| (*id, w.name.clone()))
                    .collect();
                MetaResult::WorkerList(list)
            }
            MetaCommand::GetWorkerStatus => {
                let snapshots: Vec<_> = self
                    .workers
                    .iter()
                    .map(|(id, w)| {
                        let info = w.info.lock().unwrap();
                        WorkerSnapshot {
                            id: *id,
                            name: w.name.clone(),
                            msg_count: info.msg_count,
                            state: info.current_state.clone(),
                            last_msg: info.last_msg.clone(),
                        }
                    })
                    .collect();
                MetaResult::WorkerStatus(snapshots)
            }
            MetaCommand::SendToWorker { id, text } => {
                if let Some(worker) = self.workers.get(&id) {
                    let _ = worker.addr.do_send(WorkerMessage { text });
                    MetaResult::MessageSent {
                        id,
                        name: worker.name.clone(),
                    }
                } else {
                    MetaResult::Err(format!("Worker {} not found", id))
                }
            }
        }
    }
}

// ── Command Actor ───────────────────────────────────────────

/// Commands from stdin thread.
enum ConsoleCommand {
    Spawn { name: String },
    Stop { id: ActorId },
    Send { id: ActorId, text: String },
    Status,
    List,
    Quit,
}

impl Message for ConsoleCommand {
    type Result = ConsoleResult;
}

enum ConsoleResult {
    Ok(String),
    Err(String),
    Quit,
}

struct CommandActor {
    meta: Addr<MetaActor>,
}

impl Actor for CommandActor {
    fn started(&mut self, _ctx: &mut Context) {
        println!("=== Actinium-Actor Console ===");
        println!("Type 'help' for available commands.\n");
    }
}

impl Handler<ConsoleCommand> for CommandActor {
    fn handle(&mut self, cmd: ConsoleCommand, ctx: &mut Context) -> ConsoleResult {
        match cmd {
            ConsoleCommand::Spawn { name } => {
                match ctx.send(&self.meta, MetaCommand::CreateWorker { name }) {
                    Ok(MetaResult::Created { id, name }) => {
                        ConsoleResult::Ok(format!("Spawned '{}' with ID {}", name, id))
                    }
                    Ok(MetaResult::Err(e)) => ConsoleResult::Err(e),
                    _ => ConsoleResult::Err("meta actor error".into()),
                }
            }
            ConsoleCommand::Stop { id } => {
                match ctx.send(&self.meta, MetaCommand::DestroyWorker { id }) {
                    Ok(MetaResult::Destroyed { id, name, msg_count, state }) => {
                        ConsoleResult::Ok(format!(
                            "Stopped '{}' ({}) — {} msg(s), last state: {}",
                            name, id, msg_count, state
                        ))
                    }
                    Ok(MetaResult::Err(e)) => ConsoleResult::Err(e),
                    _ => ConsoleResult::Err("meta actor error".into()),
                }
            }
            ConsoleCommand::Send { id, text } => {
                match ctx.send(&self.meta, MetaCommand::SendToWorker {
                    id,
                    text: text.clone(),
                }) {
                    Ok(MetaResult::MessageSent { id, name }) => {
                        ConsoleResult::Ok(format!("Message sent to '{}' ({})", name, id))
                    }
                    Ok(MetaResult::Err(e)) => ConsoleResult::Err(e),
                    _ => ConsoleResult::Err("meta actor error".into()),
                }
            }
            ConsoleCommand::Status => {
                match ctx.send(&self.meta, MetaCommand::GetWorkerStatus) {
                    Ok(MetaResult::WorkerStatus(snapshots)) => {
                        if snapshots.is_empty() {
                            ConsoleResult::Ok("No actors running.".into())
                        } else {
                            let lines: Vec<String> = snapshots
                                .iter()
                                .map(|s| {
                                    format!(
                                        "  {} '{}' — {} msg(s), state: {}, last: \"{}\"",
                                        s.id, s.name, s.msg_count, s.state, s.last_msg
                                    )
                                })
                                .collect();
                            ConsoleResult::Ok(format!(
                                "{} actor(s) running:\n{}",
                                snapshots.len(),
                                lines.join("\n")
                            ))
                        }
                    }
                    _ => ConsoleResult::Err("meta actor error".into()),
                }
            }
            ConsoleCommand::List => {
                match ctx.send(&self.meta, MetaCommand::GetWorkers) {
                    Ok(MetaResult::WorkerList(list)) => {
                        if list.is_empty() {
                            ConsoleResult::Ok("No actors.".into())
                        } else {
                            let ids: Vec<String> = list
                                .iter()
                                .map(|(id, name)| format!("{} ({})", name, id))
                                .collect();
                            ConsoleResult::Ok(format!("Actors: {}", ids.join(", ")))
                        }
                    }
                    _ => ConsoleResult::Err("meta actor error".into()),
                }
            }
            ConsoleCommand::Quit => ConsoleResult::Quit,
        }
    }
}

// ── Main ────────────────────────────────────────────────────

fn main() {
    let rt = Runtime::with_threads(2);
    let log = Arc::new(Mutex::new(Vec::new()));

    // 1. Spawn MetaActor first (auto-started, manages worker lifecycle)
    let meta = MetaActor {
        workers: HashMap::new(),
        log: log.clone(),
    };
    let meta_addr = rt.spawn(meta);

    // 2. Spawn CommandActor with reference to MetaActor
    let cmd_actor = CommandActor {
        meta: meta_addr.clone(),
    };
    let cmd_addr = rt.spawn(cmd_actor);

    // 3. Stdin thread sends commands to CommandActor
    let cmd_addr_clone = cmd_addr.clone();
    let stdin_handle = thread::spawn(move || {
        let stdin = io::stdin();
        let reader = io::BufReader::new(stdin.lock());

        print_prompt();
        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => break,
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                print_prompt();
                continue;
            }
            let result = process_command(&cmd_addr_clone, trimmed);
            match result {
                ConsoleResult::Ok(msg) => println!("  {}", msg),
                ConsoleResult::Err(msg) => eprintln!("  Error: {}", msg),
                ConsoleResult::Quit => {
                    println!("  Shutting down...");
                    break;
                }
            }
            print_prompt();
        }
        drop(cmd_addr_clone);
    });

    stdin_handle.join().unwrap();
    drop(cmd_addr);
    drop(meta_addr);
    drop(rt);

    let log_entries = log.lock().unwrap();
    if !log_entries.is_empty() {
        println!("\n--- Event Log ---");
        for entry in log_entries.iter() {
            println!("  {}", entry);
        }
    }
    println!("Goodbye.");
}

fn print_prompt() {
    print!("\n> ");
    io::stdout().flush().ok();
}

fn process_command(cmd_addr: &Addr<CommandActor>, input: &str) -> ConsoleResult {
    let parts: Vec<&str> = input.splitn(3, ' ').collect();
    let command = parts[0].to_lowercase();

    match command.as_str() {
        "help" => ConsoleResult::Ok(
            "Commands:\n  spawn <name>      Create a worker actor\n  stop <id>         Stop an actor\n  send <id> <msg>   Send message to an actor\n  status            Show actor states\n  list              List all actors\n  help              Show this help\n  quit              Shutdown"
                .into(),
        ),
        "spawn" => {
            if parts.len() < 2 || parts[1].is_empty() {
                return ConsoleResult::Err("Usage: spawn <name>".into());
            }
            cmd_addr
                .send(ConsoleCommand::Spawn { name: parts[1].to_string() })
                .unwrap_or(ConsoleResult::Err("send failed".into()))
        }
        "stop" => {
            if parts.len() < 2 {
                return ConsoleResult::Err("Usage: stop <id>".into());
            }
            match parts[1].parse::<u64>() {
                Ok(raw_id) => cmd_addr
                    .send(ConsoleCommand::Stop { id: ActorId::from_raw(raw_id) })
                    .unwrap_or(ConsoleResult::Err("send failed".into())),
                Err(_) => ConsoleResult::Err("Invalid actor ID".into()),
            }
        }
        "send" => {
            if parts.len() < 3 {
                return ConsoleResult::Err("Usage: send <id> <message>".into());
            }
            match parts[1].parse::<u64>() {
                Ok(raw_id) => cmd_addr
                    .send(ConsoleCommand::Send {
                        id: ActorId::from_raw(raw_id),
                        text: parts[2].to_string(),
                    })
                    .unwrap_or(ConsoleResult::Err("send failed".into())),
                Err(_) => ConsoleResult::Err("Invalid actor ID".into()),
            }
        }
        "status" => cmd_addr
            .send(ConsoleCommand::Status)
            .unwrap_or(ConsoleResult::Err("send failed".into())),
        "list" => cmd_addr
            .send(ConsoleCommand::List)
            .unwrap_or(ConsoleResult::Err("send failed".into())),
        "quit" | "exit" => {
            let _ = cmd_addr.do_send(ConsoleCommand::Quit);
            ConsoleResult::Quit
        }
        _ => ConsoleResult::Err(format!("Unknown command: '{}'. Type 'help'.", command)),
    }
}
