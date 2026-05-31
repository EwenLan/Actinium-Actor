//! Console-based actor management REPL with state machine workers.
//!
//! Each worker actor cycles through states:
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
    /// Waiting for the first message.
    Idle,
    /// Receiving a message.
    Receiving,
    /// Processing the received message.
    Processing,
}

// ── Worker Actor ────────────────────────────────────────────

/// Shared state for querying worker status without blocking sends.
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

// ── Command Actor (Supervisor) ──────────────────────────────

struct ManagedActor {
    addr: Addr<WorkerSM>,
    info: Arc<Mutex<WorkerInfo>>,
    name: String,
}

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

impl Handler<ConsoleCommand> for CommandActor {
    fn handle(&mut self, cmd: ConsoleCommand, ctx: &mut Context) -> ConsoleResult {
        match cmd {
            ConsoleCommand::Spawn { name } => {
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
                self.actors.insert(id, ManagedActor {
                    addr,
                    info,
                    name: name.clone(),
                });
                ConsoleResult::Ok(format!("Spawned actor '{}' with ID {} (state: Idle)", name, id))
            }
            ConsoleCommand::Stop { id } => {
                if let Some(managed) = self.actors.remove(&id) {
                    let info = managed.info.lock().unwrap();
                    ConsoleResult::Ok(format!(
                        "Stopped actor '{}' ({}) — {} msg(s), last state: {}",
                        managed.name, id, info.msg_count, info.current_state
                    ))
                } else {
                    ConsoleResult::Err(format!("Actor {} not found", id))
                }
            }
            ConsoleCommand::Send { id, text } => {
                if let Some(managed) = self.actors.get(&id) {
                    let name = managed.name.clone();
                    let _ = managed.addr.do_send(WorkerMessage { text });
                    ConsoleResult::Ok(format!("Message sent to '{}' ({})", name, id))
                } else {
                    ConsoleResult::Err(format!("Actor {} not found", id))
                }
            }
            ConsoleCommand::Status => {
                let mut lines = vec![];
                for (id, managed) in &self.actors {
                    let info = managed.info.lock().unwrap();
                    lines.push(format!(
                        "  {} '{}' — {} msg(s), state: {}, last: \"{}\"",
                        id, managed.name, info.msg_count, info.current_state, info.last_msg
                    ));
                }
                if lines.is_empty() {
                    ConsoleResult::Ok("No actors running.".into())
                } else {
                    ConsoleResult::Ok(format!(
                        "{} actor(s) running:\n{}",
                        lines.len(),
                        lines.join("\n")
                    ))
                }
            }
            ConsoleCommand::List => {
                if self.actors.is_empty() {
                    ConsoleResult::Ok("No actors.".into())
                } else {
                    let ids: Vec<String> = self
                        .actors
                        .iter()
                        .map(|(id, m)| format!("{} ({})", m.name, id))
                        .collect();
                    ConsoleResult::Ok(format!("Actors: {}", ids.join(", ")))
                }
            }
            ConsoleCommand::Quit => ConsoleResult::Quit,
        }
    }
}

struct CommandActor {
    actors: HashMap<ActorId, ManagedActor>,
    log: Arc<Mutex<Vec<String>>>,
}

impl Actor for CommandActor {
    fn started(&mut self, _ctx: &mut Context) {
        println!("=== Actinium-Actor Console (State Machine) ===");
        println!("Type 'help' for available commands.\n");
    }
}

// ── Main ────────────────────────────────────────────────────

fn main() {
    let rt = Runtime::with_threads(2);
    let log = Arc::new(Mutex::new(Vec::new()));

    let cmd_actor = CommandActor {
        actors: HashMap::new(),
        log: log.clone(),
    };
    let cmd_addr = rt.spawn(cmd_actor);

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
                    .send(ConsoleCommand::Send { id: ActorId::from_raw(raw_id), text: parts[2].to_string() })
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
