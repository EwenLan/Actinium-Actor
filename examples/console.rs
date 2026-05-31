//! Console-based actor management REPL.
//!
//! A main CommandActor supervises worker actors. A stdin thread reads
//! commands and forwards them to the CommandActor for processing.
//!
//! Commands:
//!   spawn <name>     Create a new worker actor
//!   stop <id>        Stop an actor by ID
//!   send <id> <msg>  Send a message to an actor
//!   status           Show all actors and their state
//!   list             List all actors
//!   help             Show this help
//!   quit             Shutdown

use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};
use std::thread;

use actinium_actor::{Actor, ActorId, Addr, Context, Handler, Message, Runtime};

// ── Worker Actor ────────────────────────────────────────────

/// Shared state for querying worker status without blocking sends.
#[derive(Debug, Clone)]
struct WorkerInfo {
    name: String,
    msg_count: usize,
    last_msg: String,
}

/// A managed worker actor. Its state is stored in an `Arc<Mutex<WorkerInfo>>`
/// so the supervisor can read it without blocking message sends.
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

impl Handler<WorkerMessage> for WorkerActor {
    fn handle(&mut self, msg: WorkerMessage, ctx: &mut Context) {
        let mut info = self.info.lock().unwrap();
        info.msg_count += 1;
        info.last_msg = msg.text.clone();
        self.log.lock().unwrap().push(format!(
            "[{}] '{}' received #{}: {}",
            ctx.id(),
            info.name,
            info.msg_count,
            msg.text
        ));
    }
}

// ── Command Actor (Supervisor) ──────────────────────────────

struct ManagedActor {
    addr: Addr<WorkerActor>,
    info: Arc<Mutex<WorkerInfo>>,
    name: String,
}

/// Command sent from the stdin thread to the CommandActor.
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
                }));
                let worker = WorkerActor {
                    info: info.clone(),
                    log: self.log.clone(),
                };
                let addr = ctx.spawn(worker);
                let id = addr.id();
                self.actors.insert(id, ManagedActor {
                    addr,
                    info,
                    name: name.clone(),
                });
                ConsoleResult::Ok(format!("Spawned actor '{}' with ID {}", name, id))
            }
            ConsoleCommand::Stop { id } => {
                if let Some(managed) = self.actors.remove(&id) {
                    let name = managed.name.clone();
                    let info = managed.info.lock().unwrap();
                    ConsoleResult::Ok(format!(
                        "Stopped actor '{}' ({}) — processed {} messages",
                        name, id, info.msg_count
                    ))
                } else {
                    ConsoleResult::Err(format!("Actor {} not found", id))
                }
            }
            ConsoleCommand::Send { id, text } => {
                if let Some(managed) = self.actors.get(&id) {
                    let name = managed.name.clone();
                    // Use do_send to avoid deadlock (same-worker actors)
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
                        "  {} '{}' — {} msg(s), last: \"{}\"",
                        id, managed.name, info.msg_count, info.last_msg
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
        println!("=== Actinium-Actor Console ===");
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

    // Spawn stdin reader thread
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

    // Wait for stdin thread to finish, then shut down workers
    stdin_handle.join().unwrap();
    drop(cmd_addr);
    drop(rt);

    // Print final log
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
            let cmd = ConsoleCommand::Spawn {
                name: parts[1].to_string(),
            };
            cmd_addr
                .send(cmd)
                .unwrap_or(ConsoleResult::Err("send failed".into()))
        }

        "stop" => {
            if parts.len() < 2 {
                return ConsoleResult::Err("Usage: stop <id>".into());
            }
            match parts[1].parse::<u64>() {
                Ok(raw_id) => {
                    let cmd = ConsoleCommand::Stop {
                        id: ActorId::from_raw(raw_id),
                    };
                    cmd_addr
                        .send(cmd)
                        .unwrap_or(ConsoleResult::Err("send failed".into()))
                }
                Err(_) => ConsoleResult::Err("Invalid actor ID. Use the numeric part (e.g., 'actor-3' → 3)".into()),
            }
        }

        "send" => {
            if parts.len() < 3 {
                return ConsoleResult::Err("Usage: send <id> <message>".into());
            }
            match parts[1].parse::<u64>() {
                Ok(raw_id) => {
                    let cmd = ConsoleCommand::Send {
                        id: ActorId::from_raw(raw_id),
                        text: parts[2].to_string(),
                    };
                    cmd_addr
                        .send(cmd)
                        .unwrap_or(ConsoleResult::Err("send failed".into()))
                }
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
