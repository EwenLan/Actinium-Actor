//! Multi-threaded state machine demo.
//!
//! Each worker cycles through 3 states:
//!   Receive → Execute → Report → Receive → ...
//!
//! Spawns more actors than threads to demonstrate concurrent state processing.

use std::thread;
use std::time::{Duration, Instant};

use actinium_actor::{
    Actor, Context, Message, Runtime, StateHandler, StateMachine, DEFAULT_WORKER_THREADS,
};

// ── States ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum TaskState {
    /// Accept a task.
    Receive,
    /// Execute the task (simulate work).
    Execute,
    /// Report completion.
    Report,
}

// ── Message ─────────────────────────────────────────────────

struct Task {
    duration_ms: u64,
}

impl Message for Task {
    type Result = String;
}

// ── Worker Actor ────────────────────────────────────────────

struct WorkerActor {
    id: usize,
}

impl Actor for WorkerActor {
    fn started(&mut self, _ctx: &mut Context) {
        println!("Worker-{} started", self.id);
    }
    fn stopped(&mut self, _ctx: &mut Context) {
        println!("Worker-{} stopped", self.id);
    }
}

impl StateHandler<Task, TaskState> for WorkerActor {
    fn initial_state() -> TaskState {
        TaskState::Receive
    }

    fn state_sequence() -> Vec<TaskState> {
        vec![TaskState::Receive, TaskState::Execute, TaskState::Report]
    }

    fn handle_in_state(
        &mut self,
        _idx: usize,
        state: &TaskState,
        msg: Task,
        _ctx: &mut Context,
    ) -> String {
        match state {
            TaskState::Receive => {
                format!(
                    "[W-{}] RECEIVED task ({}ms) on {:?}",
                    self.id,
                    msg.duration_ms,
                    thread::current().id()
                )
            }
            TaskState::Execute => {
                println!(
                    "[W-{}] EXECUTING for {}ms on {:?}",
                    self.id,
                    msg.duration_ms,
                    thread::current().id()
                );
                thread::sleep(Duration::from_millis(msg.duration_ms));
                format!("[W-{}] execution complete", self.id)
            }
            TaskState::Report => {
                format!(
                    "[W-{}] REPORTING done on {:?}",
                    self.id,
                    thread::current().id()
                )
            }
        }
    }
}

fn main() {
    let num_threads = DEFAULT_WORKER_THREADS;
    let num_actors = 4; // 4 actors in parallel
    let msgs_per_actor = 3; // one per state (Receive, Execute, Report)

    println!(
        "=== Multi-Threaded State Machine Demo ===\n\
         Threads: {}, Actors: {}, Messages per actor: {}\n",
        num_threads, num_actors, msgs_per_actor
    );

    let rt = Runtime::with_threads(num_threads);

    // Spawn state-machine actors
    let addrs: Vec<_> = (0..num_actors)
        .map(|i| {
            let worker = WorkerActor { id: i };
            let sm = StateMachine::new(worker);
            rt.spawn(sm)
        })
        .collect();

    let start = Instant::now();

    // Send 3 tasks to each actor (one for each state)
    let handles: Vec<_> = addrs
        .into_iter()
        .map(|addr| {
            thread::spawn(move || {
                for _ in 0..msgs_per_actor {
                    let _result = addr
                        .send(Task {
                            duration_ms: 50,
                        })
                        .unwrap();
                }
                drop(addr);
            })
        })
        .collect();

    rt.run();
    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "\n{} actors × {} states completed in {:?}",
        num_actors, msgs_per_actor, elapsed
    );
}
