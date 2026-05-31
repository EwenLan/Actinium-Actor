//! Demonstrates multi-threaded actor execution.
//!
//! Spawns actors across configurable worker threads and shows concurrent
//! message processing with more actors than threads.

use std::thread;
use std::time::{Duration, Instant};

use actinium_actor::{Actor, Context, Handler, Message, Runtime, DEFAULT_WORKER_THREADS};

// ── Worker Actor ────────────────────────────────────────────

struct WorkerActor {
    id: usize,
}

impl Actor for WorkerActor {
    fn started(&mut self, _ctx: &mut Context) {
        println!("WorkerActor-{} started", self.id);
    }
    fn stopped(&mut self, _ctx: &mut Context) {
        println!("WorkerActor-{} stopped", self.id);
    }
}

struct DoWork {
    duration_ms: u64,
}

impl Message for DoWork {
    type Result = usize;
}

impl Handler<DoWork> for WorkerActor {
    fn handle(&mut self, msg: DoWork, _ctx: &mut Context) -> usize {
        println!(
            "WorkerActor-{} working for {}ms on thread {:?}",
            self.id,
            msg.duration_ms,
            thread::current().id()
        );
        thread::sleep(Duration::from_millis(msg.duration_ms));
        self.id
    }
}

fn main() {
    let num_threads = DEFAULT_WORKER_THREADS;
    let num_actors = 8; // more actors than threads

    println!(
        "=== Multi-Threaded Actor Demo ===\n\
         Runtime threads: {}\n\
         Actors:           {}\n",
        num_threads, num_actors
    );

    let rt = Runtime::with_threads(num_threads);

    // Spawn actors
    let addrs: Vec<_> = (0..num_actors)
        .map(|i| rt.spawn(WorkerActor { id: i }))
        .collect();

    let start = Instant::now();

    // Send work to all actors concurrently
    let handles: Vec<_> = addrs
        .into_iter()
        .map(|addr| {
            thread::spawn(move || {
                let _result = addr.send(DoWork { duration_ms: 100 }).unwrap();
                drop(addr);
            })
        })
        .collect();

    println!("All messages sent, waiting for completion...");
    rt.run();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "\nAll {} actors completed in {:?} (serial would be ~{}ms)",
        num_actors,
        elapsed,
        num_actors * 100
    );
}
