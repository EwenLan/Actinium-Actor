# Actinium-Actor

A lightweight, multi-threaded Actor framework for Rust.

## Features

- **Actor model**: Type-safe actors with message passing
- **Single-threaded runtime** (`ActorSystem`): Simple event loop for deterministic execution
- **Multi-threaded runtime** (`Runtime`): Distributes actors across configurable worker threads
- **Lifecycle hooks**: `started` and `stopped` callbacks
- **Type-safe addresses**: `Addr<A>` provides compile-time message type checking
- **Zero dependencies**: Built on `std::sync::mpsc`, no external crates required

## Quick Start

### Single-Threaded

```rust
use std::thread;
use actinium_actor::{Actor, ActorSystem, Context, Handler, Message};

struct CounterActor { count: usize }

impl Actor for CounterActor {}

struct Increment;
impl Message for Increment { type Result = usize; }

impl Handler<Increment> for CounterActor {
    fn handle(&mut self, _msg: Increment, _ctx: &mut Context) -> usize {
        self.count += 1;
        self.count
    }
}

fn main() {
    let mut system = ActorSystem::new();
    let counter = system.spawn(CounterActor { count: 0 });

    let counter_clone = counter.clone();
    let sender = thread::spawn(move || {
        println!("Count: {}", counter_clone.send(Increment).unwrap());
        drop(counter_clone);
    });

    system.run();
    sender.join().unwrap();
}
```

### Multi-Threaded

```rust
use std::thread;
use actinium_actor::{Actor, Context, Handler, Message, Runtime};

struct WorkerActor { id: usize }

impl Actor for WorkerActor {
    fn started(&mut self, _ctx: &mut Context) { println!("Worker-{} started", self.id); }
}

struct DoWork;
impl Message for DoWork { type Result = usize; }

impl Handler<DoWork> for WorkerActor {
    fn handle(&mut self, _msg: DoWork, _ctx: &mut Context) -> usize { self.id }
}

fn main() {
    let rt = Runtime::with_threads(4); // configurable thread count
    let addrs: Vec<_> = (0..8).map(|i| rt.spawn(WorkerActor { id: i })).collect();

    let handles: Vec<_> = addrs.into_iter().map(|addr| {
        thread::spawn(move || { let _ = addr.send(DoWork); drop(addr); })
    }).collect();

    rt.run(); // waits for all workers to finish
    for h in handles { h.join().unwrap(); }
}
```

## Key Concepts

### Actor
A unit of computation that processes messages sequentially. Implement the `Actor` trait and `Handler<M>` for each message type.

### Message
Any `Send + 'static` type can be a message. Messages must implement the `Message` trait with an associated `Result` type.

### Addr\<A\>
A type-safe handle to an actor. `Addr<A>::send(msg)` blocks until the actor processes the message. Use `do_send(msg)` for fire-and-forget or inter-actor communication.

### Context
Provides the actor with access to the runtime: its own ID, the ability to send messages to other actors, and the ability to stop itself.

### ActorSystem (Single-Threaded)
Single-threaded runtime. Call `spawn()` to create actors, then `run()` to process messages on the current thread.

### Runtime (Multi-Threaded)
Multi-threaded runtime with configurable worker count. Actors are distributed across workers via round-robin. Use `Runtime::with_threads(n)` to set the thread count.

## Running Examples

```bash
cargo run --example ping_pong        # Single-threaded inter-actor communication
cargo run --example multi_thread     # Multi-threaded concurrent execution
```

## Architecture

### Single-Threaded
```
┌─────────────────────────────────────┐
│            ActorSystem              │
│  ┌──────────┐  ┌──────────┐       │
│  │  Actor A  │  │  Actor B  │  ...  │
│  └──────────┘  └──────────┘       │
│       ▲              ▲              │
│       │   Envelope   │              │
│  ┌────┴──────────────┴────┐        │
│  │     Event Loop (run)    │        │
│  └─────────────────────────┘        │
└─────────────────────────────────────┘
```

### Multi-Threaded
```
┌─────────────────────────────────────────────┐
│                  Runtime                     │
│                                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐    │
│  │ Worker 0  │ │ Worker 1  │ │ Worker N  │   │
│  │ ┌──────┐ │ │ ┌──────┐ │ │ ┌──────┐ │   │
│  │ │Actor │ │ │ │Actor │ │ │ │Actor │ │   │
│  │ │Actor │ │ │ │Actor │ │ │ │Actor │ │   │
│  │ └──────┘ │ │ └──────┘ │ │ └──────┘ │   │
│  └──────────┘ └──────────┘ └──────────┘    │
│                                              │
│   Distribution: Round-Robin                  │
└─────────────────────────────────────────────┘
```

## Development Roadmap

See [DEVELOPMENT.md](DEVELOPMENT.md) for the full development plan and progress.

## License

MIT
