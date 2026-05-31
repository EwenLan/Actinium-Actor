# Actinium-Actor

A lightweight, multi-threaded Actor framework for Rust.

## Features

- **Actor model**: Type-safe actors with message passing
- **Single-threaded runtime**: Simple event loop for deterministic execution
- **Lifecycle hooks**: `started` and `stopped` callbacks
- **Type-safe addresses**: `Addr<A>` provides compile-time message type checking
- **Zero dependencies**: Built on `std::sync::mpsc`, no external crates required

## Quick Start

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
    let sender = std::thread::spawn(move || {
        println!("Count: {}", counter_clone.send(Increment).unwrap()); // 1
        println!("Count: {}", counter_clone.send(Increment).unwrap()); // 2
        drop(counter_clone);
    });

    system.run();
    sender.join().unwrap();
}
```

## Key Concepts

### Actor
A unit of computation that processes messages sequentially. Implement the `Actor` trait and `Handler<M>` for each message type.

### Message
Any `Send + 'static` type can be a message. Messages must implement the `Message` trait with an associated `Result` type.

### Addr\<A\>
A type-safe handle to an actor. `Addr<A>::send(msg)` blocks until the actor processes the message and returns a result.

### Context
Provides the actor with access to the runtime: its own ID, the ability to send messages to other actors, and the ability to stop itself.

### ActorSystem
The single-threaded runtime. Call `spawn()` to create actors, then `run()` to start processing messages.

## Running the Example

```bash
cargo run --example ping_pong
```

## Architecture

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

## License

MIT
