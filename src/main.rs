use std::thread;

use actinium_actor::{Actor, ActorSystem, Context, Handler, Message};

// ── Counter Actor ───────────────────────────────────────────

struct CounterActor {
    count: usize,
}

impl Actor for CounterActor {}

struct Increment;
impl Message for Increment {
    type Result = usize;
}

impl Handler<Increment> for CounterActor {
    fn handle(&mut self, _msg: Increment, _ctx: &mut Context) -> usize {
        self.count += 1;
        self.count
    }
}

struct GetCount;
impl Message for GetCount {
    type Result = usize;
}

impl Handler<GetCount> for CounterActor {
    fn handle(&mut self, _msg: GetCount, _ctx: &mut Context) -> usize {
        self.count
    }
}

// ── Printer Actor ───────────────────────────────────────────

struct PrinterActor;

impl Actor for PrinterActor {}

struct Print(String);
impl Message for Print {
    type Result = ();
}

impl Handler<Print> for PrinterActor {
    fn handle(&mut self, msg: Print, _ctx: &mut Context) {
        println!("Printer received: {}", msg.0);
    }
}

fn main() {
    let mut system = ActorSystem::new();

    let counter = system.spawn(CounterActor { count: 0 });
    let printer = system.spawn(PrinterActor);

    // Move addresses into the sender thread; system.run() runs on main thread.
    let sender = thread::spawn(move || {
        for _i in 1..=5 {
            let count = counter.send(Increment).unwrap();
            printer
                .send(Print(format!("Counter incremented to {}", count)))
                .unwrap();
        }
        // When `counter` and `printer` are dropped here, the channel closes
        // and the event loop exits.
    });

    // Run the actor system on the main thread.
    system.run();

    sender.join().unwrap();
    println!("Actor system shut down cleanly.");
}
