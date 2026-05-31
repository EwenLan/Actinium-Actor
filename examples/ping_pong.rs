use std::thread;

use actinium_actor::{Actor, ActorSystem, Context, Handler, Message};

// ── Ping Actor ──────────────────────────────────────────────

struct PingActor {
    count: usize,
}

impl Actor for PingActor {}

struct Ping {
    reply_to: actinium_actor::Addr<PongActor>,
    text: String,
}

impl Message for Ping {
    type Result = usize;
}

impl Handler<Ping> for PingActor {
    fn handle(&mut self, msg: Ping, ctx: &mut Context) -> usize {
        println!("Ping received: {}", msg.text);
        self.count += 1;
        // Notify the pong actor asynchronously
        ctx.notify(&msg.reply_to, Pong {
            reply_to: ctx.id(),
            text: format!("pong-{}", self.count),
        });
        self.count
    }
}

// ── Pong Actor ──────────────────────────────────────────────

struct PongActor {
    count: usize,
}

impl Actor for PongActor {}

struct Pong {
    reply_to: actinium_actor::ActorId,
    text: String,
}

impl Message for Pong {
    type Result = ();
}

impl Handler<Pong> for PongActor {
    fn handle(&mut self, msg: Pong, _ctx: &mut Context) {
        println!("Pong received: {} (from {})", msg.text, msg.reply_to);
        self.count += 1;
    }
}

fn main() {
    let mut system = ActorSystem::new();

    let pong = system.spawn(PongActor { count: 0 });
    let ping = system.spawn(PingActor { count: 0 });

    // Move addresses into the sender thread so the channel closes when done.
    let sender = thread::spawn(move || {
        for i in 1..=3 {
            ping.send(Ping {
                reply_to: pong.clone(),
                text: format!("ping-{}", i),
            })
            .unwrap();
        }
        drop(ping);
        drop(pong);
    });

    system.run();
    sender.join().unwrap();

    println!("Ping pong exchange complete.");
}
