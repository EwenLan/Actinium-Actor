//! Ping-pong using the state machine mechanism.
//!
//! Two actors cycle through states independently:
//!   Ping → Pong → Ping → Pong → ...
//!
//! Each actor processes one message per state, then auto-advances.
//! After 4 messages, each actor has completed 2 full cycles.

use std::thread;

use actinium_actor::{
    Actor, ActorSystem, Context, Message, StateHandler, StateMachine,
};

// ── States ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum RallyState {
    Ping,
    Pong,
}

// ── Message ─────────────────────────────────────────────────

struct Rally {
    player: String,
    round: usize,
}

impl Message for Rally {
    type Result = String;
}

// ── Rally Actor ─────────────────────────────────────────────

struct RallyActor {
    name: String,
}

impl Actor for RallyActor {}

impl StateHandler<Rally, RallyState> for RallyActor {
    fn initial_state() -> RallyState {
        RallyState::Ping
    }

    fn state_sequence() -> Vec<RallyState> {
        vec![RallyState::Ping, RallyState::Pong]
    }

    fn handle_in_state(
        &mut self,
        idx: usize,
        state: &RallyState,
        msg: Rally,
        _ctx: &mut Context,
    ) -> String {
        match state {
            RallyState::Ping => {
                format!(
                    "[{}] PING  ← round {} from {} (state {})",
                    self.name, msg.round, msg.player, idx
                )
            }
            RallyState::Pong => {
                format!(
                    "[{}] PONG  ← round {} from {} (state {})",
                    self.name, msg.round, msg.player, idx
                )
            }
        }
    }
}

fn main() {
    let mut system = ActorSystem::new();

    // Wrap each actor in a state machine and spawn
    let alice_sm = StateMachine::new(RallyActor {
        name: "Alice".into(),
    });
    let alice = system.spawn(alice_sm);

    let bob_sm = StateMachine::new(RallyActor {
        name: "Bob".into(),
    });
    let bob = system.spawn(bob_sm);

    println!("=== Ping-Pong State Machine ===\n");

    let sender = thread::spawn(move || {
        for round in 1..=4 {
            let result = alice
                .send(Rally { player: "alice".into(), round })
                .unwrap();
            println!("  {}", result);

            let result = bob
                .send(Rally { player: "bob".into(), round })
                .unwrap();
            println!("  {}", result);
        }
        drop(alice);
        drop(bob);
    });

    system.run();
    sender.join().unwrap();

    println!("\n=== Complete: 4 rounds × 2 players = 8 messages, 2 full state cycles each ===");
}
