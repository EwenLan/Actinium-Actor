//! Demonstrates the state machine mechanism.
//!
//! A document processing pipeline with 4 states:
//!   Receive → Validate → Process → Archive → Receive → ...
//!
//! Each state processes one message then the machine auto-advances.

use std::thread;

use actinium_actor::{
    Actor, Context, Message, Runtime, StateHandler, StateMachine,
};

// ── States ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum DocState {
    Receive,
    Validate,
    Process,
    Archive,
}

// ── Message ─────────────────────────────────────────────────

struct Document {
    id: usize,
    content: String,
}

impl Message for Document {
    type Result = String;
}

// ── Pipeline Actor ──────────────────────────────────────────

struct PipelineActor {
    processed: usize,
    validated: usize,
}

impl Actor for PipelineActor {
    fn started(&mut self, _ctx: &mut Context) {
        println!("[Pipeline] Started");
    }
    fn stopped(&mut self, _ctx: &mut Context) {
        println!(
            "[Pipeline] Stopped — validated: {}, fully processed: {}",
            self.validated, self.processed
        );
    }
}

impl StateHandler<Document, DocState> for PipelineActor {
    fn initial_state() -> DocState {
        DocState::Receive
    }

    fn state_sequence() -> Vec<DocState> {
        vec![
            DocState::Receive,
            DocState::Validate,
            DocState::Process,
            DocState::Archive,
        ]
    }

    fn handle_in_state(
        &mut self,
        idx: usize,
        state: &DocState,
        msg: Document,
        _ctx: &mut Context,
    ) -> String {
        match state {
            DocState::Receive => {
                let result = format!(
                    "[Receive]  Accepted document #{}: \"{}\"",
                    msg.id, msg.content
                );
                println!("{}", result);
                result
            }
            DocState::Validate => {
                let valid = !msg.content.is_empty();
                let result = if valid {
                    self.validated += 1;
                    format!(
                        "[Validate] Document #{} is valid (total valid: {})",
                        msg.id, self.validated
                    )
                } else {
                    format!("[Validate] Document #{} is INVALID", msg.id)
                };
                println!("{}", result);
                result
            }
            DocState::Process => {
                let result = format!(
                    "[Process]  Processing document #{} (state idx: {})...",
                    msg.id, idx
                );
                println!("{}", result);
                self.processed += 1;
                result
            }
            DocState::Archive => {
                let result = format!(
                    "[Archive]  Document #{} archived (total done: {})",
                    msg.id, self.processed
                );
                println!("{}", result);
                result
            }
        }
    }
}

fn main() {
    let rt = Runtime::with_threads(2);

    // Wrap the pipeline actor in a state machine
    let pipeline = PipelineActor {
        processed: 0,
        validated: 0,
    };
    let sm = StateMachine::new(pipeline);
    let addr = rt.spawn(sm);

    println!("=== State Machine Pipeline Demo ===\n");
    println!("Sending 8 documents (2 complete cycles of 4 states):\n");

    // Send documents from another thread
    let addr_clone = addr.clone();
    let handle = thread::spawn(move || {
        for i in 1..=8 {
            let doc = Document {
                id: i,
                content: format!("Document-{} content", i),
            };
            let response = addr_clone.send(doc).unwrap();
            println!("  -> {}", response);
        }
        drop(addr_clone);
    });

    handle.join().unwrap();
    drop(addr);
    drop(rt);

    println!("\n=== Pipeline Complete ===");
}
