use std::thread;

use actinium_actor::{
    Actor, Addr, Context, Handler, Message, ProbeActor, TestKit, TestProbe,
};

// ── Test actors ──────────────────────────────────────────────

#[derive(Clone)]
struct WorkDone {
    actor_id: usize,
    result: String,
}

impl Message for WorkDone {
    type Result = ();
}

struct WorkerActor {
    id: usize,
    output: Addr<ProbeActor<WorkDone>>,
}

impl Actor for WorkerActor {}

struct DoWork {
    text: String,
}

impl Message for DoWork {
    type Result = ();
}

impl Handler<DoWork> for WorkerActor {
    fn handle(&mut self, msg: DoWork, ctx: &mut Context) {
        let result = format!("worker-{} processed: {}", self.id, msg.text);
        ctx.notify(
            &self.output,
            WorkDone {
                actor_id: self.id,
                result,
            },
        );
    }
}

// ── Tests ───────────────────────────────────────────────────

#[test]
fn test_worker_sends_output_to_probe() {
    let mut kit = TestKit::new();

    // Create a probe to collect WorkDone messages
    let probe = kit.spawn_probe::<WorkDone>();

    // Spawn a worker that sends results to the probe
    let worker = kit.spawn(WorkerActor {
        id: 1,
        output: probe.recipient(),
    });

    // Send work to the worker from another thread
    let worker_clone = worker.clone();
    let handle = thread::spawn(move || {
        let _ = worker_clone.do_send(DoWork {
            text: "hello".into(),
        });
        let _ = worker_clone.do_send(DoWork {
            text: "world".into(),
        });
        drop(worker_clone);
    });

    thread::sleep(std::time::Duration::from_millis(10));
    kit.run_until_idle();
    handle.join().unwrap();

    // Assert: both messages were delivered to the probe
    assert_eq!(probe.count(), 2);
    assert!(probe.any_match(|w| w.result.contains("hello")));
    assert!(probe.any_match(|w| w.result.contains("world")));
}

#[test]
fn test_multiple_workers() {
    let mut kit = TestKit::new();

    let probe = kit.spawn_probe::<WorkDone>();

    // Spawn 3 workers, all sending to the same probe
    let workers: Vec<_> = (1..=3)
        .map(|i| {
            kit.spawn(WorkerActor {
                id: i,
                output: probe.recipient(),
            })
        })
        .collect();

    // Send one message to each worker
    let handles: Vec<_> = workers
        .into_iter()
        .map(|addr| {
            thread::spawn(move || {
                let _ = addr.do_send(DoWork {
                    text: format!("task"),
                });
                drop(addr);
            })
        })
        .collect();

    thread::sleep(std::time::Duration::from_millis(10));
    kit.run_until_idle();
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(probe.count(), 3);
}

#[test]
fn test_probe_only_receives_its_own_type() {
    let mut kit = TestKit::new();

    let probe: TestProbe<WorkDone> = kit.spawn_probe::<WorkDone>();

    // Messages of the correct type are recorded
    let addr = probe.recipient();
    let handle = thread::spawn(move || {
        let _ = addr.do_send(WorkDone {
            actor_id: 0,
            result: "test".into(),
        });
        drop(addr);
    });

    thread::sleep(std::time::Duration::from_millis(10));
    kit.run_until_idle();
    handle.join().unwrap();

    assert_eq!(probe.count(), 1);
}

#[test]
fn test_actor_chain_with_probes() {
    // Demonstrates: Actor A → Probe B, Actor B → Probe C
    let mut kit = TestKit::new();

    let probe_b = kit.spawn_probe::<WorkDone>();
    let probe_c = kit.spawn_probe::<WorkDone>();

    let actor_a = kit.spawn(WorkerActor {
        id: 10,
        output: probe_b.recipient(),
    });

    let actor_b = kit.spawn(WorkerActor {
        id: 20,
        output: probe_c.recipient(),
    });

    let a_clone = actor_a.clone();
    let b_clone = actor_b.clone();

    let handle_a = thread::spawn(move || {
        let _ = a_clone.do_send(DoWork {
            text: "to-b".into(),
        });
        drop(a_clone);
    });

    let handle_b = thread::spawn(move || {
        let _ = b_clone.do_send(DoWork {
            text: "to-c".into(),
        });
        drop(b_clone);
    });

    thread::sleep(std::time::Duration::from_millis(10));
    kit.run_until_idle();
    handle_a.join().unwrap();
    handle_b.join().unwrap();

    // Each probe should have received from its respective worker
    assert_eq!(probe_b.count(), 1);
    assert!(probe_b.any_match(|w| w.actor_id == 10));
    assert_eq!(probe_c.count(), 1);
    assert!(probe_c.any_match(|w| w.actor_id == 20));
}
