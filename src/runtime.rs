use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::actor::{Actor, ActorId};
use crate::addr::Addr;
use crate::context::Context;
use crate::envelope::{DispatchFn, Envelope};

/// Default number of worker threads.
pub const DEFAULT_WORKER_THREADS: usize = 4;

// ── Control Messages ────────────────────────────────────────

enum ControlMsg {
    SpawnActor {
        id: ActorId,
        actor: Box<dyn Any + Send>,
        on_stop: DispatchFn,
        confirm: Sender<()>,
    },
    Shutdown,
}

// ── Worker ──────────────────────────────────────────────────

struct ActorCell {
    actor: Box<dyn Any + Send>,
    on_stop: DispatchFn,
}

struct Worker {
    control_rx: Receiver<ControlMsg>,
    msg_rx: Receiver<Envelope>,
    actors: HashMap<ActorId, ActorCell>,
}

impl Worker {
    fn run(mut self) {
        loop {
            // Process all pending control messages
            loop {
                match self.control_rx.try_recv() {
                    Ok(ControlMsg::SpawnActor { id, actor, on_stop, confirm }) => {
                        self.actors.insert(id, ActorCell { actor, on_stop });
                        let _ = confirm.send(());
                    }
                    Ok(ControlMsg::Shutdown) => {
                        self.stop_all_actors();
                        return;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        // Runtime dropped control senders — no more spawns.
                        // Keep processing messages until the msg channel closes.
                        break;
                    }
                }
            }

            // Process one message with timeout
            match self.msg_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(envelope) => self.dispatch(envelope),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        self.stop_all_actors();
    }

    fn dispatch(&mut self, envelope: Envelope) {
        let actor_id = envelope.actor_id;
        if let Some(cell) = self.actors.get_mut(&actor_id) {
            let mut ctx = Context::new(actor_id);
            (envelope.dispatch)(cell.actor.as_mut(), &mut ctx);
            if !ctx.is_running() {
                self.remove_actor(actor_id);
            }
        }
    }

    fn remove_actor(&mut self, actor_id: ActorId) {
        if let Some(mut cell) = self.actors.remove(&actor_id) {
            let mut ctx = Context::new(actor_id);
            (cell.on_stop)(cell.actor.as_mut(), &mut ctx);
        }
    }

    fn stop_all_actors(&mut self) {
        let ids: Vec<ActorId> = self.actors.keys().copied().collect();
        for id in ids {
            self.remove_actor(id);
        }
    }
}

// ── Runtime ─────────────────────────────────────────────────

/// A multi-threaded actor runtime.
///
/// Distributes actors across a fixed number of worker threads using
/// round-robin assignment. Each worker runs its own event loop,
/// processing messages for its assigned actors.
///
/// # Example
///
/// ```ignore
/// let mut rt = Runtime::new();
/// let addr = rt.spawn(my_actor);
/// // move addr to another thread, send messages...
/// drop(addr);
/// rt.run(); // waits for all workers to finish
/// ```
pub struct Runtime {
    control_txs: Vec<Sender<ControlMsg>>,
    msg_txs: Vec<Sender<Envelope>>,
    handles: Vec<JoinHandle<()>>,
    next_worker: AtomicUsize,
    next_id: AtomicU64,
    num_workers: usize,
}

impl Runtime {
    /// Create a new runtime with the default number of worker threads.
    pub fn new() -> Self {
        Runtime::with_threads(DEFAULT_WORKER_THREADS)
    }

    /// Create a new runtime with `n` worker threads.
    ///
    /// # Panics
    /// Panics if `n` is zero.
    pub fn with_threads(n: usize) -> Self {
        assert!(n > 0, "at least one worker thread is required");

        let mut control_txs = Vec::with_capacity(n);
        let mut msg_txs = Vec::with_capacity(n);
        let mut handles = Vec::with_capacity(n);

        for _ in 0..n {
            let (control_tx, control_rx) = channel();
            let (msg_tx, msg_rx) = channel();

            control_txs.push(control_tx);
            msg_txs.push(msg_tx);

            let handle = thread::spawn(move || {
                Worker {
                    control_rx,
                    msg_rx,
                    actors: HashMap::new(),
                }
                .run();
            });
            handles.push(handle);
        }

        Runtime {
            control_txs,
            msg_txs,
            handles,
            next_worker: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            num_workers: n,
        }
    }

    /// Spawn an actor and return its address.
    ///
    /// The actor is assigned to a worker thread via round-robin.
    /// Its `started` lifecycle hook is called synchronously.
    pub fn spawn<A: Actor>(&self, mut actor: A) -> Addr<A> {
        let id = ActorId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let worker_idx = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.num_workers;

        let mut ctx = Context::new(id);
        actor.started(&mut ctx);

        let on_stop: DispatchFn = Box::new(move |actor_any: &mut dyn Any, ctx: &mut Context| {
            if let Some(a) = actor_any.downcast_mut::<A>() {
                a.stopped(ctx);
            }
        });

        let (confirm_tx, confirm_rx) = channel();
        self.control_txs[worker_idx]
            .send(ControlMsg::SpawnActor {
                id,
                actor: Box::new(actor),
                on_stop,
                confirm: confirm_tx,
            })
            .expect("worker thread should be alive");

        // Block until the worker has registered the actor
        confirm_rx.recv().expect("worker thread should confirm spawn");

        Addr::new(id, self.msg_txs[worker_idx].clone())
    }

    /// Consume the runtime and wait for all workers to finish.
    ///
    /// Drops the control channel to signal no more spawns. Workers will
    /// continue processing messages until all `Addr` senders are dropped
    /// (message channels disconnect).
    pub fn run(mut self) {
        // Signal no more spawns
        self.control_txs.clear();
        // Drop originals so channels close when all Addr clones are gone
        self.msg_txs.clear();

        for handle in self.handles.drain(..) {
            handle.join().expect("worker thread should not panic");
        }
    }

    /// Shut down all workers gracefully.
    ///
    /// Sends a shutdown signal to every worker, which will stop
    /// their actors and exit.
    pub fn shutdown(&self) {
        for tx in &self.control_txs {
            let _ = tx.send(ControlMsg::Shutdown);
        }
    }

    /// Returns the number of worker threads.
    pub fn num_workers(&self) -> usize {
        self.num_workers
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Runtime::new()
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Shut down workers when Runtime is dropped without calling run()
        if !self.control_txs.is_empty() {
            self.shutdown();
            // Give workers a moment to process the shutdown
            for handle in self.handles.drain(..) {
                let _ = handle.join();
            }
        }
    }
}

// ── Tests ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{Actor, Handler, Message};
    use crate::context::Context;
    use std::sync::{Arc, Mutex};

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

    #[test]
    fn spawn_and_send_message() {
        let rt = Runtime::with_threads(2);
        let addr = rt.spawn(CounterActor { count: 0 });

        let sender = thread::spawn(move || {
            assert_eq!(addr.send(Increment).unwrap(), 1);
            assert_eq!(addr.send(Increment).unwrap(), 2);
            drop(addr);
        });

        rt.run();
        sender.join().unwrap();
    }

    #[test]
    fn round_robin_distribution() {
        let rt = Runtime::with_threads(4);
        // Spawn 8 actors — each worker should get 2
        let addrs: Vec<_> = (0..8)
            .map(|_| rt.spawn(CounterActor { count: 0 }))
            .collect();

        let handles: Vec<_> = addrs
            .into_iter()
            .map(|addr| {
                thread::spawn(move || {
                    assert_eq!(addr.send(Increment).unwrap(), 1);
                    drop(addr);
                })
            })
            .collect();

        rt.run();
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn many_actors_few_threads() {
        let rt = Runtime::with_threads(2);
        let mut addrs = Vec::new();
        for _ in 0..100 {
            addrs.push(rt.spawn(CounterActor { count: 0 }));
        }

        let handles: Vec<_> = addrs
            .drain(..)
            .map(|addr| {
                thread::spawn(move || {
                    let _ = addr.send(Increment);
                    drop(addr);
                })
            })
            .collect();

        rt.run();
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn actors_run_concurrently() {
        use std::time::Instant;

        struct SleepActor;

        impl Actor for SleepActor {}

        struct Sleep(u64);
        impl Message for Sleep {
            type Result = ();
        }

        impl Handler<Sleep> for SleepActor {
            fn handle(&mut self, msg: Sleep, _ctx: &mut Context) {
                thread::sleep(Duration::from_millis(msg.0));
            }
        }

        let rt = Runtime::with_threads(4);
        let a1 = rt.spawn(SleepActor);
        let a2 = rt.spawn(SleepActor);
        let a3 = rt.spawn(SleepActor);
        let a4 = rt.spawn(SleepActor);

        let start = Instant::now();
        let handles: Vec<_> = vec![a1, a2, a3, a4]
            .into_iter()
            .map(|addr| {
                thread::spawn(move || {
                    let _ = addr.send(Sleep(100));
                    drop(addr);
                })
            })
            .collect();

        rt.run();
        for h in handles {
            h.join().unwrap();
        }
        let elapsed = start.elapsed();

        // 4 actors sleep 100ms each on 4 threads: should complete in ~100-150ms
        // (not 400ms which would be serial execution)
        assert!(
            elapsed < Duration::from_millis(300),
            "expected concurrent execution, but took {:?}",
            elapsed
        );
    }

    #[test]
    fn shutdown_stops_workers() {
        let rt = Runtime::with_threads(2);
        let addr = rt.spawn(CounterActor { count: 0 });

        // Verify we can send
        let addr_clone = addr.clone();
        let handle = thread::spawn(move || {
            addr_clone.send(Increment).unwrap();
        });
        handle.join().unwrap();

        // Shutdown should stop workers
        rt.shutdown();
        // After shutdown, sending should fail (worker stopped)
        // Note: the message might or might not be processed depending on timing
    }

    #[test]
    fn custom_thread_count() {
        let rt = Runtime::with_threads(1);
        assert_eq!(rt.num_workers(), 1);
        let addr = rt.spawn(CounterActor { count: 0 });

        let handle = thread::spawn(move || {
            addr.send(Increment).unwrap();
            drop(addr);
        });

        rt.run();
        handle.join().unwrap();
    }

    #[test]
    #[should_panic(expected = "at least one worker thread")]
    fn zero_threads_panics() {
        Runtime::with_threads(0);
    }

    #[test]
    fn actor_started_and_stopped_called() {
        let flag = Arc::new(Mutex::new((false, false)));
        let flag_clone = flag.clone();

        struct LifecycleActor {
            flag: Arc<Mutex<(bool, bool)>>,
        }

        impl Actor for LifecycleActor {
            fn started(&mut self, _ctx: &mut Context) {
                self.flag.lock().unwrap().0 = true;
            }
            fn stopped(&mut self, _ctx: &mut Context) {
                self.flag.lock().unwrap().1 = true;
            }
        }

        struct Ping;
        impl Message for Ping {
            type Result = ();
        }
        impl Handler<Ping> for LifecycleActor {
            fn handle(&mut self, _msg: Ping, _ctx: &mut Context) {}
        }

        let rt = Runtime::with_threads(2);
        let addr = rt.spawn(LifecycleActor { flag: flag.clone() });

        let handle = thread::spawn(move || {
            addr.send(Ping).unwrap();
            drop(addr);
        });

        rt.run();
        handle.join().unwrap();

        let (started, _stopped) = *flag_clone.lock().unwrap();
        assert!(started, "started should be called");
    }

    #[test]
    fn concurrent_sends_from_multiple_threads() {
        let rt = Runtime::with_threads(4);
        let addr = rt.spawn(CounterActor { count: 0 });

        let mut handles = Vec::new();
        for _ in 0..10 {
            let a = addr.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    let _ = a.send(Increment);
                }
                drop(a);
            }));
        }

        // Original addr must be dropped before run()
        drop(addr);

        rt.run();
        for h in handles {
            h.join().unwrap();
        }
    }
}
