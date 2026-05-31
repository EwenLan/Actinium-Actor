use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::actor::{Actor, ActorId};
use crate::addr::Addr;
use crate::context::Context;
use crate::envelope::{DispatchFn, Envelope};
use crate::scheduler::Scheduler;
use crate::spawner::{ActorCell, ActorMap, ControlMsg, SpawnShared};

pub const DEFAULT_WORKER_THREADS: usize = 4;

// ── Worker ──────────────────────────────────────────────────

struct Worker {
    control_rx: Receiver<ControlMsg>,
    msg_rx: Receiver<Envelope>,
    actor_map: ActorMap,
    scheduler: Scheduler,
    shared: Arc<SpawnShared>,
    worker_idx: usize,
}

impl Worker {
    fn run(mut self) {
        loop {
            loop {
                match self.control_rx.try_recv() {
                    Ok(ControlMsg::SpawnActor { id, actor, on_stop, confirm }) => {
                        self.actor_map
                            .lock()
                            .unwrap()
                            .insert(id, ActorCell { actor, on_stop });
                        let _ = confirm.send(());
                    }
                    Ok(ControlMsg::Shutdown) => {
                        self.stop_all_actors();
                        return;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }

            while let Ok(envelope) = self.msg_rx.try_recv() {
                self.scheduler.enqueue(envelope);
            }

            if let Some(actor_id) = self.scheduler.next_ready() {
                if let Some(envelope) = self.scheduler.dequeue(actor_id) {
                    self.dispatch(envelope);
                    self.scheduler.requeue_if_ready(actor_id);
                }
            } else {
                match self.msg_rx.recv_timeout(Duration::from_millis(10)) {
                    Ok(envelope) => self.scheduler.enqueue(envelope),
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        }

        self.stop_all_actors();
    }

    fn dispatch(&mut self, envelope: Envelope) {
        let actor_id = envelope.actor_id;

        // Take actor out of the shared map during dispatch
        let mut cell = match self.actor_map.lock().unwrap().remove(&actor_id) {
            Some(cell) => cell,
            None => return,
        };

        let shared = Arc::clone(&self.shared);
        let ctx = Context::with_spawner(actor_id, shared, self.worker_idx);

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut ctx = ctx;
            (envelope.dispatch)(cell.actor.as_mut(), &mut ctx);
            ctx.is_running()
        }));

        match result {
            Ok(running) => {
                if running {
                    // Put actor back — ctx.spawn may have added children
                    self.actor_map.lock().unwrap().insert(actor_id, cell);
                } else {
                    let shared = Arc::clone(&self.shared);
                    let mut ctx =
                        Context::with_spawner(actor_id, shared, self.worker_idx);
                    (cell.on_stop)(cell.actor.as_mut(), &mut ctx);
                    self.scheduler.remove_actor(actor_id);
                }
            }
            Err(e) => {
                let reason = if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "actor panicked".to_string()
                };
                eprintln!("[{}] panicked: {}", actor_id, reason);
                let shared = Arc::clone(&self.shared);
                let mut ctx =
                    Context::with_spawner(actor_id, shared, self.worker_idx);
                (cell.on_stop)(cell.actor.as_mut(), &mut ctx);
                self.scheduler.remove_actor(actor_id);
            }
        }
    }

    fn remove_actor(&mut self, actor_id: ActorId) {
        if let Some(mut cell) = self.shared.remove_actor(actor_id, self.worker_idx) {
            let shared = Arc::clone(&self.shared);
            let mut ctx = Context::with_spawner(actor_id, shared, self.worker_idx);
            (cell.on_stop)(cell.actor.as_mut(), &mut ctx);
        }
        self.scheduler.remove_actor(actor_id);
    }

    fn stop_all_actors(&mut self) {
        let ids: Vec<ActorId> =
            self.actor_map.lock().unwrap().keys().copied().collect();
        for id in ids {
            self.remove_actor(id);
        }
    }
}

// ── Runtime ─────────────────────────────────────────────────

pub struct Runtime {
    handles: Vec<JoinHandle<()>>,
    shared: Arc<SpawnShared>,
}

impl Runtime {
    pub fn new() -> Self {
        Runtime::with_threads(DEFAULT_WORKER_THREADS)
    }

    pub fn with_threads(n: usize) -> Self {
        assert!(n > 0, "at least one worker thread is required");

        let mut control_txs = Vec::with_capacity(n);
        let mut msg_txs = Vec::with_capacity(n);
        let mut actor_maps = Vec::with_capacity(n);
        let mut rx_pairs = Vec::with_capacity(n);

        for _ in 0..n {
            let (control_tx, control_rx) = channel();
            let (msg_tx, msg_rx) = channel();
            control_txs.push(control_tx);
            msg_txs.push(msg_tx);
            actor_maps.push(Arc::new(Mutex::new(HashMap::new())));
            rx_pairs.push((control_rx, msg_rx));
        }

        let shared = Arc::new(SpawnShared {
            control_txs,
            msg_txs,
            actor_maps: actor_maps.clone(),
            next_id: AtomicU64::new(1),
            next_worker: AtomicUsize::new(0),
            num_workers: n,
            running: AtomicBool::new(true),
        });

        let mut handles = Vec::with_capacity(n);
        for (i, (control_rx, msg_rx)) in rx_pairs.into_iter().enumerate() {
            let worker_shared = Arc::clone(&shared);
            let worker_map = Arc::clone(&actor_maps[i]);
            let handle = thread::spawn(move || {
                Worker {
                    control_rx,
                    msg_rx,
                    actor_map: worker_map,
                    scheduler: Scheduler::new(),
                    shared: worker_shared,
                    worker_idx: i,
                }
                .run();
            });
            handles.push(handle);
        }

        Runtime { handles, shared }
    }

    pub fn spawn<A: Actor>(&self, mut actor: A) -> Addr<A> {
        let id = self.shared.allocate_id();
        let worker_idx = self.shared.pick_worker();

        let mut ctx = Context::with_spawner(id, Arc::clone(&self.shared), worker_idx);
        actor.started(&mut ctx);

        let on_stop: DispatchFn = Box::new(move |actor_any, ctx| {
            if let Some(a) = actor_any.downcast_mut::<A>() {
                a.stopped(ctx);
            }
        });

        let msg_tx =
            self.shared
                .spawn_actor_remote(id, worker_idx, Box::new(actor), on_stop);

        Addr::new(id, msg_tx)
    }

    pub fn run(mut self) {
        thread::sleep(Duration::from_millis(5));
        self.shutdown();

        for handle in self.handles.drain(..) {
            handle.join().expect("worker thread should not panic");
        }
    }

    pub fn shutdown(&self) {
        for tx in &self.shared.control_txs {
            let _ = tx.send(ControlMsg::Shutdown);
        }
    }

    pub fn num_workers(&self) -> usize {
        self.shared.num_workers
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Runtime::new()
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if self.shared.running.load(std::sync::atomic::Ordering::SeqCst) {
            self.shutdown();
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
    use std::sync::{Arc, Mutex as StdMutex};

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
            drop(addr);
        });

        rt.run();
        sender.join().unwrap();
    }

    #[test]
    fn round_robin_distribution() {
        let rt = Runtime::with_threads(4);
        let addrs: Vec<_> = (0..8)
            .map(|_| rt.spawn(CounterActor { count: 0 }))
            .collect();

        let handles: Vec<_> = addrs
            .into_iter()
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
        let addrs: Vec<_> = (0..4).map(|_| rt.spawn(SleepActor)).collect();
        let start = Instant::now();

        let handles: Vec<_> = addrs
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

        assert!(start.elapsed() < Duration::from_millis(300));
    }

    #[test]
    fn shutdown_stops_workers() {
        let rt = Runtime::with_threads(2);
        let addr = rt.spawn(CounterActor { count: 0 });

        let addr_clone = addr.clone();
        let handle = thread::spawn(move || {
            addr_clone.send(Increment).unwrap();
        });
        handle.join().unwrap();

        rt.shutdown();
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
        let flag = Arc::new(StdMutex::new((false, false)));

        struct LifecycleActor {
            flag: Arc<StdMutex<(bool, bool)>>,
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
        let addr = rt.spawn(LifecycleActor {
            flag: flag.clone(),
        });

        let handle = thread::spawn(move || {
            addr.send(Ping).unwrap();
            drop(addr);
        });

        rt.run();
        handle.join().unwrap();

        assert!(flag.lock().unwrap().0);
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

        drop(addr);
        rt.run();
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn scheduler_fairness() {
        struct FairActor {
            id: usize,
            processed: Arc<StdMutex<Vec<usize>>>,
        }
        impl Actor for FairActor {}

        struct Work;
        impl Message for Work {
            type Result = ();
        }
        impl Handler<Work> for FairActor {
            fn handle(&mut self, _msg: Work, _ctx: &mut Context) {
                self.processed.lock().unwrap().push(self.id);
            }
        }

        let rt = Runtime::with_threads(1);
        let processed = Arc::new(StdMutex::new(Vec::new()));
        let addrs: Vec<_> = (0..3)
            .map(|i| {
                rt.spawn(FairActor {
                    id: i,
                    processed: processed.clone(),
                })
            })
            .collect();

        for addr in &addrs {
            for _ in 0..3 {
                let _ = addr.do_send(Work);
            }
        }

        let handles: Vec<_> = addrs
            .into_iter()
            .map(|addr| thread::spawn(move || drop(addr)))
            .collect();

        rt.run();
        for h in handles {
            h.join().unwrap();
        }

        let order = processed.lock().unwrap();
        assert_eq!(order.len(), 9);
        let first_three = &order[..3];
        let unique: std::collections::HashSet<_> = first_three.iter().collect();
        assert!(unique.len() > 1, "round-robin should interleave actors");
    }

    #[test]
    fn context_spawn_child_actor() {
        struct ParentActor {
            child_count: Arc<StdMutex<usize>>,
        }

        impl Actor for ParentActor {}

        struct SpawnChild;
        impl Message for SpawnChild {
            type Result = ();
        }

        impl Handler<SpawnChild> for ParentActor {
            fn handle(&mut self, _msg: SpawnChild, ctx: &mut Context) {
                let child = ChildActor;
                let _child_addr = ctx.spawn(child);
                *self.child_count.lock().unwrap() += 1;
            }
        }

        struct ChildActor;

        impl Actor for ChildActor {
            fn started(&mut self, _ctx: &mut Context) {}
        }

        struct Ping;
        impl Message for Ping {
            type Result = ();
        }
        impl Handler<Ping> for ChildActor {
            fn handle(&mut self, _msg: Ping, _ctx: &mut Context) {}
        }

        let rt = Runtime::with_threads(2);
        let child_count = Arc::new(StdMutex::new(0));
        let parent = rt.spawn(ParentActor {
            child_count: child_count.clone(),
        });

        let handle = thread::spawn(move || {
            parent.send(SpawnChild).unwrap();
            parent.send(SpawnChild).unwrap();
            drop(parent);
        });

        handle.join().unwrap();
        drop(rt);

        assert_eq!(*child_count.lock().unwrap(), 2);
    }

    #[test]
    fn panic_isolation() {
        struct PanicActor;
        impl Actor for PanicActor {}

        struct TriggerPanic;
        impl Message for TriggerPanic {
            type Result = ();
        }
        impl Handler<TriggerPanic> for PanicActor {
            fn handle(&mut self, _msg: TriggerPanic, _ctx: &mut Context) {
                panic!("deliberate panic in actor");
            }
        }

        let rt = Runtime::with_threads(1);
        let addr = rt.spawn(PanicActor);

        let handle = thread::spawn(move || {
            let _ = addr.send(TriggerPanic);
            drop(addr);
        });

        rt.run();
        handle.join().unwrap();
    }
}
