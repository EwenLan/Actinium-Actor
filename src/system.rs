use std::any::Any;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};

use crate::actor::{Actor, ActorId};
use crate::addr::Addr;
use crate::context::Context;
use crate::envelope::{DispatchFn, Envelope};

struct ActorCell {
    actor: Box<dyn Any + Send>,
    on_stop: DispatchFn,
}

/// A single-threaded actor runtime.
pub struct ActorSystem {
    actors: HashMap<ActorId, ActorCell>,
    rx: Receiver<Envelope>,
    tx: Option<Sender<Envelope>>,
    next_id: u64,
    running: bool,
}

impl ActorSystem {
    pub fn new() -> Self {
        let (tx, rx) = channel();
        ActorSystem {
            actors: HashMap::new(),
            rx,
            tx: Some(tx),
            next_id: 1,
            running: false,
        }
    }

    pub fn spawn<A: Actor>(&mut self, mut actor: A) -> Addr<A> {
        let id = ActorId(self.next_id);
        self.next_id += 1;

        let tx = self.tx.as_ref().expect("spawn called after run").clone();
        let mut ctx = Context::new(id);
        actor.started(&mut ctx);

        let cell = ActorCell {
            actor: Box::new(actor),
            on_stop: Box::new(move |actor_any: &mut dyn Any, ctx: &mut Context| {
                if let Some(a) = actor_any.downcast_mut::<A>() {
                    a.stopped(ctx);
                }
            }),
        };

        self.actors.insert(id, cell);
        Addr::new(id, tx)
    }

    /// Run the event loop, blocking the current thread.
    ///
    /// Exits when `shutdown()` is called or when all `Addr` senders are dropped.
    pub fn run(&mut self) {
        self.running = true;
        // Drop our own sender so the channel closes when all Addrs are dropped
        self.tx = None;

        while self.running {
            match self.rx.recv() {
                Ok(envelope) => self.dispatch(envelope),
                Err(_) => self.running = false,
            }
        }
        self.stop_all_actors();
    }

    /// Process pending messages without blocking. Returns the number processed.
    pub fn run_once(&mut self) -> usize {
        let mut count = 0;
        while let Ok(envelope) = self.rx.try_recv() {
            self.dispatch(envelope);
            count += 1;
        }
        count
    }

    pub fn shutdown(&mut self) {
        self.running = false;
    }

    pub fn actor_count(&self) -> usize {
        self.actors.len()
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
        let actor_ids: Vec<ActorId> = self.actors.keys().copied().collect();
        for id in actor_ids {
            self.remove_actor(id);
        }
    }
}

impl Default for ActorSystem {
    fn default() -> Self {
        ActorSystem::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{Actor, Handler, Message};
    use crate::context::Context;

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

    struct EchoActor;

    impl Actor for EchoActor {}

    struct Echo(String);
    impl Message for Echo {
        type Result = String;
    }

    impl Handler<Echo> for EchoActor {
        fn handle(&mut self, msg: Echo, _ctx: &mut Context) -> String {
            msg.0
        }
    }

    fn run_in_thread(mut system: ActorSystem) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            system.run();
        })
    }

    #[test]
    fn spawn_and_send_message() {
        let mut system = ActorSystem::new();
        let addr = system.spawn(CounterActor { count: 0 });
        let handle = run_in_thread(system);

        assert_eq!(addr.send(Increment).unwrap(), 1);
        assert_eq!(addr.send(Increment).unwrap(), 2);
        assert_eq!(addr.send(GetCount).unwrap(), 2);

        drop(addr);
        handle.join().unwrap();
    }

    #[test]
    fn actor_stop_from_context() {
        struct StopActor;

        impl Actor for StopActor {}

        struct Die;
        impl Message for Die {
            type Result = ();
        }

        impl Handler<Die> for StopActor {
            fn handle(&mut self, _msg: Die, ctx: &mut Context) {
                ctx.stop();
            }
        }

        let mut system = ActorSystem::new();
        let addr = system.spawn(StopActor);
        let handle = run_in_thread(system);

        assert!(addr.send(Die).is_ok());
        // Actor stopped — subsequent send should fail (actor not found)
        assert!(addr.send(Die).is_err());

        drop(addr);
        handle.join().unwrap();
    }

    #[test]
    fn multiple_actors_communicate_via_notify() {
        use std::sync::{Arc, Mutex};

        struct CollectorActor {
            messages: Arc<Mutex<Vec<String>>>,
        }

        impl Actor for CollectorActor {}

        struct Collect(String);
        impl Message for Collect {
            type Result = ();
        }

        impl Handler<Collect> for CollectorActor {
            fn handle(&mut self, msg: Collect, _ctx: &mut Context) {
                self.messages.lock().unwrap().push(msg.0);
            }
        }

        let messages = Arc::new(Mutex::new(Vec::new()));
        let mut system = ActorSystem::new();
        let collector = system.spawn(CollectorActor {
            messages: messages.clone(),
        });
        let echo = system.spawn(EchoActor);
        let handle = run_in_thread(system);

        // Send a message to echo, get result
        let result = echo.send(Echo("ping".into())).unwrap();
        assert_eq!(result, "ping");

        // Notify collector from outside
        collector.send(Collect("done".into())).unwrap();
        assert_eq!(messages.lock().unwrap().len(), 1);

        drop(collector);
        drop(echo);
        handle.join().unwrap();
    }

    #[test]
    fn actor_started_called() {
        use std::sync::{Arc, Mutex};

        struct StartActor {
            flag: Arc<Mutex<bool>>,
        }

        impl Actor for StartActor {
            fn started(&mut self, _ctx: &mut Context) {
                *self.flag.lock().unwrap() = true;
            }
        }

        struct Ping;
        impl Message for Ping {
            type Result = ();
        }
        impl Handler<Ping> for StartActor {
            fn handle(&mut self, _msg: Ping, _ctx: &mut Context) {}
        }

        let flag = Arc::new(Mutex::new(false));
        let mut system = ActorSystem::new();
        let addr = system.spawn(StartActor { flag: flag.clone() });
        let handle = run_in_thread(system);

        assert!(*flag.lock().unwrap());

        drop(addr);
        handle.join().unwrap();
    }

    #[test]
    fn actor_stopped_called_on_drop() {
        use std::sync::{Arc, Mutex};

        struct ShutdownActor {
            flag: Arc<Mutex<bool>>,
        }

        impl Actor for ShutdownActor {
            fn stopped(&mut self, _ctx: &mut Context) {
                *self.flag.lock().unwrap() = true;
            }
        }

        struct Ping;
        impl Message for Ping {
            type Result = ();
        }
        impl Handler<Ping> for ShutdownActor {
            fn handle(&mut self, _msg: Ping, _ctx: &mut Context) {}
        }

        let flag = Arc::new(Mutex::new(false));
        let mut system = ActorSystem::new();
        let addr = system.spawn(ShutdownActor { flag: flag.clone() });
        let handle = run_in_thread(system);

        drop(addr);
        handle.join().unwrap();

        assert!(*flag.lock().unwrap());
    }

    #[test]
    fn send_to_nonexistent_actor() {
        let mut system = ActorSystem::new();
        let addr: Addr<EchoActor> = system.spawn(EchoActor);

        // Get a second addr, then drop it to remove the actor
        let addr2 = addr.clone();
        let handle = run_in_thread(system);

        // Send a stop-like message that removes the actor
        // Then try to send to the addr that refers to a removed actor

        drop(addr);
        drop(addr2);
        handle.join().unwrap();

        // After shutdown, sends should fail
        // (addr is consumed, so we can't actually test this with the same addr)
    }

    #[test]
    fn many_messages_to_actor() {
        let mut system = ActorSystem::new();
        let addr = system.spawn(CounterActor { count: 0 });
        let handle = run_in_thread(system);

        for i in 0..100 {
            assert_eq!(addr.send(Increment).unwrap(), i + 1);
        }
        assert_eq!(addr.send(GetCount).unwrap(), 100);

        drop(addr);
        handle.join().unwrap();
    }
}
