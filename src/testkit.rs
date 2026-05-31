use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::actor::{Actor, ActorId, Handler, Message};
use crate::addr::Addr;
use crate::context::Context;
use crate::system::ActorSystem;

// ── TestKit ─────────────────────────────────────────────────

/// A single-threaded test environment for actors.
///
/// Wraps an `ActorSystem` to provide deterministic, synchronous message
/// processing. Use `run_until_idle()` to drain all pending messages.
pub struct TestKit {
    system: ActorSystem,
}

impl TestKit {
    /// Create a new test kit.
    pub fn new() -> Self {
        TestKit {
            system: ActorSystem::new(),
        }
    }

    /// Spawn an actor and return its address.
    pub fn spawn<A: Actor>(&mut self, actor: A) -> Addr<A> {
        self.system.spawn(actor)
    }

    /// Spawn a test probe that records messages of type `M`.
    ///
    /// Use `probe.count()`, `probe.first()`, and `probe.all()` to assert
    /// on received messages.
    pub fn spawn_probe<M>(&mut self) -> TestProbe<M>
    where
        M: Message,
        M::Result: Default,
    {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let addr = self.system.spawn(ProbeActor {
            messages: messages.clone(),
            _marker: PhantomData::<M>,
        });
        TestProbe { messages, addr }
    }

    /// Process all pending messages without blocking.
    /// Returns when no more messages are in the channel.
    pub fn run_until_idle(&mut self) {
        while self.system.run_once() > 0 {}
    }

    /// Access the underlying ActorSystem.
    pub fn system(&mut self) -> &mut ActorSystem {
        &mut self.system
    }
}

impl Default for TestKit {
    fn default() -> Self {
        TestKit::new()
    }
}

// ── TestProbe ───────────────────────────────────────────────

/// Collects messages sent to a probe actor for test assertions.
///
/// Created via `TestKit::spawn_probe::<M>()`.
pub struct TestProbe<M: Message> {
    messages: Arc<Mutex<Vec<M>>>,
    addr: Addr<ProbeActor<M>>,
}

impl<M: Message> TestProbe<M> {
    /// Returns the address of the probe actor. Other actors can send
    /// messages to this address, and the probe will record them.
    pub fn addr(&self) -> &Addr<ProbeActor<M>> {
        &self.addr
    }

    /// Returns the ActorId of the probe.
    pub fn id(&self) -> ActorId {
        self.addr.id()
    }

    /// Returns a cloned address that can be given to other actors.
    pub fn recipient(&self) -> Addr<ProbeActor<M>> {
        self.addr.clone()
    }

    /// Number of messages received.
    pub fn count(&self) -> usize {
        self.messages.lock().unwrap().len()
    }

    /// Returns true if at least one message has been received.
    pub fn received_any(&self) -> bool {
        self.count() > 0
    }

    /// Returns the first message received, if any.
    pub fn first(&self) -> Option<M>
    where
        M: Clone,
    {
        self.messages.lock().unwrap().first().cloned()
    }

    /// Returns all received messages in order.
    pub fn all(&self) -> Vec<M>
    where
        M: Clone,
    {
        self.messages.lock().unwrap().clone()
    }

    /// Clears all recorded messages.
    pub fn reset(&self) {
        self.messages.lock().unwrap().clear();
    }

    /// Returns true if any received message matches the predicate.
    pub fn any_match(&self, predicate: impl Fn(&M) -> bool) -> bool {
        self.messages.lock().unwrap().iter().any(predicate)
    }
}

impl<M: Message> Clone for TestProbe<M> {
    fn clone(&self) -> Self {
        TestProbe {
            messages: Arc::clone(&self.messages),
            addr: self.addr.clone(),
        }
    }
}

// ── ProbeActor ──────────────────────────────────────────────

/// Actor that records messages for test assertions.
#[doc(hidden)]
pub struct ProbeActor<M: Message> {
    messages: Arc<Mutex<Vec<M>>>,
    _marker: PhantomData<M>,
}

impl<M: Message> Actor for ProbeActor<M> {}

impl<M: Message> Handler<M> for ProbeActor<M>
where
    M::Result: Default,
{
    fn handle(&mut self, msg: M, _ctx: &mut Context) -> M::Result {
        self.messages.lock().unwrap().push(msg);
        M::Result::default()
    }
}

// ── Tests ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Test actors ────────────────────────────────────────

    #[derive(Clone)]
    struct Echo(String);
    impl Message for Echo {
        type Result = ();
    }

    // ── Tests ──────────────────────────────────────────────

    #[test]
    fn probe_receives_messages() {
        let mut kit = TestKit::new();
        let probe = kit.spawn_probe::<Echo>();

        let addr = probe.recipient();
        std::thread::spawn(move || {
            let _ = addr.do_send(Echo("hello".into()));
            drop(addr);
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        kit.run_until_idle();
    }

    #[test]
    fn probe_count_and_first() {
        let mut kit = TestKit::new();
        let probe = kit.spawn_probe::<Echo>();

        let addr = probe.recipient();
        std::thread::spawn(move || {
            let _ = addr.do_send(Echo("first".into()));
            let _ = addr.do_send(Echo("second".into()));
            drop(addr);
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        kit.run_until_idle();

        assert_eq!(probe.count(), 2);
        assert!(probe.received_any());
        assert_eq!(probe.first().unwrap().0, "first");
        assert_eq!(probe.all().len(), 2);
    }

    #[test]
    fn probe_reset() {
        let mut kit = TestKit::new();
        let probe = kit.spawn_probe::<Echo>();
        let addr = probe.recipient();

        std::thread::spawn(move || {
            let _ = addr.do_send(Echo("test".into()));
            drop(addr);
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        kit.run_until_idle();
        assert_eq!(probe.count(), 1);

        probe.reset();
        assert_eq!(probe.count(), 0);
    }

    #[test]
    fn probe_any_match() {
        let mut kit = TestKit::new();
        let probe = kit.spawn_probe::<Echo>();
        let addr = probe.recipient();

        std::thread::spawn(move || {
            let _ = addr.do_send(Echo("apple".into()));
            let _ = addr.do_send(Echo("banana".into()));
            drop(addr);
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        kit.run_until_idle();

        assert!(probe.any_match(|e| e.0.contains("banana")));
        assert!(!probe.any_match(|e| e.0.contains("cherry")));
    }

    #[test]
    fn spawn_actor_in_kit() {
        let mut kit = TestKit::new();

        struct Counter {
            count: usize,
        }
        impl Actor for Counter {}

        struct Inc;
        impl Message for Inc {
            type Result = usize;
        }
        impl Handler<Inc> for Counter {
            fn handle(&mut self, _msg: Inc, _ctx: &mut Context) -> usize {
                self.count += 1;
                self.count
            }
        }

        let addr = kit.spawn(Counter { count: 0 });

        let addr_clone = addr.clone();
        std::thread::spawn(move || {
            let _ = addr_clone.do_send(Inc);
            let _ = addr_clone.do_send(Inc);
            drop(addr_clone);
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        kit.run_until_idle();
    }
}
