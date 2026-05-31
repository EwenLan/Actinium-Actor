use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique identifier for an actor within the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActorId(pub(crate) u64);

impl ActorId {
    /// Create an ActorId from a raw u64 value.
    pub fn from_raw(id: u64) -> Self {
        ActorId(id)
    }

    #[allow(dead_code)]
    pub(crate) fn next() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        ActorId(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "actor-{}", self.0)
    }
}

/// Trait for messages that can be sent between actors.
pub trait Message: Send + 'static {
    type Result: Send + 'static;
}

/// The core trait that every actor must implement.
pub trait Actor: Send + Sized + 'static {
    fn started(&mut self, _ctx: &mut crate::context::Context) {}
    fn stopped(&mut self, _ctx: &mut crate::context::Context) {}
}

/// Trait for handling a specific message type.
///
/// An actor can implement `Handler<M>` for multiple message types.
pub trait Handler<M: Message>: Actor {
    fn handle(&mut self, msg: M, ctx: &mut crate::context::Context) -> M::Result;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_id_uniqueness() {
        let ids: Vec<ActorId> = (0..100).map(|_| ActorId::next()).collect();
        let mut dedup = ids.clone();
        dedup.sort_by_key(|id| id.0);
        dedup.dedup();
        assert_eq!(ids.len(), dedup.len());
    }

    #[test]
    fn actor_id_display() {
        let id = ActorId(42);
        assert_eq!(format!("{id}"), "actor-42");
    }

    #[test]
    fn actor_id_eq() {
        assert_eq!(ActorId(1), ActorId(1));
        assert_ne!(ActorId(1), ActorId(2));
    }

    // Compile-time verification that traits can be implemented
    struct TestActor;
    impl Actor for TestActor {}
    impl Handler<TestMessage> for TestActor {
        fn handle(&mut self, msg: TestMessage, _ctx: &mut crate::context::Context) -> String {
            msg.0
        }
    }

    struct TestMessage(String);
    impl Message for TestMessage {
        type Result = String;
    }

    #[test]
    fn handler_sends_result() {
        let mut actor = TestActor;
        let result = actor.handle(TestMessage("hello".into()), &mut crate::context::Context::dummy());
        assert_eq!(result, "hello");
    }
}
