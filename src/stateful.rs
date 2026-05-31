use std::marker::PhantomData;

use crate::actor::{Actor, Message};
use crate::context::Context;

// ── StateHandler trait ──────────────────────────────────────

/// Implemented by an actor to handle messages differently depending on
/// the current state in a state machine cycle.
///
/// After each message is processed, the state machine automatically
/// advances to the next state. When the last state's handler completes,
/// the cycle restarts from the initial state.
pub trait StateHandler<M: Message, S: Clone + Send + 'static>: Actor {
    /// Returns the initial state for the state machine.
    fn initial_state() -> S;

    /// Returns the ordered sequence of states. The machine cycles through
    /// these in order, processing one message per state.
    fn state_sequence() -> Vec<S>;

    /// Handle a message in the given state. `idx` is the position in the
    /// state sequence (0-based).
    fn handle_in_state(
        &mut self,
        idx: usize,
        state: &S,
        msg: M,
        ctx: &mut Context,
    ) -> M::Result;
}

// ── StateMachine wrapper ────────────────────────────────────

/// Wraps an actor that implements `StateHandler<M, S>` and provides
/// automatic state transitions.
///
/// The state machine cycles through the state sequence, processing one
/// message per state:
///
/// ```text
///   State[0] ──msg──> State[1] ──msg──> ... ──msg──> State[N-1]
///      ▲                                                 │
///      └─────────────────────────────────────────────────┘
/// ```
pub struct StateMachine<A, M, S>
where
    A: StateHandler<M, S>,
    M: Message,
    S: Clone + Send + 'static,
{
    inner: A,
    current: usize,
    states: Vec<S>,
    _msg: PhantomData<M>,
}

impl<A, M, S> StateMachine<A, M, S>
where
    A: StateHandler<M, S>,
    M: Message,
    S: Clone + Send + 'static,
{
    /// Create a new state machine wrapping the given actor.
    /// The actor starts in `A::initial_state()`.
    pub fn new(inner: A) -> Self {
        let states = A::state_sequence();
        assert!(!states.is_empty(), "state sequence must not be empty");
        StateMachine {
            inner,
            current: 0,
            states,
            _msg: PhantomData,
        }
    }

    /// Returns the index of the current state.
    pub fn current_index(&self) -> usize {
        self.current
    }

    /// Returns the current state.
    pub fn current_state(&self) -> &S {
        &self.states[self.current]
    }

    /// Returns a reference to the inner actor.
    pub fn inner(&self) -> &A {
        &self.inner
    }

    /// Returns a mutable reference to the inner actor.
    pub fn inner_mut(&mut self) -> &mut A {
        &mut self.inner
    }

    /// Consume the state machine and return the inner actor.
    pub fn into_inner(self) -> A {
        self.inner
    }
}

impl<A, M, S> Actor for StateMachine<A, M, S>
where
    A: StateHandler<M, S>,
    M: Message,
    S: Clone + Send + 'static,
{
    fn started(&mut self, ctx: &mut Context) {
        self.inner.started(ctx);
    }

    fn stopped(&mut self, ctx: &mut Context) {
        self.inner.stopped(ctx);
    }
}

impl<A, M, S> crate::actor::Handler<M> for StateMachine<A, M, S>
where
    A: StateHandler<M, S>,
    M: Message,
    S: Clone + Send + 'static,
{
    fn handle(&mut self, msg: M, ctx: &mut Context) -> M::Result {
        let idx = self.current;
        let state = self.states[idx].clone();
        let result = self.inner.handle_in_state(idx, &state, msg, ctx);
        // Advance to next state, cycle back to 0 after last
        self.current = (self.current + 1) % self.states.len();
        result
    }
}

// ── Tests ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Handler;

    // ── Test state machine ────────────────────────────────

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestState {
        Init,
        Process,
        Validate,
    }

    struct TestActor {
        log: Vec<String>,
    }

    impl Actor for TestActor {}

    struct TestMsg {
        text: String,
    }

    impl Message for TestMsg {
        type Result = String;
    }

    impl StateHandler<TestMsg, TestState> for TestActor {
        fn initial_state() -> TestState {
            TestState::Init
        }

        fn state_sequence() -> Vec<TestState> {
            vec![TestState::Init, TestState::Process, TestState::Validate]
        }

        fn handle_in_state(
            &mut self,
            idx: usize,
            state: &TestState,
            msg: TestMsg,
            _ctx: &mut Context,
        ) -> String {
            let entry = format!("{:?}({}): {}", state, idx, msg.text);
            self.log.push(entry.clone());
            entry
        }
    }

    // ── Tests ──────────────────────────────────────────────

    #[test]
    fn starts_at_initial_state() {
        let actor = TestActor { log: vec![] };
        let sm = StateMachine::<TestActor, TestMsg, TestState>::new(actor);
        assert_eq!(sm.current_index(), 0);
        assert_eq!(*sm.current_state(), TestState::Init);
    }

    #[test]
    fn cycles_through_states() {
        let actor = TestActor { log: vec![] };
        let mut sm = StateMachine::<TestActor, TestMsg, TestState>::new(actor);

        // Init state
        assert_eq!(sm.current_state(), &TestState::Init);
        let result = sm.handle(TestMsg { text: "first".into() }, &mut Context::dummy());
        assert_eq!(result, "Init(0): first");

        // Should now be in Process state
        assert_eq!(sm.current_state(), &TestState::Process);
        let result = sm.handle(TestMsg { text: "second".into() }, &mut Context::dummy());
        assert_eq!(result, "Process(1): second");

        // Should now be in Validate state
        assert_eq!(sm.current_state(), &TestState::Validate);
        let result = sm.handle(TestMsg { text: "third".into() }, &mut Context::dummy());
        assert_eq!(result, "Validate(2): third");

        // Should cycle back to Init
        assert_eq!(sm.current_state(), &TestState::Init);
    }

    #[test]
    fn cycles_multiple_times() {
        let actor = TestActor { log: vec![] };
        let mut sm = StateMachine::<TestActor, TestMsg, TestState>::new(actor);

        // Process 6 messages (2 complete cycles of 3 states)
        for i in 0..6 {
            sm.handle(TestMsg { text: format!("msg{}", i) }, &mut Context::dummy());
        }

        // After 6 messages: 6 % 3 = 0 → back to Init
        assert_eq!(sm.current_index(), 0);
        assert_eq!(*sm.current_state(), TestState::Init);
    }

    #[test]
    fn log_records_all_transitions() {
        let actor = TestActor { log: vec![] };
        let mut sm = StateMachine::<TestActor, TestMsg, TestState>::new(actor);

        sm.handle(TestMsg { text: "a".into() }, &mut Context::dummy());
        sm.handle(TestMsg { text: "b".into() }, &mut Context::dummy());
        sm.handle(TestMsg { text: "c".into() }, &mut Context::dummy());

        let log = &sm.inner().log;
        assert_eq!(log.len(), 3);
        assert_eq!(log[0], "Init(0): a");
        assert_eq!(log[1], "Process(1): b");
        assert_eq!(log[2], "Validate(2): c");
    }

    #[test]
    fn lifecycle_hooks_delegated() {
        use std::sync::{Arc, Mutex};

        struct LifecycleActor {
            started: Arc<Mutex<bool>>,
            stopped: Arc<Mutex<bool>>,
        }

        impl Actor for LifecycleActor {
            fn started(&mut self, _ctx: &mut Context) {
                *self.started.lock().unwrap() = true;
            }
            fn stopped(&mut self, _ctx: &mut Context) {
                *self.stopped.lock().unwrap() = true;
            }
        }

        #[derive(Clone)]
        enum S { A }

        struct M;
        impl Message for M { type Result = (); }

        impl StateHandler<M, S> for LifecycleActor {
            fn initial_state() -> S { S::A }
            fn state_sequence() -> Vec<S> { vec![S::A] }
            fn handle_in_state(&mut self, _: usize, _: &S, _: M, _: &mut Context) {}
        }

        let started = Arc::new(Mutex::new(false));
        let stopped = Arc::new(Mutex::new(false));

        let mut sm = StateMachine::<LifecycleActor, M, S>::new(LifecycleActor {
            started: started.clone(),
            stopped: stopped.clone(),
        });

        sm.started(&mut Context::dummy());
        assert!(*started.lock().unwrap());

        sm.stopped(&mut Context::dummy());
        assert!(*stopped.lock().unwrap());
    }

    #[test]
    #[should_panic(expected = "state sequence must not be empty")]
    fn empty_state_sequence_panics() {
        struct EmptyActor;
        impl Actor for EmptyActor {}

        #[derive(Clone)]
        enum S { A }

        struct M;
        impl Message for M { type Result = (); }

        impl StateHandler<M, S> for EmptyActor {
            fn initial_state() -> S { S::A }
            fn state_sequence() -> Vec<S> { vec![] }
            fn handle_in_state(&mut self, _: usize, _: &S, _: M, _: &mut Context) {}
        }

        StateMachine::<EmptyActor, M, S>::new(EmptyActor);
    }

    #[test]
    fn into_inner_returns_actor() {
        let actor = TestActor { log: vec![] };
        let sm = StateMachine::<TestActor, TestMsg, TestState>::new(actor);
        let inner = sm.into_inner();
        assert!(inner.log.is_empty());
    }
}
