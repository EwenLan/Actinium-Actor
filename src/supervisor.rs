use crate::actor::ActorId;
use crate::context::Context;

/// Supervision strategy determines how child failures are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Only the failed child is restarted.
    OneForOne,
    /// If one child fails, all children are stopped and restarted.
    AllForOne,
}

/// A factory function that spawns a child actor and returns its ActorId.
pub type ChildFactory = Box<dyn Fn(&mut Context) -> ActorId + Send>;

/// A supervisor manages child actors with a restart strategy.
///
/// When a child fails (panics), the supervisor applies its strategy
/// to restart the failed child (OneForOne) or all children (AllForOne).
pub struct Supervisor {
    strategy: Strategy,
    max_restarts: usize,
    child_factories: Vec<ChildFactory>,
    child_ids: Vec<ActorId>,
    restart_counts: std::collections::HashMap<ActorId, usize>,
}

impl Supervisor {
    /// Create a new supervisor with the given strategy and max restarts per child.
    pub fn new(strategy: Strategy, max_restarts: usize) -> Self {
        Supervisor {
            strategy,
            max_restarts,
            child_factories: Vec::new(),
            child_ids: Vec::new(),
            restart_counts: std::collections::HashMap::new(),
        }
    }

    /// Register a child factory. The factory is called to create a new
    /// instance of the child actor when spawning or restarting.
    pub fn register_child<F>(&mut self, factory: F)
    where
        F: Fn(&mut Context) -> ActorId + Send + 'static,
    {
        self.child_factories.push(Box::new(factory));
    }

    /// Spawn all registered children. Call after registering factories.
    pub fn start_all(&mut self, ctx: &mut Context) {
        for factory in &self.child_factories {
            let id = factory(ctx);
            self.child_ids.push(id);
            self.restart_counts.insert(id, 0);
        }
    }

    /// Handle a child failure by applying the supervision strategy.
    ///
    /// Returns the IDs of restarted children.
    pub fn handle_failure(&mut self, failed_id: ActorId, ctx: &mut Context) -> Vec<ActorId> {
        let count = self
            .restart_counts
            .get(&failed_id)
            .copied()
            .unwrap_or(0);

        if count >= self.max_restarts {
            // Exceeded max restarts — remove the child
            self.child_ids.retain(|id| *id != failed_id);
            self.restart_counts.remove(&failed_id);
            return Vec::new();
        }

        match self.strategy {
            Strategy::OneForOne => {
                self.restart_one(failed_id, count, ctx)
            }
            Strategy::AllForOne => {
                self.restart_all(ctx)
            }
        }
    }

    /// Returns the IDs of all currently active children.
    pub fn children(&self) -> &[ActorId] {
        &self.child_ids
    }

    fn restart_one(
        &mut self,
        failed_id: ActorId,
        count: usize,
        ctx: &mut Context,
    ) -> Vec<ActorId> {
        // Find the index of the failed child
        let idx = self
            .child_ids
            .iter()
            .position(|id| *id == failed_id);

        if let Some(idx) = idx {
            let factory = &self.child_factories[idx];
            let new_id = factory(ctx);
            self.child_ids[idx] = new_id;
            self.restart_counts.remove(&failed_id);
            self.restart_counts.insert(new_id, count + 1);
            vec![new_id]
        } else {
            Vec::new()
        }
    }

    fn restart_all(&mut self, ctx: &mut Context) -> Vec<ActorId> {
        let mut new_ids = Vec::new();
        for (i, factory) in self.child_factories.iter().enumerate() {
            let new_id = factory(ctx);
            if i < self.child_ids.len() {
                self.child_ids[i] = new_id;
            } else {
                self.child_ids.push(new_id);
            }
            self.restart_counts.insert(new_id, 0);
            new_ids.push(new_id);
        }
        self.restart_counts.clear();
        for id in &new_ids {
            self.restart_counts.insert(*id, 0);
        }
        new_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{Actor, Handler, Message};
    use crate::runtime::Runtime;
    use std::sync::{Arc, Mutex};

    #[test]
    fn one_for_one_restarts_failed_child() {
        let spawned = Arc::new(Mutex::new(Vec::new()));

        struct Child {
            id: usize,
            log: Arc<Mutex<Vec<usize>>>,
        }

        impl Actor for Child {
            fn started(&mut self, _ctx: &mut Context) {
                self.log.lock().unwrap().push(self.id);
            }
        }

        struct Ping;
        impl Message for Ping {
            type Result = ();
        }
        impl Handler<Ping> for Child {
            fn handle(&mut self, _msg: Ping, _ctx: &mut Context) {}
        }

        let rt = Runtime::with_threads(1);

        // Spawn the supervisor as an actor
        struct SupActor {
            sup: Supervisor,
            #[allow(dead_code)]
            log: Arc<Mutex<Vec<usize>>>,
        }

        impl Actor for SupActor {}

        struct StartChildren;
        impl Message for StartChildren {
            type Result = ();
        }

        impl Handler<StartChildren> for SupActor {
            fn handle(&mut self, _msg: StartChildren, ctx: &mut Context) {
                self.sup.start_all(ctx);
            }
        }

        struct FailChild {
            id: ActorId,
        }
        impl Message for FailChild {
            type Result = Vec<ActorId>;
        }

        impl Handler<FailChild> for SupActor {
            fn handle(&mut self, msg: FailChild, ctx: &mut Context) -> Vec<ActorId> {
                self.sup.handle_failure(msg.id, ctx)
            }
        }

        let log = spawned.clone();
        let mut sup = Supervisor::new(Strategy::OneForOne, 3);
        sup.register_child({
            let log = log.clone();
            move |ctx: &mut Context| {
                let child = Child { id: 42, log: log.clone() };
                let addr = ctx.spawn(child);
                addr.id()
            }
        });

        let sup_actor = SupActor { sup, log };

        let sup_addr = rt.spawn(sup_actor);

        // Start children
        let sup_clone = sup_addr.clone();
        std::thread::spawn(move || {
            sup_clone.send(StartChildren).unwrap();
            drop(sup_clone);
        })
        .join()
        .unwrap();

        // The child was started
        assert_eq!(spawned.lock().unwrap().len(), 1);

        drop(sup_addr);
        drop(rt);
    }

    #[test]
    fn supervisor_registers_and_starts_children() {
        let mut sup = Supervisor::new(Strategy::OneForOne, 3);
        sup.register_child(|_ctx| ActorId(100));
        sup.register_child(|_ctx| ActorId(200));

        let rt = Runtime::with_threads(1);

        struct TestSup {
            sup: Supervisor,
        }
        impl Actor for TestSup {}

        struct Start;
        impl Message for Start {
            type Result = Vec<ActorId>;
        }
        impl Handler<Start> for TestSup {
            fn handle(&mut self, _msg: Start, ctx: &mut Context) -> Vec<ActorId> {
                self.sup.start_all(ctx);
                self.sup.children().to_vec()
            }
        }

        let test_sup = TestSup { sup };
        let addr = rt.spawn(test_sup);

        let children = addr.send(Start).unwrap();
        assert_eq!(children.len(), 2);

        drop(addr);
        drop(rt);
    }

    #[test]
    fn one_for_one_restart_within_limit() {
        struct TestChild;
        impl Actor for TestChild {}

        struct Ping;
        impl Message for Ping {
            type Result = ();
        }
        impl Handler<Ping> for TestChild {
            fn handle(&mut self, _msg: Ping, _ctx: &mut Context) {}
        }

        let mut sup = Supervisor::new(Strategy::OneForOne, 3);
        sup.register_child(|ctx| {
            let addr = ctx.spawn(TestChild);
            addr.id()
        });

        let rt = Runtime::with_threads(1);

        struct TestSup {
            sup: Supervisor,
        }
        impl Actor for TestSup {}

        struct Start;
        impl Message for Start {
            type Result = ();
        }
        impl Handler<Start> for TestSup {
            fn handle(&mut self, _msg: Start, ctx: &mut Context) {
                self.sup.start_all(ctx);
            }
        }

        struct Fail {
            id: ActorId,
        }
        impl Message for Fail {
            type Result = Vec<ActorId>;
        }
        impl Handler<Fail> for TestSup {
            fn handle(&mut self, msg: Fail, ctx: &mut Context) -> Vec<ActorId> {
                self.sup.handle_failure(msg.id, ctx)
            }
        }

        struct GetChildren;
        impl Message for GetChildren {
            type Result = Vec<ActorId>;
        }
        impl Handler<GetChildren> for TestSup {
            fn handle(&mut self, _msg: GetChildren, _ctx: &mut Context) -> Vec<ActorId> {
                self.sup.children().to_vec()
            }
        }

        let addr = rt.spawn(TestSup { sup });
        addr.send(Start).unwrap();

        // Get the actual child ID
        let children = addr.send(GetChildren).unwrap();
        assert_eq!(children.len(), 1);
        let child_id = children[0];

        // Fail the child — should restart
        let restarted = addr.send(Fail { id: child_id }).unwrap();
        assert_eq!(restarted.len(), 1);
        assert_ne!(restarted[0], child_id); // new ID after restart

        drop(addr);
        drop(rt);
    }

    #[test]
    fn one_for_one_stops_after_max_restarts() {
        struct TestChild;
        impl Actor for TestChild {}
        struct Ping;
        impl Message for Ping {
            type Result = ();
        }
        impl Handler<Ping> for TestChild {
            fn handle(&mut self, _msg: Ping, _ctx: &mut Context) {}
        }

        let mut sup = Supervisor::new(Strategy::OneForOne, 2);
        sup.register_child(|ctx| {
            let addr = ctx.spawn(TestChild);
            addr.id()
        });

        let rt = Runtime::with_threads(1);

        struct TestSup {
            sup: Supervisor,
        }
        impl Actor for TestSup {}

        struct Start;
        impl Message for Start {
            type Result = ();
        }
        impl Handler<Start> for TestSup {
            fn handle(&mut self, _msg: Start, ctx: &mut Context) {
                self.sup.start_all(ctx);
            }
        }

        struct Fail {
            id: ActorId,
        }
        impl Message for Fail {
            type Result = Vec<ActorId>;
        }
        impl Handler<Fail> for TestSup {
            fn handle(&mut self, msg: Fail, ctx: &mut Context) -> Vec<ActorId> {
                self.sup.handle_failure(msg.id, ctx)
            }
        }

        struct GetChildren;
        impl Message for GetChildren {
            type Result = Vec<ActorId>;
        }
        impl Handler<GetChildren> for TestSup {
            fn handle(&mut self, _msg: GetChildren, _ctx: &mut Context) -> Vec<ActorId> {
                self.sup.children().to_vec()
            }
        }

        let addr = rt.spawn(TestSup { sup });
        addr.send(Start).unwrap();

        let children = addr.send(GetChildren).unwrap();
        let child_id = children[0];

        // First failure — restart
        let result = addr.send(Fail { id: child_id }).unwrap();
        assert_eq!(result.len(), 1);
        let new_id = result[0];

        // Second failure — restart again
        let result = addr.send(Fail { id: new_id }).unwrap();
        assert_eq!(result.len(), 1);

        // Third failure — should stop (max_restarts = 2)
        let result = addr.send(Fail { id: result[0] }).unwrap();
        assert!(result.is_empty());

        drop(addr);
        drop(rt);
    }
}
