use std::any::Any;
use std::sync::Arc;

use crate::actor::ActorId;
use crate::actor::{Actor, Handler, Message};
use crate::addr::{Addr, SendError};
use crate::envelope::DispatchFn;

/// Execution context provided to an actor during message handling.
pub struct Context {
    pub(crate) actor_id: ActorId,
    pub(crate) running: bool,
    pub(crate) spawn_shared: Option<Arc<crate::spawner::SpawnShared>>,
    pub(crate) worker_idx: usize,
}

impl Context {
    pub(crate) fn new(actor_id: ActorId) -> Self {
        Context {
            actor_id,
            running: true,
            spawn_shared: None,
            worker_idx: 0,
        }
    }

    pub(crate) fn with_spawner(
        actor_id: ActorId,
        shared: Arc<crate::spawner::SpawnShared>,
        worker_idx: usize,
    ) -> Self {
        Context {
            actor_id,
            running: true,
            spawn_shared: Some(shared),
            worker_idx,
        }
    }

    /// Returns this actor's unique identifier.
    pub fn id(&self) -> ActorId {
        self.actor_id
    }

    /// Send a message to another actor and wait for the result.
    pub fn send<A: Actor + Handler<M>, M: Message>(
        &self,
        addr: &Addr<A>,
        msg: M,
    ) -> Result<M::Result, SendError> {
        addr.send(msg)
    }

    /// Notify another actor without waiting for a result.
    pub fn notify<A, M>(&self, addr: &Addr<A>, msg: M)
    where
        A: Actor + Handler<M>,
        M: Message,
    {
        let _ = addr.do_send(msg);
    }

    /// Request this actor to stop after the current message is processed.
    pub fn stop(&mut self) {
        self.running = false;
    }

    /// Spawn a child actor on the same worker. Returns its address.
    ///
    /// Inserts the child directly into the worker's actor map, avoiding
    /// the deadlock that would occur from sending a control message to
    /// the currently dispatching worker.
    ///
    /// # Panics
    /// Panics if called outside of a Runtime.
    pub fn spawn<A: Actor>(&mut self, mut actor: A) -> Addr<A> {
        let shared = self
            .spawn_shared
            .as_ref()
            .expect("Context::spawn requires a multi-threaded Runtime");

        let id = shared.allocate_id();

        let child_shared = Arc::clone(shared);
        let mut child_ctx = Context::with_spawner(id, child_shared, self.worker_idx);
        actor.started(&mut child_ctx);

        let on_stop: DispatchFn = Box::new(move |actor_any: &mut dyn Any, ctx: &mut Context| {
            if let Some(a) = actor_any.downcast_mut::<A>() {
                a.stopped(ctx);
            }
        });

        // Directly insert into the worker's actor map (no channel, no deadlock)
        let msg_tx =
            shared.spawn_actor_direct(id, self.worker_idx, Box::new(actor), on_stop);

        Addr::new(id, msg_tx)
    }

    /// Returns true if the actor should continue running.
    pub(crate) fn is_running(&self) -> bool {
        self.running
    }

    #[cfg(test)]
    pub(crate) fn dummy() -> Self {
        Context {
            actor_id: ActorId(0),
            running: true,
            spawn_shared: None,
            worker_idx: 0,
        }
    }
}
