use crate::actor::ActorId;
use crate::addr::Addr;
use crate::actor::{Actor, Handler, Message};

/// Execution context provided to an actor during message handling.
pub struct Context {
    pub(crate) actor_id: ActorId,
    pub(crate) running: bool,
}

impl Context {
    pub(crate) fn new(actor_id: ActorId) -> Self {
        Context {
            actor_id,
            running: true,
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
    ) -> Result<M::Result, crate::addr::SendError> {
        addr.send(msg)
    }

    /// Notify another actor without waiting for a result.
    /// Safe to use for inter-actor communication within the same runtime.
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

    /// Returns true if the actor should continue running.
    pub(crate) fn is_running(&self) -> bool {
        self.running
    }

    #[cfg(test)]
    pub(crate) fn dummy() -> Self {
        Context {
            actor_id: ActorId(0),
            running: true,
        }
    }
}
