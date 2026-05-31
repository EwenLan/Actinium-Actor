use std::marker::PhantomData;
use std::sync::mpsc::{Sender, channel};

use crate::actor::{Actor, ActorId, Handler, Message};
use crate::context::Context;
use crate::envelope::Envelope;

/// Error returned when sending a message fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError {
    /// The target actor no longer exists in the system.
    ActorNotRunning,
    /// The runtime has been shut down.
    RuntimeShutdown,
}

/// A type-safe address of an actor.
///
/// `Addr<A>` can be cloned and sent between threads. It is the primary
/// handle for interacting with an actor from outside the actor system.
pub struct Addr<A: Actor> {
    pub(crate) actor_id: ActorId,
    pub(crate) tx: Sender<Envelope>,
    _marker: PhantomData<A>,
}

impl<A: Actor> Clone for Addr<A> {
    fn clone(&self) -> Self {
        Addr {
            actor_id: self.actor_id,
            tx: self.tx.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A: Actor> Addr<A> {
    pub(crate) fn new(actor_id: ActorId, tx: Sender<Envelope>) -> Self {
        Addr {
            actor_id,
            tx,
            _marker: PhantomData,
        }
    }

    /// Returns the actor's unique identifier.
    pub fn id(&self) -> ActorId {
        self.actor_id
    }

    /// Send a message to the actor and block until the result is returned.
    ///
    /// Warning: Do not call `send` from within an actor's `handle` method on
    /// the same runtime — it will deadlock. Use `do_send` or `Context::notify`
    /// for inter-actor communication.
    pub fn send<M>(&self, msg: M) -> Result<M::Result, SendError>
    where
        A: Handler<M>,
        M: Message,
    {
        let (result_tx, result_rx) = channel();
        let envelope = Envelope {
            actor_id: self.actor_id,
            dispatch: Box::new(move |actor_any: &mut dyn std::any::Any, ctx: &mut Context| {
                let actor = actor_any
                    .downcast_mut::<A>()
                    .expect("actor type mismatch: wrong actor type stored for this ActorId");
                let result = <A as Handler<M>>::handle(actor, msg, ctx);
                let _ = result_tx.send(result);
            }),
        };
        self.tx.send(envelope).map_err(|_| SendError::RuntimeShutdown)?;
        result_rx.recv().map_err(|_| SendError::ActorNotRunning)
    }

    /// Fire-and-forget message send. Does not wait for a result.
    ///
    /// Safe to use from within an actor's `handle` method.
    pub fn do_send<M>(&self, msg: M) -> Result<(), SendError>
    where
        A: Handler<M>,
        M: Message,
    {
        let envelope = Envelope {
            actor_id: self.actor_id,
            dispatch: Box::new(move |actor_any: &mut dyn std::any::Any, ctx: &mut Context| {
                let actor = actor_any
                    .downcast_mut::<A>()
                    .expect("actor type mismatch: wrong actor type stored for this ActorId");
                <A as Handler<M>>::handle(actor, msg, ctx);
            }),
        };
        self.tx.send(envelope).map_err(|_| SendError::RuntimeShutdown)
    }
}
