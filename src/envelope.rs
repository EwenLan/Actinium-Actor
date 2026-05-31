use std::any::Any;

use crate::actor::ActorId;
use crate::context::Context;

/// Type-erased dispatch function.
pub(crate) type DispatchFn = Box<dyn FnOnce(&mut dyn Any, &mut Context) + Send>;

/// Type-erased envelope that carries a message dispatch to the runtime.
pub(crate) struct Envelope {
    pub actor_id: ActorId,
    pub dispatch: DispatchFn,
}
