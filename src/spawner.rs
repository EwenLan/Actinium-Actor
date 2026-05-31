use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use crate::actor::ActorId;
use crate::envelope::{DispatchFn, Envelope};

/// Control message sent from the Runtime to a Worker.
pub(crate) enum ControlMsg {
    SpawnActor {
        id: ActorId,
        actor: Box<dyn Any + Send>,
        on_stop: DispatchFn,
        confirm: Sender<()>,
    },
    Shutdown,
}

/// Internal per-worker actor storage shared with Context for direct spawns.
pub(crate) struct ActorCell {
    pub actor: Box<dyn Any + Send>,
    pub on_stop: DispatchFn,
}

pub(crate) type ActorMap = Arc<Mutex<HashMap<ActorId, ActorCell>>>;

/// Shared state for spawning actors from within Context.
pub(crate) struct SpawnShared {
    pub control_txs: Vec<Sender<ControlMsg>>,
    pub msg_txs: Vec<Sender<Envelope>>,
    pub actor_maps: Vec<ActorMap>,
    pub next_id: AtomicU64,
    pub next_worker: AtomicUsize,
    pub num_workers: usize,
    pub running: AtomicBool,
}

impl SpawnShared {
    pub fn allocate_id(&self) -> ActorId {
        ActorId(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    pub fn pick_worker(&self) -> usize {
        self.next_worker.fetch_add(1, Ordering::Relaxed) % self.num_workers
    }

    /// Spawn via control channel (cross-worker, from Runtime::spawn).
    pub fn spawn_actor_remote(
        &self,
        id: ActorId,
        worker_idx: usize,
        actor: Box<dyn Any + Send>,
        on_stop: DispatchFn,
    ) -> Sender<Envelope> {
        let (confirm_tx, confirm_rx) = std::sync::mpsc::channel();
        self.control_txs[worker_idx]
            .send(ControlMsg::SpawnActor {
                id,
                actor,
                on_stop,
                confirm: confirm_tx,
            })
            .expect("worker thread should be alive");
        confirm_rx.recv().expect("worker should confirm spawn");
        self.msg_txs[worker_idx].clone()
    }

    /// Spawn directly into a worker's actor map (same-worker, from Context::spawn).
    /// Avoids deadlock by bypassing the control channel.
    pub fn spawn_actor_direct(
        &self,
        id: ActorId,
        worker_idx: usize,
        actor: Box<dyn Any + Send>,
        on_stop: DispatchFn,
    ) -> Sender<Envelope> {
        self.actor_maps[worker_idx]
            .lock()
            .unwrap()
            .insert(id, ActorCell { actor, on_stop });
        self.msg_txs[worker_idx].clone()
    }

    /// Remove an actor from its worker's map (called by Worker).
    pub fn remove_actor(&self, actor_id: ActorId, worker_idx: usize) -> Option<ActorCell> {
        self.actor_maps[worker_idx].lock().unwrap().remove(&actor_id)
    }
}
