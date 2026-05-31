use std::collections::{HashMap, VecDeque};

use crate::actor::ActorId;
use crate::envelope::Envelope;

/// Per-worker round-robin scheduler for fair message dispatch.
///
/// Each actor has its own mailbox. The scheduler maintains a ready queue
/// and processes one message per actor per turn (round-robin), preventing
/// any single actor from starving others.
pub(crate) struct Scheduler {
    mailboxes: HashMap<ActorId, VecDeque<Envelope>>,
    ready_queue: VecDeque<ActorId>,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            mailboxes: HashMap::new(),
            ready_queue: VecDeque::new(),
        }
    }

    /// Enqueue a message for an actor. If the actor isn't already in the
    /// ready queue, adds it.
    pub fn enqueue(&mut self, envelope: Envelope) {
        let actor_id = envelope.actor_id;
        self.mailboxes.entry(actor_id).or_default().push_back(envelope);
        if !self.ready_queue.contains(&actor_id) {
            self.ready_queue.push_back(actor_id);
        }
    }

    /// Return the next ready actor ID in round-robin order. Skips actors
    /// whose mailboxes are empty or have been removed.
    pub fn next_ready(&mut self) -> Option<ActorId> {
        // Try at most the queue length to avoid infinite loops
        let len = self.ready_queue.len();
        for _ in 0..len {
            let id = self.ready_queue.pop_front()?;
            if self.has_pending(id) {
                return Some(id);
            }
            // Actor has no messages — skip and don't re-queue
        }
        None
    }

    /// Dequeue one message for the given actor.
    pub fn dequeue(&mut self, actor_id: ActorId) -> Option<Envelope> {
        self.mailboxes.get_mut(&actor_id)?.pop_front()
    }

    /// Re-queue an actor if it still has pending messages. Call after
    /// processing one of its messages.
    pub fn requeue_if_ready(&mut self, actor_id: ActorId) {
        if self.has_pending(actor_id) {
            self.ready_queue.push_back(actor_id);
        } else {
            self.cleanup_empty(actor_id);
        }
    }

    /// Remove an actor's mailbox entirely (called when actor stops).
    pub fn remove_actor(&mut self, actor_id: ActorId) {
        self.mailboxes.remove(&actor_id);
        self.ready_queue.retain(|id| *id != actor_id);
    }

    fn has_pending(&self, actor_id: ActorId) -> bool {
        self.mailboxes
            .get(&actor_id)
            .map(|q| !q.is_empty())
            .unwrap_or(false)
    }

    fn cleanup_empty(&mut self, actor_id: ActorId) {
        if self
            .mailboxes
            .get(&actor_id)
            .map(|q| q.is_empty())
            .unwrap_or(false)
        {
            self.mailboxes.remove(&actor_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Context;
    use std::any::Any;

    fn make_envelope(id: u64) -> Envelope {
        Envelope {
            actor_id: ActorId(id),
            dispatch: Box::new(|_: &mut dyn Any, _: &mut Context| {}),
        }
    }

    #[test]
    fn enqueue_and_dequeue() {
        let mut s = Scheduler::new();
        s.enqueue(make_envelope(1));
        s.enqueue(make_envelope(1));

        assert_eq!(s.next_ready(), Some(ActorId(1)));
        assert!(s.dequeue(ActorId(1)).is_some());
        s.requeue_if_ready(ActorId(1)); // still has 1 message

        assert_eq!(s.next_ready(), Some(ActorId(1)));
        assert!(s.dequeue(ActorId(1)).is_some());
        s.requeue_if_ready(ActorId(1)); // empty now

        assert_eq!(s.next_ready(), None);
    }

    #[test]
    fn round_robin_between_actors() {
        let mut s = Scheduler::new();

        // Actor 1: 3 messages, Actor 2: 1 message
        s.enqueue(make_envelope(1));
        s.enqueue(make_envelope(1));
        s.enqueue(make_envelope(1));
        s.enqueue(make_envelope(2));

        // Turn 1: Actor 1
        assert_eq!(s.next_ready(), Some(ActorId(1)));
        s.dequeue(ActorId(1));
        s.requeue_if_ready(ActorId(1));

        // Turn 2: Actor 2
        assert_eq!(s.next_ready(), Some(ActorId(2)));
        s.dequeue(ActorId(2));
        s.requeue_if_ready(ActorId(2)); // done

        // Turn 3: Actor 1 (re-queued)
        assert_eq!(s.next_ready(), Some(ActorId(1)));
        s.dequeue(ActorId(1));
        s.requeue_if_ready(ActorId(1));

        // Turn 4: Actor 1 (last message)
        assert_eq!(s.next_ready(), Some(ActorId(1)));
        s.dequeue(ActorId(1));
        s.requeue_if_ready(ActorId(1)); // done

        // No more
        assert_eq!(s.next_ready(), None);
    }

    #[test]
    fn round_robin_fairness_order() {
        let mut s = Scheduler::new();

        // 3 actors, 2 messages each
        for _ in 0..2 {
            s.enqueue(make_envelope(1));
            s.enqueue(make_envelope(2));
            s.enqueue(make_envelope(3));
        }

        let mut order = Vec::new();
        while let Some(id) = s.next_ready() {
            order.push(id.0);
            s.dequeue(id);
            s.requeue_if_ready(id);
        }

        // Expected: 1,2,3, 1,2,3 (round-robin)
        assert_eq!(order, vec![1, 2, 3, 1, 2, 3]);
    }

    #[test]
    fn remove_actor_cleans_up() {
        let mut s = Scheduler::new();

        s.enqueue(make_envelope(1));
        s.enqueue(make_envelope(2));

        s.remove_actor(ActorId(1));

        // Actor 1 should be gone
        assert_eq!(s.next_ready(), Some(ActorId(2)));
        s.dequeue(ActorId(2));
        s.requeue_if_ready(ActorId(2));

        assert_eq!(s.next_ready(), None);
    }

    #[test]
    fn duplicate_enqueue_only_adds_once() {
        let mut s = Scheduler::new();

        s.enqueue(make_envelope(1));
        s.enqueue(make_envelope(1)); // second message, same actor

        let id = s.next_ready().unwrap();
        assert_eq!(id, ActorId(1));
        s.dequeue(id);
        s.requeue_if_ready(id);

        // Still has one message
        assert_eq!(s.next_ready(), Some(ActorId(1)));
    }
}
