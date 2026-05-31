# Development Roadmap

## Phase 1: Core Traits & Single-Threaded Runtime ✅

**Status**: Complete

### Modules
| File | Purpose |
|------|---------|
| `src/actor.rs` | `ActorId`, `Actor` trait, `Handler<M>` trait, `Message` trait |
| `src/addr.rs` | `Addr<A>` type-safe address with `send()` and `do_send()` |
| `src/context.rs` | `Context` - lifecycle management (id, stop, notify) |
| `src/envelope.rs` | `Envelope` - type-erased message dispatch |
| `src/system.rs` | `ActorSystem` - single-threaded event loop |
| `src/lib.rs` | Crate root |
| `src/main.rs` | Demo with Counter + Printer actors |

### Tests: 11 passing
- Actor ID uniqueness, display, equality
- Handler result passing
- Spawn and send messages
- Actor stop via Context
- Inter-actor communication via notify
- Lifecycle hooks (started, stopped)
- Many messages to single actor
- Send to nonexistent actor

### Examples
- `examples/ping_pong.rs` - Ping/Pong inter-actor communication

---

## Phase 2: Multi-Threaded Runtime ✅

**Status**: Complete

### New Modules
| File | Purpose |
|------|---------|
| `src/runtime.rs` | `Runtime` - multi-threaded actor runtime with configurable workers |

### Design
- Each worker runs its own event loop on a dedicated OS thread
- Dual-channel per worker (control for spawn/shutdown, message for envelopes)
- Round-robin actor distribution across workers
- Synchronous spawn confirmation prevents race conditions
- Control channel disconnect signals "no more spawns", message channel disconnect exits worker

### Tests: 9 passing (20 total across all modules)
- Round-robin distribution across workers
- Concurrent execution (4 actors × 100ms sleep ≈ 100ms wall time)
- Many actors with few threads (100 actors / 2 workers)
- Shutdown stops workers
- Custom thread count (1 worker)
- Zero threads panics
- Lifecycle hooks in multi-threaded context
- Concurrent sends from multiple threads

### Examples
- `examples/multi_thread.rs` - 8 actors on 4 threads, ~200ms vs ~800ms serial

---

## Phase 3: Scheduler ✅

**Status**: Complete

### New Modules
| File | Purpose |
|------|---------|
| `src/scheduler.rs` | `Scheduler` — per-actor mailboxes with round-robin dispatch |

### Design
- Each actor has a `VecDeque<Envelope>` mailbox managed by the scheduler
- `ready_queue` tracks actors with pending messages in FIFO order
- `next_ready()` pops the next actor and dispatches one message
- After dispatch, actor is re-queued if more messages remain
- Guarantees no actor starves: each actor gets one message per turn

### Tests: 6 new (26 total across all modules)
- Enqueue and dequeue messages
- Round-robin ordering between multiple actors
- Round-robin fairness (3 actors, 2 messages each → 1,2,3,1,2,3)
- Actor removal cleans up mailbox and ready queue
- Duplicate enqueue only adds actor once to ready queue
- Integration test: fairness in live runtime with single worker

---

## Phase 4: Supervisor Actor ✅

**Status**: Complete

### New Modules
| File | Purpose |
|------|---------|
| `src/supervisor.rs` | `Supervisor` with restart strategies, `Context::spawn` support |

### Design
- `Supervisor` manages child actors via factory functions
- `Strategy::OneForOne` restarts only the failed child (up to `max_restarts`)
- `Strategy::AllForOne` restarts all children when one fails
- `Context::spawn()` allows actors to spawn children directly into the worker's actor map
- Panic isolation via `catch_unwind` in Worker — panics don't crash workers
- Children are spawned on the same worker as the parent (no deadlock)

### API
```rust
let mut sup = Supervisor::new(Strategy::OneForOne, 3);
sup.register_child(|ctx| { ctx.spawn(child_actor).id() });
sup.start_all(ctx);  // spawn initial children
sup.handle_failure(failed_id, ctx);  // restart on failure
```

### Tests: 4 new (32 total across all modules)
- Supervisor registers and starts children
- OneForOne restarts failed child with new ActorId
- OneForOne stops child after max_restarts exceeded
- Child factory spawns via Context::spawn

### Architecture Changes
- `SpawnShared` refactored with shared `ActorMap` for direct spawns
- `Context::spawn()` uses `spawn_actor_direct()` — no channels, no deadlock
- Worker removes actor from map during dispatch, re-inserts if still running

---

## Phase 5: Integration Test Framework ✅

**Status**: Complete

### New Modules
| File | Purpose |
|------|---------|
| `src/testkit.rs` | `TestKit`, `TestProbe<M>`, `ProbeActor<M>` |

### Design
- `TestKit` wraps `ActorSystem` for deterministic single-threaded testing
- `TestProbe<M>` records messages of type `M` for test assertions
- `ProbeActor<M>` is the underlying actor that collects messages
- `run_until_idle()` drains all pending messages synchronously
- Messages are stored in `Arc<Mutex<Vec<M>>>` for cross-thread access

### API
```rust
let mut kit = TestKit::new();
let probe = kit.spawn_probe::<MyMessage>();
let actor = kit.spawn(MyActor { output: probe.recipient() });

// Send messages from another thread
thread::spawn(move || { actor.do_send(msg); drop(actor); });

kit.run_until_idle();

// Assert
assert_eq!(probe.count(), 1);
assert!(probe.any_match(|m| m.field == expected));
```

### Tests: 5 new (41 total across all targets)
- Probe receives messages
- Count and first message retrieval
- Probe reset clears messages
- any_match predicate filtering
- Spawn custom actor in TestKit
- **4 integration tests**: worker→probe, multiple workers, type filtering, actor chain

## Summary

All 5 phases complete. 41 tests passing, zero warnings.

| Phase | Feature | Tests |
|-------|---------|-------|
| 1 | Core traits + Single-threaded runtime | 11 |
| 2 | Multi-threaded Runtime | 9 |
| 3 | Round-robin Scheduler | 6 |
| 4 | Supervisor + Context::spawn | 6 |
| 5 | Integration Test Framework | 9 |
| **Total** | | **41** |

---

## Key Design Decisions

1. **Type-erased Envelopes**: `Box<dyn FnOnce(&mut dyn Any, &mut Context) + Send>` — no proc macros needed
2. **Synchronous `send()`**: Blocks until the actor processes the message and returns a result
3. **Fire-and-forget `do_send()`**: Non-blocking, safe for inter-actor communication
4. **Self-cleaning channels**: Workers exit when all `Addr` senders are dropped
5. **Zero external dependencies**: Pure `std` only (mpsc channels, threads, Any)
