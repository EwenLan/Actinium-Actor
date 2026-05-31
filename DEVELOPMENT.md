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

## Phase 3: Scheduler 🔜

**Goal**: Support cooperative scheduling within each worker. Each worker's event loop processes messages from its assigned actors in round-robin fashion. Actors yield after processing each message.

**Pending**:
- [ ] Per-worker message queue with round-robin dispatch
- [ ] Fair scheduling when more actors than threads
- [ ] Tests for scheduling fairness

---

## Phase 4: Supervisor Actor 🔜

**Goal**: A supervisor actor that spawns and monitors child actors with restart strategies.

**Pending**:
- [ ] `Supervisor` actor with OneForOne / AllForOne strategies
- [ ] Child failure detection and restart
- [ ] Context::spawn for spawning actors from within actors
- [ ] Tests for supervisor behavior

---

## Phase 5: Integration Test Framework 🔜

**Goal**: Test utilities for spawning actors, stubbing, and asserting on message flow.

**Pending**:
- [ ] `TestKit` for isolated actor testing
- [ ] `StubHandle` for message interception
- [ ] Integration tests for full actor flows

---

## Key Design Decisions

1. **Type-erased Envelopes**: `Box<dyn FnOnce(&mut dyn Any, &mut Context) + Send>` — no proc macros needed
2. **Synchronous `send()`**: Blocks until the actor processes the message and returns a result
3. **Fire-and-forget `do_send()`**: Non-blocking, safe for inter-actor communication
4. **Self-cleaning channels**: Workers exit when all `Addr` senders are dropped
5. **Zero external dependencies**: Pure `std` only (mpsc channels, threads, Any)
