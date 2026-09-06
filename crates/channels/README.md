# xiaoyong-channels

A collection of channel primitives for asynchronous programming, offering both
thread-safe (`sync`) and single-threaded (`unsync`) implementations.

## Modules

This crate provides asynchronous channels optimized for different use cases:

- `sync`: Thread-safe channels for cross-task communication in multithreaded
  executors. These use `Arc` with atomics or a short mutex to share state safely.
- `unsync`: Single-threaded channels for tasks that remain on one thread, such
  as tasks running in `tokio::task::LocalSet`. These use `Rc` and `Cell` to
  avoid atomic overhead and are `!Send`.

## Channels

### Multi-Producer, Multi-Consumer (MPMC) Fixed

- `sync::async_mpmc_bounded`: A bounded MPMC broadcast channel with a mutex
  protecting buffer ownership and subscription changes. Independent event
  listeners support simultaneous pending sends on the same handle. Each
  receiver sees every value sent after it subscribes; the slowest receiver
  applies backpressure. Receivers implement `futures::Stream` and provide
  `try_pop`, `try_pop_many`, and `is_closed`.

### Multi-Producer, Single-Consumer (MPSC) Fixed

- `unsync::async_mpsc_bounded`: A single-threaded bounded MPSC channel.

### Single-Producer, Single-Consumer (SPSC) Fixed

- `sync::async_spsc_bounded`: A bounded SPSC channel using cache-padded
  atomics.

  Dropping either endpoint wakes the peer: sends return their unsent value after
  the consumer closes, while receives return `None` after the producer closes
  and buffered values have drained.

### Oneshot

- `sync::async_oneshot`: A thread-safe, single-use channel for transferring a
  single value.
- `unsync::async_oneshot`: A single-threaded oneshot channel.

## Migrating to 3.0

SPSC send and receive operations now require `&mut self`. Declare endpoints
with `let (mut producer, mut consumer) = channel(capacity)` and keep at most
one pending operation per endpoint. This enforces the exclusive access required
by the underlying buffer.

The broadcast channel now uses a mutex rather than the previous atomic slot
algorithm, fixing races during subscription reuse and value reclamation. Its
performance characteristics have changed; benchmark latency-sensitive workloads.
Consumed values are reclaimed once every subscribed receiver has advanced.
Zero-length batch operations return immediately, including on closed channels.
