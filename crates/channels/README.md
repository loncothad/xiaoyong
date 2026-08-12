# xiaoyong-channels

A collection of channel primitives for asynchronous programming, offering both
thread-safe (`sync`) and single-threaded (`unsync`) implementations.

## Modules

This crate provides asynchronous channels optimized for different use cases:

- `sync`: Thread-safe channels for cross-task communication in multithreaded
  executors. These use atomic operations and `Arc` to share state safely.
- `unsync`: Single-threaded channels for tasks that remain on one thread, such
  as tasks running in `tokio::task::LocalSet`. These use `Rc` and `Cell` to
  avoid atomic overhead and are `!Send`.

## Channels

### Multi-Producer, Multi-Consumer (MPMC) Fixed

- `sync::async_mpmc_bounded`: A bounded MPMC broadcast channel using a
  sequence-locked ring buffer.

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
