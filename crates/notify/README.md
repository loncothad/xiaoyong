# xiaoyong-notify

Asynchronous notification primitives for signaling between tasks.

## Modules

This crate provides `sync` (thread-safe) and `unsync` (single-threaded)
variants. The `unsync` variants use `Rc` and `Cell` to avoid atomic operations
on single-threaded executors such as `tokio::task::LocalSet`.

### Thread safety

Types in the `unsync` module are explicitly designed for single-threaded usage.
They use `Rc` for internal reference counting. Attempting to implement `Send` on
these types and passing them across threads would lead to undefined behavior
(data races on the reference count). Do not share these types across OS threads.

## Primitives

### Notify

- `sync::oneshot::Notify`: A one-time thread-safe notification.
- `sync::reusable::Notify`: A reusable thread-safe notification.
- `unsync::oneshot::Notify`: A one-time single-threaded notification.
- `unsync::reusable::Notify`: A reusable single-threaded notification.
- `unsync::queued::Notify`: A single-threaded notification primitive that
  supports FIFO waiters and queued notification permits.

### Semaphore

- `unsync::semaphore::Semaphore`: A fair, cancellation-safe, single-threaded
  asynchronous semaphore for rate-limiting.
