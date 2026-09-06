# xiaoyong-value

Shared-state primitives for synchronous and asynchronous code.

## Modules

This crate provides `sync` (thread-safe) and `unsync` (single-threaded)
primitives. The `unsync` module is intended for executors such as
`tokio::task::LocalSet`.

### Thread safety

Unlike some other `unsync` types, `xiaoyong_value::unsync::async_mutex::Mutex`
and `xiaoyong_value::unsync::async_rwlock::RwLock` are built on top of `Cell`
and `UnsafeCell`. Because they do not use `Rc`, these types implement `Send`
when the underlying data `T` implements `Send`.

However, their lock guards, such as `MutexGuard`, are explicitly `!Send`.
This guarantees that while you can transfer the `Mutex` itself across threads,
you cannot lock it on one thread, move the guard to another thread, and unlock
it there. They must be used within the confines of a single thread at any given
time.

`unsync::async_rcswap::RcSwap`, on the other hand, utilizes `Rc` and is strictly
`!Send`.

## Primitives

- `unsync::async_mutex::Mutex`: An asynchronous, single-threaded mutex.
- `unsync::async_rwlock::RwLock`: An asynchronous, single-threaded reader-writer
  lock.
- `unsync::async_rcswap::RcSwap`: An asynchronous primitive for swapping `Rc`
  pointers.
- `unsync::rcswap::RcSwap`: A synchronous primitive for swapping `Rc`
  pointers.
- `sync::arcswap::ArcSwap`: A lock-free snapshot container for swapping
  `triomphe::Arc` pointers.
- `sync::async_arcswap::ArcSwap`: A snapshot container whose changes can be
  awaited. Reads delegate to the lock-free synchronous implementation.
- `sync::atomic_once::AtomicOnce`: A lightweight, lock-free alternative to
  `std::sync::OnceLock`.
- `sync::permanent::Permanent`: A handle to statically allocated data.

## Features

### `arc-swap`

Enables both `sync::arcswap` and `sync::async_arcswap`. This feature is disabled
by default because these modules use the external `triomphe::Arc` type rather
than `std::sync::Arc`; the asynchronous variant additionally uses
`event-listener`.

The single-threaded mutex and reader-writer lock implement `Default` and
`From<T>`. Both offer `get_mut` for exclusive access without locking and
`into_inner` to recover the value. Queued lock requests retain FIFO priority,
including when a waiting writer is canceled while readers remain active.

`AtomicOnce` implements `From<T>`, provides exclusive `get_mut`, and supports
`take` to leave an empty cell that can be initialized again. Asynchronous swap
waiters return the current snapshot after observing a change; intermediate
snapshots may be coalesced.
