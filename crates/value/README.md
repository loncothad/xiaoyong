# xiaoyong-value

Shared state primitives for sharing state.

## Overview

This crate provides shared state primitives like `Mutex` and `RwLock`.

The `unsync` module provides primitives tailored for single-threaded executors (like `tokio::task::LocalSet`).

### Thread Safety in `unsync`

Unlike some other `unsync` types, `xiaoyong_value::unsync::async_mutex::Mutex` and `xiaoyong_value::unsync::async_rwlock::RwLock` are built on top of `Cell` and `UnsafeCell`. Because they do not use `Rc`, **these types automatically implement `Send` if the underlying data `T` is `Send`.**

However, their lock guards (e.g., `MutexGuard`) are explicitly `!Send`. This guarantees that while you can transfer the `Mutex` itself across threads, you cannot lock it on one thread, move the guard to another thread, and unlock it there. They must be used within the confines of a single thread at any given time.

`unsync::async_rcswap::RcSwap`, on the other hand, utilizes `Rc` and is strictly `!Send`.

## Available Primitives

* `unsync::async_mutex::Mutex`: An asynchronous, single-threaded Mutex.
* `unsync::async_rwlock::RwLock`: An asynchronous, single-threaded Reader-Writer Lock.
* `unsync::async_rcswap::RcSwap`: An asynchronous primitive for atomic swapping of `Rc` pointers.
* `unsync::rcswap::RcSwap`: A synchronous primitive for atomic swapping of `Rc` pointers.
* `sync::atomic_once::AtomicOnce`: A lightweight, lock-free alternative to `std::sync::OnceLock`.
* `sync::permanent::Permanent`: Handle to a statically allocated data.