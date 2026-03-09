# xiaoyong-channels

A collection of channel primitives for asynchronous programming, offering both thread-safe (`sync`) and single-threaded (`unsync`) implementations.

## Overview

This crate provides asynchronous channels optimized for different use cases:

*   **`sync`**: Thread-safe channels designed for cross-task communication in multi-threaded executors. These use atomic operations and `Arc` to safely share state across thread boundaries.
*   **`unsync`**: Single-threaded channels designed for environments where tasks are pinned to a single thread (e.g., `tokio::task::LocalSet`). These avoid atomic overhead by utilizing `Rc` and `Cell`, making them extremely fast but explicitly `!Send`.

## Available Channels

### Multi-Producer, Multi-Consumer (MPMC) Fixed

* `sync::async_mpmc_bounded`: A bounded MPMC broadcast channel using a sequence-locked ring buffer.

### Multi-Producer, Single-Consumer (MPSC) Fixed

* `unsync::async_mpsc_bounded`: A single-threaded bounded MPSC channel.

### Single-Producer, Single-Consumer (SPSC) Fixed

* `sync::async_spsc_bounded`: A fast bounded SPSC channel utilizing cache-padded atomics.

### Oneshot

* `sync::async_oneshot`: A thread-safe, single-use channel for transferring a single value.
* `unsync::async_oneshot`: A single-threaded oneshot channel.
