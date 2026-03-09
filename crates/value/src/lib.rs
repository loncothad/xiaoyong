//! Asynchronous synchronization primitives for sharing state.
//!
//! This crate offers both thread-safe (`sync`) and single-threaded (`unsync`)
//! implementations. The `unsync` types are optimized for environments where
//! tasks remain on a single thread.

#[doc(hidden)]
pub mod sync;
pub mod unsync;
