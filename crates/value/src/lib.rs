//! Primitives for sharing state.
//!
//! This crate offers both thread-safe (`sync`) and single-threaded (`unsync`)
//! implementations. The `unsync` types are optimized for environments where
//! tasks remain on a single thread.
//!
//! The `sync::arcswap` and `sync::async_arcswap` modules require the optional
//! `arc-swap` feature because they use the external `triomphe::Arc` type rather
//! than `std::sync::Arc`.

pub mod sync;
pub mod unsync;
