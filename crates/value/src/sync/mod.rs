//! Thread-safe primitives.
//!
//! The `arcswap` and `async_arcswap` modules are available with the `arc-swap`
//! feature. They are feature-gated because they use the external
//! `triomphe::Arc` type rather than `std::sync::Arc`.

#[cfg(feature = "arc-swap")]
pub mod arcswap;
#[cfg(feature = "arc-swap")]
pub mod async_arcswap;
pub mod atomic_once;
pub mod permanent;
