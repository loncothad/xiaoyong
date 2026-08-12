#![no_std]
//! Fixed-capacity collection types.
//!
//! The collections in this crate store their elements inline and never
//! allocate. They are useful in embedded code and in hot paths where a known
//! upper bound is preferable to heap growth.

#[cfg(test)]
extern crate std;

pub mod unsync;
