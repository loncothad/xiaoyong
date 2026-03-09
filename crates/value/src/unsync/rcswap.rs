//! Single-threaded, synchronous primitive for atomic swapping of Rc pointers.

use std::{
    cell::UnsafeCell,
    mem,
    ptr,
    rc::Rc,
};

/// Synchronous primitive for atomic swapping of Rc pointers.
///
/// **Thread Safety:** This type utilizes Rc and is strictly !Send.
pub struct RcSwap<T> {
    ptr: UnsafeCell<Rc<T>>,
}

impl<T> RcSwap<T> {
    /// Create a new instance.
    pub fn new(value: Rc<T>) -> Self {
        Self {
            ptr: UnsafeCell::new(value),
        }
    }

    /// Load the current value.
    pub fn load(&self) -> Rc<T> {
        // SAFETY:
        // 1. The type is !Sync, meaning no other thread can execute `store` or `swap`
        //    concurrently.
        // 2. We only take a shared reference to the inner `Rc<T>` for the duration of
        //    the clone.
        unsafe { (*self.ptr.get()).clone() }
    }

    /// Replace the current value with a new one, dropping the old value.
    pub fn store(&self, value: Rc<T>) {
        // SAFETY:
        // `ptr::replace` performs a bitwise swap of the pointer values.
        // We do this instead of `*self.ptr.get() = value;` so that the old `Rc<T>`
        // is dropped *outside* the UnsafeCell assignment. This prevents undefined
        // behavior if `T`'s `Drop` implementation attempts to access this `RcSwap`.
        unsafe {
            mem::drop(ptr::replace(self.ptr.get(), value));
        }
    }

    /// Replace the current value and returns the old one.
    pub fn swap(&self, value: Rc<T>) -> Rc<T> {
        // SAFETY:
        // Same invariants as `store`. Safe from concurrent mutation due to !Sync.
        unsafe { ptr::replace(self.ptr.get(), value) }
    }
}

impl<T> Default for RcSwap<T>
where
    T: Default,
{
    fn default() -> Self {
        Self::new(Rc::new(T::default()))
    }
}

impl<T> From<Rc<T>> for RcSwap<T> {
    fn from(value: Rc<T>) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;

    #[test]
    fn rcswap() {
        let swap = RcSwap::new(Rc::new(10));
        assert_eq!(*swap.load(), 10);
        swap.store(Rc::new(20));
        assert_eq!(*swap.load(), 20);
        let old = swap.swap(Rc::new(30));
        assert_eq!(*old, 20);
        assert_eq!(*swap.load(), 30);
    }
}
