//! Lightweight, lock-free alternative to `std::sync::OnceLock`.

use core::ptr;
use std::sync::atomic::{
    AtomicPtr,
    Ordering,
};

/// Lock-free, single-assignment cell.
///
/// Allows multiple threads to race to initialize a value. The
/// winning thread stores its value, while losing threads receive their owned
/// candidate back together with a reference to the winning value.
pub struct AtomicOnce<T> {
    ptr: AtomicPtr<T>,
}

// SAFETY: `AtomicOnce` is safe to share across threads if the underlying data
// `T` is safe to share across threads.
unsafe impl<T: Sync + Send> Sync for AtomicOnce<T> {}
// SAFETY: `AtomicOnce` is safe to send across threads if `T` is safe to send.
unsafe impl<T: Send> Send for AtomicOnce<T> {}

impl<T> Drop for AtomicOnce<T> {
    fn drop(&mut self) {
        let p = *self.ptr.get_mut();
        if !p.is_null() {
            // SAFETY: Reassert ownership of the heap allocation to drop it.
            // This is only called once when the AtomicOnce itself goes out of scope.
            unsafe { drop(Box::from_raw(p)) };
        }
    }
}

impl<T> AtomicOnce<T> {
    /// Create a new instance.
    pub const fn new() -> Self {
        Self {
            ptr: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Creates a new instance already initialized with the given value.
    pub fn new_initialized(val: Box<T>) -> Self {
        let ptr = Box::into_raw(val);
        Self {
            ptr: AtomicPtr::new(ptr),
        }
    }

    /// Returns a reference to the initialized value, or `None` if
    /// uninitialized.
    pub fn get(&self) -> Option<&T> {
        let p = self.ptr.load(Ordering::Acquire);
        if p.is_null() {
            None
        } else {
            // SAFETY: The pointer is either null or a valid pointer resulting
            // from Box::into_raw. It is never mutated after initialization,
            // and lives until the AtomicOnce is dropped.
            Some(unsafe { &*p })
        }
    }

    /// Returns exclusive access to the initialized value, if present.
    pub fn get_mut(&mut self) -> Option<&mut T> {
        let pointer = *self.ptr.get_mut();
        // SAFETY: an exclusive borrow prevents readers or initializers, and a
        // non-null pointer represents the Box owned by this cell.
        unsafe { pointer.as_mut() }
    }

    /// Removes the initialized value, leaving the cell empty for reuse.
    pub fn take(&mut self) -> Option<T> {
        let pointer = core::mem::replace(self.ptr.get_mut(), ptr::null_mut());
        if pointer.is_null() {
            None
        } else {
            // SAFETY: exclusive access transfers the cell's sole Box ownership.
            Some(*unsafe { Box::from_raw(pointer) })
        }
    }

    /// Returns the initialized value without checking for an empty cell.
    ///
    /// # Safety
    ///
    /// The caller must ensure this cell has been successfully initialized.
    /// Initialization must not race with destruction of the cell.
    pub unsafe fn get_unchecked(&self) -> &T {
        let pointer = self.ptr.load(Ordering::Acquire);
        debug_assert!(!pointer.is_null());
        // SAFETY: upheld by the caller. The Acquire load synchronizes with the
        // successful initializer's Release operation.
        unsafe { &*pointer }
    }

    /// Attempts to initialize the cell with the provided value.
    ///
    /// If the cell was already initialized or we lost the CAS race, returns
    /// the reference to the initialized value and the owned value `val`.
    pub fn init(&self, val: Box<T>) -> Result<(), (&T, Box<T>)> {
        if let Some(existing) = self.get() {
            return Err((existing, val));
        }

        let val_ptr = Box::into_raw(val);

        match self
            .ptr
            .compare_exchange(ptr::null_mut(), val_ptr, Ordering::Release, Ordering::Acquire)
        {
            | Ok(_) => Ok(()),
            | Err(existing_ptr) => {
                // SAFETY: We just created this raw pointer from a Box. Since we lost
                // the CAS race, we still have exclusive ownership over this specific
                // allocation.
                let this_candidate = unsafe { Box::from_raw(val_ptr) };

                // SAFETY: `existing_ptr` was successfully written by the winning thread.
                let existing = unsafe { &*existing_ptr };

                Err((existing, this_candidate))
            },
        }
    }

    /// Returns a reference to the value, initializing it with `f` if necessary.
    /// If the cell was already initialized or we lost the CAS race, returns
    /// the reference to the initialized value and the owned value that was
    /// computed by the `f`.
    ///
    /// Note that `f` may be executed multiple times concurrently if multiple
    /// threads attempt initialization simultaneously. Only one result will
    /// be retained.
    pub fn get_or_init<F>(&self, f: F) -> Result<&T, (&T, Box<T>)>
    where
        F: FnOnce() -> Box<T>,
    {
        if let Some(val) = self.get() {
            return Ok(val);
        }

        let val_ptr = Box::into_raw(f());

        match self
            .ptr
            .compare_exchange(ptr::null_mut(), val_ptr, Ordering::Release, Ordering::Acquire)
        {
            | Ok(_) => {
                // SAFETY: We won the race and successfully stored the pointer.
                Ok(unsafe { &*val_ptr })
            },
            | Err(existing_ptr) => {
                // SAFETY: We just created this raw pointer from a Box. Since we lost
                // the CAS race, we still have exclusive ownership over this specific
                // allocation.
                let this_candidate = unsafe { Box::from_raw(val_ptr) };

                // SAFETY: `existing_ptr` was successfully written by the winning thread.
                let existing = unsafe { &*existing_ptr };

                Err((existing, this_candidate))
            },
        }
    }

    /// Consumes the `AtomicOnce`, returning the inner value if initialized.
    pub fn into_inner(mut self) -> Option<T> {
        self.take()
    }
}

impl<T> Default for AtomicOnce<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> From<T> for AtomicOnce<T> {
    fn from(value: T) -> Self {
        Self::new_initialized(Box::new(value))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            Barrier,
        },
        thread,
    };

    use super::*;

    #[test]
    fn initializes_only_once_and_returns_losing_value() {
        let once = AtomicOnce::new();
        assert!(once.init(Box::new(10)).is_ok());
        let result = once.init(Box::new(20));
        assert!(matches!(result, Err((existing, value)) if *existing == 10 && *value == 20));
        assert_eq!(once.into_inner(), Some(10));
    }

    #[test]
    fn concurrent_initialization_publishes_one_complete_value() {
        let once = Arc::new(AtomicOnce::new());
        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0 .. 8)
            .map(|value| {
                let once = Arc::clone(&once);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let _result = once.init(Box::new((value, value * 2)));
                })
            })
            .collect();
        for thread in threads {
            assert!(thread.join().is_ok());
        }

        let (value, doubled) = once.get().copied().expect("one thread must initialize the cell");
        assert_eq!(doubled, value * 2);
    }

    #[test]
    fn exclusive_access_can_take_and_reinitialize() {
        let mut once = AtomicOnce::from(String::from("old"));
        once.get_mut().unwrap().push_str(" value");
        assert_eq!(once.take().as_deref(), Some("old value"));
        assert!(once.get_mut().is_none());
        assert!(once.take().is_none());
        assert!(once.init(Box::new(String::from("new"))).is_ok());
        assert_eq!(once.into_inner().as_deref(), Some("new"));
    }
}
