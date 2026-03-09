//! Lightweight, lock-free alternative to `std::sync::OnceLock`.

use std::{
    ptr,
    sync::atomic::{
        AtomicPtr,
        Ordering,
    },
};

/// Lock-free, single-assignment cell.
///
/// Allows multiple threads to race to initialize a value. The
/// winning thread stores its value, while losing threads silently discard their
/// work.
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

    /// Attempts to initialize the cell with the provided value.
    ///
    /// Returns `Ok(())` on success. If the cell was already initialized or or
    /// we lost the atomic CAS race, it returns `Err(val)`.
    pub unsafe fn init(&self, val: Box<T>) -> Result<(), Box<T>> {
        if !self.ptr.load(Ordering::Acquire).is_null() {
            return Err(val);
        }

        let val_ptr = Box::into_raw(val);

        match self
            .ptr
            .compare_exchange(ptr::null_mut(), val_ptr, Ordering::Release, Ordering::Acquire)
        {
            | Ok(_) => Ok(()),
            | Err(_) => {
                // SAFETY: We just created this raw pointer from a Box. Since we lost
                // the CAS race, we still have exclusive ownership over this specific
                // allocation.
                let unneeded_box = unsafe { Box::from_raw(val_ptr) };
                Err(unneeded_box)
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
        // Bypass atomics since we have an exclusive mutable reference.
        let p = *self.ptr.get_mut();
        if p.is_null() {
            None
        } else {
            // Set the pointer to null so `Drop` doesn't double-free.
            *self.ptr.get_mut() = ptr::null_mut();

            // SAFETY: The pointer was valid, and we now take ownership.
            Some(*unsafe { Box::from_raw(p) })
        }
    }
}
