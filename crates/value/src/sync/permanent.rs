//! Handle to a statically allocated data.

use std::{
    fmt,
    mem,
    ops::Deref,
};

/// Handle that mimics a `'static` reference.
///
/// **Thread Safety:** Can be used anywhere and does not produce any form of
/// overhead.
pub struct Permanent<T: 'static> {
    inner: &'static T,
}

// Safety: Permanent<T> is Send/Sync if the underlying T is Sync.
// Since we only provide &T access, T does not strictly need to be Send
// for the handle to be Sync, but usually, we want T: Send + Sync.
unsafe impl<T: Sync + 'static> Send for Permanent<T> {}
unsafe impl<T: Sync + 'static> Sync for Permanent<T> {}

impl<T: 'static> Permanent<T> {
    /// Create a new instance.
    pub fn new(value: Box<T>) -> Self {
        let leaked = Box::leak(value);
        Self {
            inner: leaked
        }
    }

    /// Drop the stored value.
    ///
    /// ## Safety
    ///
    /// The caller MUST ensure that:
    /// - No other copies of this `Permanent<T>` (or any references derived from
    ///   it) exist anywhere in the program.
    /// - This function is called exactly once for the allocated memory.
    pub unsafe fn drop_permanent(self) {
        let ptr = self.inner as *const T as *mut T;
        unsafe {
            mem::drop(Box::from_raw(ptr));
        }
    }
}

impl<T: 'static> Clone for Permanent<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: 'static> Copy for Permanent<T> {}

impl<T: 'static> Deref for Permanent<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<T: fmt::Debug + 'static> fmt::Debug for Permanent<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Permanent").field("inner", &self.inner).finish()
    }
}

impl<T: fmt::Display + 'static> fmt::Display for Permanent<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.inner, f)
    }
}
