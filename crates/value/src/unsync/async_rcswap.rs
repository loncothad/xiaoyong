//! Single-threaded, asynchronous primitive for atomic swapping of Rc
//! pointers.

use std::{
    cell::UnsafeCell,
    future::Future,
    mem,
    pin::Pin,
    ptr,
    rc::Rc,
    task::{
        Context,
        Poll,
        Waker,
    },
};

use smallvec::SmallVec;

struct Inner<T> {
    value:          Rc<T>,
    version:        u64,
    next_waiter_id: usize,
    waiters:        SmallVec<[(usize, Waker); 8]>,
}

/// Asynchronous primitive for atomic swapping of Rc pointers.
///
/// **Thread Safety:** This type utilizes Rc and is strictly !Send.
pub struct RcSwap<T> {
    inner: UnsafeCell<Inner<T>>,
}

impl<T> RcSwap<T> {
    /// Create a new instance.
    pub fn new(value: Rc<T>) -> Self {
        Self {
            inner: UnsafeCell::new(Inner {
                value,
                version: 0,
                next_waiter_id: 0,
                waiters: SmallVec::new(),
            }),
        }
    }

    /// Load the current value.
    pub fn load(&self) -> Rc<T> {
        // SAFETY: The type is !Sync. No concurrent thread can execute mutations.
        unsafe { (*self.inner.get()).value.clone() }
    }

    /// Replace the current value with a new one, dropping the old value.
    pub fn store(&self, value: Rc<T>) {
        // SAFETY:
        // We drop the old value outside the UnsafeCell assignment and after waking.
        // This prevents UB if `T`'s `Drop` implementation attempts to access this swap.
        mem::drop(self.swap(value));
    }

    /// Replace the current value, wake pending futures, and returns the old
    /// value.
    pub fn swap(&self, value: Rc<T>) -> Rc<T> {
        let ptr = self.inner.get();

        // SAFETY:
        // 1. Bitwise swap the pointer values.
        // 2. Increment state version.
        // 3. Extract wakers to drop them from the internal state before iteration.
        let (old_value, wakers) = unsafe {
            let old = ptr::replace(&mut (*ptr).value, value);
            (*ptr).version = (*ptr).version.wrapping_add(1);
            let wakers = mem::take(&mut (*ptr).waiters)
                .into_iter()
                .map(|(_, waker)| waker)
                .collect::<SmallVec<[Waker; 8]>>();
            (old, wakers)
        };

        // Waking is done outside the unsafe block and after state mutation.
        // This is safe from aliasing even if a waker recursively interacts with
        // `AsyncRcSwap`.
        for waker in wakers {
            waker.wake();
        }

        old_value
    }

    /// Returns a Future that resolves with the new value once it changes.
    pub fn wait_until_changed(&self) -> WaitUntilChanged<'_, T> {
        // Capture the baseline version at the time the future is created.
        // SAFETY: the type is `!Sync`, and the value is copied without letting
        // a reference into the cell escape.
        let version = unsafe { (*self.inner.get()).version };
        WaitUntilChanged {
            swap:          self,
            start_version: version,
            waiter_id:     None,
        }
    }
}

/// Future that resolves when the inner Rc is swapped.
pub struct WaitUntilChanged<'a, T> {
    swap:          &'a RcSwap<T>,
    start_version: u64,
    waiter_id:     Option<usize>,
}

impl<'a, T> Future for WaitUntilChanged<'a, T> {
    type Output = Rc<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ptr = self.swap.inner.get();

        // SAFETY: Safe from concurrent mutation due to !Sync.
        let current_version = unsafe { (*ptr).version };

        if current_version != self.start_version {
            // SAFETY: the type is `!Sync`; cloning does not retain a reference
            // into the internal cell.
            Poll::Ready(unsafe { (*ptr).value.clone() })
        } else {
            let id = match self.waiter_id {
                | Some(id) => id,
                | None => {
                    // SAFETY: single-threaded access and no reference escapes.
                    let inner = unsafe { &mut *ptr };
                    let id = inner.next_waiter_id;
                    inner.next_waiter_id = id.wrapping_add(1);
                    self.waiter_id = Some(id);
                    id
                },
            };
            // SAFETY: single-threaded access and no reference escapes.
            let waiters = unsafe { &mut (*ptr).waiters };
            if let Some((_, waker)) = waiters.iter_mut().find(|(waiter_id, _)| *waiter_id == id) {
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
            } else {
                waiters.push((id, cx.waker().clone()));
            }

            Poll::Pending
        }
    }
}

impl<T> Drop for WaitUntilChanged<'_, T> {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id {
            // SAFETY: single-threaded access and no reference escapes.
            unsafe { &mut *self.swap.inner.get() }
                .waiters
                .retain(|(waiter_id, _)| *waiter_id != id);
        }
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

    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn async_rcswap() {
        let local = task::LocalSet::new();
        local
            .run_until(async move {
                let swap = Rc::new(RcSwap::new(Rc::new(10)));
                let changed = Rc::clone(&swap);
                let task = task::spawn_local(async move { changed.wait_until_changed().await });
                task::yield_now().await;
                swap.store(Rc::new(20));
                assert!(matches!(task.await, Ok(value) if *value == 20));
            })
            .await;
    }
}
