//! Single-threaded, asynchronous primitive for atomic swapping of Rc
//! pointers.

use std::{
    cell::RefCell,
    future::Future,
    mem,
    pin::Pin,
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
    inner: RefCell<Inner<T>>,
}

impl<T> RcSwap<T> {
    /// Create a new instance.
    pub fn new(value: Rc<T>) -> Self {
        Self {
            inner: RefCell::new(Inner {
                value,
                version: 0,
                next_waiter_id: 0,
                waiters: SmallVec::new(),
            }),
        }
    }

    /// Load the current value.
    pub fn load(&self) -> Rc<T> {
        Rc::clone(&self.inner.borrow().value)
    }

    /// Replace the current value with a new one, dropping the old value.
    pub fn store(&self, value: Rc<T>) {
        mem::drop(self.swap(value));
    }

    /// Replace the current value, wake pending futures, and returns the old
    /// value.
    pub fn swap(&self, value: Rc<T>) -> Rc<T> {
        let (old_value, waiters) = {
            let mut inner = self.inner.borrow_mut();
            let old_value = mem::replace(&mut inner.value, value);
            inner.version = inner.version.wrapping_add(1);
            (old_value, mem::take(&mut inner.waiters))
        };
        for (_, waker) in waiters {
            waker.wake();
        }

        old_value
    }

    /// Returns a Future that resolves with the new value once it changes.
    pub fn wait_until_changed(&self) -> WaitUntilChanged<'_, T> {
        // Capture the baseline version at the time the future is created.
        let version = self.inner.borrow().version;
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
        let waker = cx.waker().clone();
        let mut inner = self.swap.inner.borrow_mut();
        if inner.version != self.start_version {
            return Poll::Ready(Rc::clone(&inner.value));
        }
        let id = self.waiter_id.unwrap_or_else(|| {
            let id = inner.next_waiter_id;
            inner.next_waiter_id = id.checked_add(1).expect("waiter IDs exhausted");
            self.waiter_id = Some(id);
            id
        });
        let old = if let Some((_, registered)) = inner.waiters.iter_mut().find(|(i, _)| *i == id) {
            Some(mem::replace(registered, waker))
        } else {
            inner.waiters.push((id, waker));
            None
        };
        drop(inner);
        drop(old);
        Poll::Pending
    }
}

impl<T> Drop for WaitUntilChanged<'_, T> {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id {
            let removed = {
                let mut inner = self.swap.inner.borrow_mut();
                inner
                    .waiters
                    .iter()
                    .position(|(i, _)| *i == id)
                    .map(|index| inner.waiters.remove(index))
            };
            drop(removed);
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

    #[test]
    fn change_before_first_poll_and_old_waiter_drop_are_safe() {
        let swap = RcSwap::from(Rc::new(1));
        let mut old = Box::pin(swap.wait_until_changed());
        swap.store(Rc::new(2));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(old.as_mut().poll(&mut cx), Poll::Ready(value) if *value == 2));
        let mut new = Box::pin(swap.wait_until_changed());
        assert!(new.as_mut().poll(&mut cx).is_pending());
        drop(old);
        swap.store(Rc::new(3));
        assert!(matches!(new.as_mut().poll(&mut cx), Poll::Ready(value) if *value == 3));
    }
}
