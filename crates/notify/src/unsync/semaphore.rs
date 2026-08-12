//! Single-threaded asynchronous semaphore for limiting concurrent access.

use std::{
    cell::UnsafeCell,
    pin::Pin,
    rc::Rc,
    task::{
        Context,
        Poll,
        Waker,
    },
};

use smallvec::SmallVec;

struct Waiter {
    id:     usize,
    amount: usize,
    waker:  Waker,
}

struct Inner {
    permits: usize,
    next_id: usize,
    waiters: SmallVec<[Waiter; 8]>,
}

/// A fair, single-threaded asynchronous semaphore.
///
/// Requests are served in FIFO order. A request for multiple permits therefore
/// cannot be starved by newer one-permit requests.
pub struct Semaphore {
    inner: UnsafeCell<Inner>,
}

impl Semaphore {
    /// Creates a semaphore with `permits` initially available.
    #[must_use]
    pub fn new(permits: usize) -> Self {
        Self {
            inner: UnsafeCell::new(Inner {
                permits,
                next_id: 0,
                waiters: SmallVec::new(),
            }),
        }
    }

    /// Acquires one borrowed permit.
    #[must_use]
    pub fn acquire(&self) -> Acquire<'_> {
        self.acquire_many(1)
    }

    /// Acquires `amount` borrowed permits.
    #[must_use]
    pub fn acquire_many(&self, amount: usize) -> Acquire<'_> {
        Acquire {
            semaphore: self,
            amount,
            waiter_id: None,
            completed: false,
        }
    }

    /// Acquires one permit whose ownership includes an [`Rc`] to this
    /// semaphore.
    #[must_use]
    pub fn acquire_owned(self: Rc<Self>) -> AcquireOwned {
        self.acquire_many_owned(1)
    }

    /// Acquires `amount` permits whose ownership includes an [`Rc`] to this
    /// semaphore.
    #[must_use]
    pub fn acquire_many_owned(self: Rc<Self>, amount: usize) -> AcquireOwned {
        AcquireOwned {
            semaphore: self,
            amount,
            waiter_id: None,
            completed: false,
        }
    }

    /// Returns the current number of unclaimed permits.
    #[must_use]
    pub fn available_permits(&self) -> usize {
        // SAFETY: `Semaphore` is `!Sync`, and all access is confined to one
        // thread. No reference into `Inner` escapes this method.
        unsafe { (*self.inner.get()).permits }
    }

    fn poll_acquire(&self, amount: usize, waiter_id: &mut Option<usize>, context: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `Semaphore` is `!Sync`, and the mutable borrow remains scoped
        // to this method. Wakers are cloned but never invoked while borrowed.
        let inner = unsafe { &mut *self.inner.get() };

        if let Some(id) = *waiter_id {
            let index = inner
                .waiters
                .iter()
                .position(|waiter| waiter.id == id)
                .expect("registered semaphore waiter disappeared");
            if index == 0 && inner.permits >= amount {
                inner.permits -= amount;
                inner.waiters.remove(0);
                *waiter_id = None;
                return Poll::Ready(());
            }
            let waiter = &mut inner.waiters[index];
            if !waiter.waker.will_wake(context.waker()) {
                waiter.waker = context.waker().clone();
            }
            return Poll::Pending;
        }

        if inner.waiters.is_empty() && inner.permits >= amount {
            inner.permits -= amount;
            return Poll::Ready(());
        }

        let id = inner.next_id;
        inner.next_id = inner.next_id.wrapping_add(1);
        inner.waiters.push(Waiter {
            id,
            amount,
            waker: context.waker().clone(),
        });
        *waiter_id = Some(id);
        Poll::Pending
    }

    fn release(&self, amount: usize) {
        // SAFETY: see `poll_acquire`.
        let inner = unsafe { &mut *self.inner.get() };
        inner.permits = inner
            .permits
            .checked_add(amount)
            .expect("semaphore permit count overflowed");
        let waker = inner
            .waiters
            .first()
            .filter(|waiter| inner.permits >= waiter.amount)
            .map(|waiter| waiter.waker.clone());

        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn cancel(&self, id: usize) {
        // SAFETY: see `poll_acquire`.
        let inner = unsafe { &mut *self.inner.get() };
        let was_first = inner.waiters.first().is_some_and(|waiter| waiter.id == id);
        inner.waiters.retain(|waiter| waiter.id != id);
        let waker = was_first
            .then(|| inner.waiters.first())
            .flatten()
            .filter(|waiter| inner.permits >= waiter.amount)
            .map(|waiter| waiter.waker.clone());
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Future that acquires a borrowed [`Permit`].
pub struct Acquire<'a> {
    semaphore: &'a Semaphore,
    amount:    usize,
    waiter_id: Option<usize>,
    completed: bool,
}

impl<'a> Future for Acquire<'a> {
    type Output = Permit<'a>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.completed, "acquire future polled after completion");
        let semaphore = self.semaphore;
        let amount = self.amount;
        if semaphore.poll_acquire(amount, &mut self.waiter_id, context).is_ready() {
            self.completed = true;
            Poll::Ready(Permit {
                semaphore,
                amount,
            })
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id {
            self.semaphore.cancel(id);
        }
    }
}

/// An RAII permit borrowed from a [`Semaphore`].
pub struct Permit<'a> {
    semaphore: &'a Semaphore,
    amount:    usize,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.semaphore.release(self.amount);
    }
}

/// Future that acquires an [`OwnedPermit`].
pub struct AcquireOwned {
    semaphore: Rc<Semaphore>,
    amount:    usize,
    waiter_id: Option<usize>,
    completed: bool,
}

impl Future for AcquireOwned {
    type Output = OwnedPermit;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.completed, "acquire future polled after completion");
        let semaphore = Rc::clone(&self.semaphore);
        let amount = self.amount;
        if semaphore.poll_acquire(amount, &mut self.waiter_id, context).is_ready() {
            self.completed = true;
            Poll::Ready(OwnedPermit {
                semaphore,
                amount,
            })
        } else {
            Poll::Pending
        }
    }
}

impl Drop for AcquireOwned {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id {
            self.semaphore.cancel(id);
        }
    }
}

/// An RAII permit that owns an [`Rc`] to its [`Semaphore`].
pub struct OwnedPermit {
    semaphore: Rc<Semaphore>,
    amount:    usize,
}

impl Drop for OwnedPermit {
    fn drop(&mut self) {
        self.semaphore.release(self.amount);
    }
}

#[cfg(test)]
mod tests {
    use futures::poll;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn permits_are_returned_on_drop() {
        let semaphore = Rc::new(Semaphore::new(2));
        let first = semaphore.acquire().await;
        let second = Rc::clone(&semaphore).acquire_owned().await;
        assert_eq!(semaphore.available_permits(), 0);
        drop(first);
        drop(second);
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queue_is_fair_and_cancellation_safe() {
        let semaphore = Semaphore::new(1);
        let held = semaphore.acquire().await;
        let mut first = Box::pin(semaphore.acquire_many(2));
        let mut second = Box::pin(semaphore.acquire());
        assert!(poll!(first.as_mut()).is_pending());
        assert!(poll!(second.as_mut()).is_pending());

        drop(held);
        assert!(poll!(second.as_mut()).is_pending());
        drop(first);
        assert!(poll!(second.as_mut()).is_ready());
    }
}
