//! A single-threaded asynchronous semaphore for limiting concurrent access.

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

struct Inner {
    permits: usize,
    wakers:  SmallVec<[Waker; 8]>,
}

/// A single-threaded asynchronous semaphore.
///
/// **Thread Safety:** This type is designed for single-threaded executors and
/// is !Send.
pub struct Semaphore {
    inner: UnsafeCell<Inner>,
}

impl Semaphore {
    /// Create a new instance.
    pub fn new(permits: usize) -> Self {
        Self {
            inner: UnsafeCell::new(Inner {
                permits,
                wakers: SmallVec::new(),
            }),
        }
    }

    /// Acquire one permit tied to the reference lifetime.
    pub fn acquire(&self) -> Acquire<'_> {
        Acquire {
            semaphore: self,
            amount:    1,
        }
    }

    /// Acquire a specified number of permits tied to the reference lifetime.
    pub fn acquire_many(&self, amount: usize) -> Acquire<'_> {
        Acquire {
            semaphore: self,
            amount,
        }
    }

    /// Acquire one permit, returning an owned permit.
    pub fn acquire_owned(self: Rc<Self>) -> AcquireOwned {
        AcquireOwned {
            semaphore: self,
            amount:    1,
        }
    }

    /// Acquire a specified number of permits, returning an owned permit.
    pub fn acquire_many_owned(self: Rc<Self>, amount: usize) -> AcquireOwned {
        AcquireOwned {
            semaphore: self,
            amount,
        }
    }

    fn release(&self, amount: usize) {
        // SAFETY: Safe from concurrent mutation due to !Sync.
        let inner = unsafe { &mut *self.inner.get() };
        inner.permits = inner.permits.checked_add(amount).expect("Semaphore permit overflow");

        for waker in inner.wakers.drain(..) {
            waker.wake();
        }
    }

    /// Returns the current number of available permits.
    pub fn available_permits(&self) -> usize {
        // SAFETY: Safe from concurrent mutation due to !Sync.
        unsafe { (*self.inner.get()).permits }
    }
}

/// Future that resolves to a permit when acquired.
pub struct Acquire<'a> {
    semaphore: &'a Semaphore,
    amount:    usize,
}

impl<'a> Future for Acquire<'a> {
    type Output = Permit<'a>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: Safe from concurrent mutation due to !Sync.
        let inner = unsafe { &mut *self.semaphore.inner.get() };

        if inner.permits >= self.amount {
            inner.permits -= self.amount;
            Poll::Ready(Permit {
                semaphore: self.semaphore,
                amount:    self.amount,
            })
        } else {
            if !inner.wakers.iter().any(|w| w.will_wake(cx.waker())) {
                inner.wakers.push(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}

/// A permit acquired from the semaphore.
pub struct Permit<'a> {
    semaphore: &'a Semaphore,
    amount:    usize,
}

impl<'a> Drop for Permit<'a> {
    fn drop(&mut self) {
        self.semaphore.release(self.amount);
    }
}

/// Future that resolves to an owned permit when acquired.
pub struct AcquireOwned {
    semaphore: Rc<Semaphore>,
    amount:    usize,
}

impl Future for AcquireOwned {
    type Output = OwnedPermit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: Safe from concurrent mutation due to !Sync.
        let inner = unsafe { &mut *self.semaphore.inner.get() };

        if inner.permits >= self.amount {
            inner.permits -= self.amount;
            Poll::Ready(OwnedPermit {
                semaphore: Rc::clone(&self.semaphore),
                amount:    self.amount,
            })
        } else {
            if !inner.wakers.iter().any(|w| w.will_wake(cx.waker())) {
                inner.wakers.push(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}

/// An owned permit acquired from the semaphore.
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
    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn semaphore() {
        let local = task::LocalSet::new();
        local
            .run_until(async move {
                let sem = Rc::new(Semaphore::new(2));

                let permit1 = sem.acquire().await;
                assert_eq!(sem.available_permits(), 1);

                let permit2 = Rc::clone(&sem).acquire_owned().await;
                assert_eq!(sem.available_permits(), 0);

                drop(permit1);
                assert_eq!(sem.available_permits(), 1);
                drop(permit2);
                assert_eq!(sem.available_permits(), 2);
            })
            .await;
    }
}
