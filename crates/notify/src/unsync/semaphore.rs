//! Single-threaded asynchronous semaphore for limiting concurrent access.

use std::{
    cell::RefCell,
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
    waker:  Option<Waker>,
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
    inner: RefCell<Inner>,
}

impl Semaphore {
    /// Creates a semaphore with `permits` initially available.
    #[must_use]
    pub fn new(permits: usize) -> Self {
        Self {
            inner: RefCell::new(Inner {
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
        self.inner.borrow().permits
    }

    fn poll_acquire(&self, amount: usize, waiter_id: &mut Option<usize>, context: &mut Context<'_>) -> Poll<()> {
        let waker = context.waker().clone();
        let mut inner = self.inner.borrow_mut();

        if let Some(id) = *waiter_id {
            let index = inner
                .waiters
                .iter()
                .position(|waiter| waiter.id == id)
                .expect("registered semaphore waiter disappeared");
            if index == 0 && inner.permits >= amount {
                inner.permits -= amount;
                let removed = inner.waiters.remove(0);
                *waiter_id = None;
                drop(inner);
                drop(removed);
                self.wake_next();
                return Poll::Ready(());
            }
            let waiter = &mut inner.waiters[index];
            if waiter
                .waker
                .as_ref()
                .is_none_or(|registered| !registered.will_wake(context.waker()))
            {
                let old = waiter.waker.replace(waker);
                drop(inner);
                drop(old);
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
            waker: Some(waker),
        });
        *waiter_id = Some(id);
        Poll::Pending
    }

    fn release(&self, amount: usize) {
        {
            let mut inner = self.inner.borrow_mut();
            inner.permits = inner
                .permits
                .checked_add(amount)
                .expect("semaphore permit count overflowed");
        }
        self.wake_next();
    }

    fn wake_next(&self) {
        let waker = {
            let mut inner = self.inner.borrow_mut();
            let permits = inner.permits;
            inner
                .waiters
                .first_mut()
                .filter(|waiter| permits >= waiter.amount)
                .and_then(|waiter| waiter.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn cancel(&self, id: usize) {
        let removed = {
            let mut inner = self.inner.borrow_mut();
            inner
                .waiters
                .iter()
                .position(|waiter| waiter.id == id)
                .map(|index| inner.waiters.remove(index))
        };
        drop(removed);
        self.wake_next();
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

    #[test]
    fn acquiring_front_waiter_wakes_next_eligible_request() {
        use std::{
            sync::{
                Arc,
                atomic::{
                    AtomicUsize,
                    Ordering,
                },
            },
            task::Wake,
        };
        struct Counter(AtomicUsize);
        impl Wake for Counter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let semaphore = Semaphore::new(2);
        let mut cx = Context::from_waker(Waker::noop());
        let held = match Box::pin(semaphore.acquire_many(2)).as_mut().poll(&mut cx) {
            | Poll::Ready(permit) => permit,
            | _ => panic!("initial permits available"),
        };
        let mut first = Box::pin(semaphore.acquire());
        let mut second = Box::pin(semaphore.acquire());
        assert!(first.as_mut().poll(&mut cx).is_pending());
        let counter = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut second_cx = Context::from_waker(&waker);
        assert!(second.as_mut().poll(&mut second_cx).is_pending());
        drop(held);
        let first_permit = match first.as_mut().poll(&mut cx) {
            | Poll::Ready(permit) => permit,
            | _ => panic!("released permits available"),
        };
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert!(second.as_mut().poll(&mut second_cx).is_ready());
        drop(first_permit);
    }
}
