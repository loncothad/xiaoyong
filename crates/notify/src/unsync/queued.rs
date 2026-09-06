//! Single-threaded notification primitive with queued permits.

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
    id:           usize,
    notified:     bool,
    transferable: bool,
    waker:        Option<Waker>,
}

#[derive(Default)]
struct Inner {
    permits: usize,
    next_id: usize,
    waiters: SmallVec<[Waiter; 8]>,
}

/// A single-threaded notification queue.
///
/// Calls to [`notify_one`](Self::notify_one) made without a waiter accumulate
/// permits. Once waiters are registered, notifications are assigned in FIFO
/// order and cannot be stolen by a newer waiter.
pub struct Notify {
    inner: RefCell<Inner>,
}

impl Notify {
    /// Creates an empty notification queue.
    #[must_use]
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            inner: RefCell::new(Inner {
                permits: 0,
                next_id: 0,
                waiters: SmallVec::new(),
            }),
        })
    }

    /// Delivers one notification to the oldest waiter, or queues a permit.
    pub fn notify_one(&self) {
        let mut inner = self.inner.borrow_mut();
        let waker = if let Some(waiter) = inner.waiters.iter_mut().find(|waiter| !waiter.notified) {
            waiter.notified = true;
            waiter.transferable = true;
            waiter.waker.take()
        } else {
            inner.permits = inner.permits.saturating_add(1);
            None
        };
        drop(inner);

        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Delivers a notification to every waiter currently registered.
    ///
    /// This method does not store a permit when there are no waiters.
    pub fn notify_waiters(&self) {
        let mut inner = self.inner.borrow_mut();
        let mut wakers = SmallVec::<[Waker; 8]>::new();
        for waiter in &mut inner.waiters {
            waiter.notified = true;
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            }
        }
        drop(inner);

        for waker in wakers {
            waker.wake();
        }
    }

    /// Returns a future that consumes one notification.
    #[must_use]
    pub fn notified(self: &Rc<Self>) -> NotifiedFuture {
        NotifiedFuture {
            notify:    Rc::clone(self),
            id:        None,
            completed: false,
        }
    }
}

/// Future returned by [`Notify::notified`].
pub struct NotifiedFuture {
    notify:    Rc<Notify>,
    id:        Option<usize>,
    completed: bool,
}

/// Backwards-compatible alias for the misspelled original type name.
#[deprecated(since = "1.1.0", note = "use NotifiedFuture")]
pub type NotifedFuture = NotifiedFuture;

impl Future for NotifiedFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.completed {
            return Poll::Ready(());
        }

        let waker = context.waker().clone();
        let notify = Rc::clone(&self.notify);
        let mut inner = notify.inner.borrow_mut();
        if let Some(id) = self.id {
            if let Some(index) = inner.waiters.iter().position(|waiter| waiter.id == id) {
                if inner.waiters[index].notified {
                    let removed = inner.waiters.remove(index);
                    self.id = None;
                    self.completed = true;
                    drop(inner);
                    drop(removed);
                    return Poll::Ready(());
                }

                let waiter = &mut inner.waiters[index];
                if waiter
                    .waker
                    .as_ref()
                    .is_none_or(|waker| !waker.will_wake(context.waker()))
                {
                    let old = waiter.waker.replace(waker);
                    drop(inner);
                    drop(old);
                } else {
                    drop(inner);
                }
                return Poll::Pending;
            }
        }

        if inner.permits > 0 {
            inner.permits -= 1;
            self.completed = true;
            drop(inner);
            return Poll::Ready(());
        }

        let id = inner.next_id;
        inner.next_id = inner.next_id.wrapping_add(1);
        inner.waiters.push(Waiter {
            id,
            notified: false,
            transferable: false,
            waker: Some(waker),
        });
        self.id = Some(id);
        drop(inner);
        Poll::Pending
    }
}

impl Drop for NotifiedFuture {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            let mut inner = self.notify.inner.borrow_mut();
            let removed = inner
                .waiters
                .iter()
                .position(|waiter| waiter.id == id)
                .map(|index| inner.waiters.remove(index));
            drop(inner);
            if removed.as_ref().is_some_and(|waiter| waiter.transferable) {
                self.notify.notify_one();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::poll;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn notification_makes_registered_future_ready() {
        let notify = Notify::new();
        let mut future = Box::pin(notify.notified());
        assert!(poll!(future.as_mut()).is_pending());
        notify.notify_one();
        assert!(poll!(future.as_mut()).is_ready());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn permits_queue_and_waiters_are_fifo() {
        let notify = Notify::new();
        notify.notify_one();
        notify.notify_one();
        notify.notified().await;
        notify.notified().await;

        let mut first = Box::pin(notify.notified());
        let mut second = Box::pin(notify.notified());
        assert!(poll!(first.as_mut()).is_pending());
        assert!(poll!(second.as_mut()).is_pending());
        notify.notify_one();
        assert!(poll!(first.as_mut()).is_ready());
        assert!(poll!(second.as_mut()).is_pending());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_waiter_does_not_consume_notification() {
        let notify = Notify::new();
        let mut canceled = Box::pin(notify.notified());
        assert!(poll!(canceled.as_mut()).is_pending());
        drop(canceled);

        notify.notify_one();
        notify.notified().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_assigned_notification_is_transferred() {
        let notify = Notify::new();
        let mut first = Box::pin(notify.notified());
        let mut second = Box::pin(notify.notified());
        assert!(poll!(first.as_mut()).is_pending());
        assert!(poll!(second.as_mut()).is_pending());
        notify.notify_one();
        drop(first);
        assert!(poll!(second.as_mut()).is_ready());

        let mut canceled = Box::pin(notify.notified());
        assert!(poll!(canceled.as_mut()).is_pending());
        notify.notify_one();
        drop(canceled);
        assert!(poll!(Box::pin(notify.notified())).is_ready());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_broadcast_does_not_create_a_permit() {
        let notify = Notify::new();
        let mut first = Box::pin(notify.notified());
        assert!(poll!(first.as_mut()).is_pending());
        notify.notify_waiters();
        drop(first);
        assert!(poll!(Box::pin(notify.notified())).is_pending());
    }
}
