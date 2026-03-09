//! Single-threaded notification primitive supporting queuing.

use std::{
    cell::Cell,
    pin::Pin,
    rc::Rc,
    task::{
        Context,
        Poll,
        Waker,
    },
};

use smallvec::SmallVec;

/// Single-threaded notification primitive supporting queuing.
///
/// **Thread Safety:** This type is designed for single-threaded executors and
/// is !Send.
pub struct Notify {
    permit:  Cell<bool>,
    waiters: Cell<SmallVec<[Option<Waker>; 8]>>,
}

impl Notify {
    /// Creates a new instance.
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            permit:  Cell::new(false),
            waiters: Cell::new(SmallVec::new()),
        })
    }

    /// Wakes up a single waiting task.
    pub fn notify_one(&self) {
        let mut waiters = self.waiters.take();
        let mut woken = false;

        for waker_opt in waiters.iter_mut() {
            if let Some(waker) = waker_opt.take() {
                waker.wake();
                woken = true;
                break;
            }
        }

        self.waiters.set(waiters);

        if !woken {
            self.permit.set(true);
        }
    }

    /// Wakes up all waiting tasks.
    pub fn notify_waiters(&self) {
        let mut waiters = self.waiters.take();
        for waker in waiters.drain(..) {
            if let Some(w) = waker {
                w.wake();
            }
        }
        self.waiters.set(waiters);
    }

    /// Returns a future that resolves when notified.
    pub fn notified(self: &Rc<Self>) -> NotifedFuture {
        NotifedFuture {
            notify: Rc::clone(self),
            index:  None,
        }
    }
}

/// Future that resolves when the notification is received.
pub struct NotifedFuture {
    notify: Rc<Notify>,
    index:  Option<usize>,
}

impl Future for NotifedFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.notify.permit.get() {
            self.notify.permit.set(false);
            return Poll::Ready(());
        }

        let mut waiters = self.notify.waiters.take();
        if let Some(idx) = self.index {
            waiters[idx] = Some(cx.waker().clone());
        } else if let Some(idx) = waiters.iter().position(|w| w.is_none()) {
            waiters[idx] = Some(cx.waker().clone());
            self.index = Some(idx);
        } else {
            let idx = waiters.len();
            waiters.push(Some(cx.waker().clone()));
            self.index = Some(idx);
        }
        self.notify.waiters.set(waiters);

        Poll::Pending
    }
}

impl Drop for NotifedFuture {
    fn drop(&mut self) {
        if let Some(idx) = self.index {
            let mut waiters = self.notify.waiters.take();
            if idx < waiters.len() {
                waiters[idx] = None;
            }
            self.notify.waiters.set(waiters);
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn notify_queued() {
        let local = task::LocalSet::new();
        local
            .run_until(async move {
                let notify = Notify::new();
                let notify_clone = Rc::clone(&notify);

                let task = task::spawn_local(async move {
                    notify_clone.notified().await;
                    true
                });

                notify.notify_one();
                assert!(task.await.unwrap());
            })
            .await;
    }
}
