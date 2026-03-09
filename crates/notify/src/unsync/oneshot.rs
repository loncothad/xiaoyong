//! Single-threaded, one-time notification primitive.

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

/// Single-threaded, one-time notification primitive.
///
/// **Thread Safety:** This type is designed for single-threaded executors and
/// is !Send.
pub struct Notify {
    fired:   Cell<bool>,
    waiters: Cell<SmallVec<[Option<Waker>; 8]>>,
}

impl Notify {
    /// Creates a new instance.
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            fired:   Cell::new(false),
            waiters: Cell::new(SmallVec::new()),
        })
    }

    /// Fires the notification, waking up waiting tasks.
    pub fn fire(&self) {
        if !self.fired.get() {
            self.fired.set(true);

            let mut waiters = self.waiters.take();
            for waker in waiters.drain(..) {
                if let Some(w) = waker {
                    w.wake();
                }
            }
        }
    }

    /// Returns a future that resolves when the notification is triggered.
    pub fn wait(self: &Rc<Self>) -> WaitFuture {
        WaitFuture {
            notify: Rc::clone(self),
            index:  None,
        }
    }
}

/// Future that resolves when the notification is triggered.
pub struct WaitFuture {
    notify: Rc<Notify>,
    index:  Option<usize>,
}

impl Future for WaitFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.notify.fired.get() {
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

impl Drop for WaitFuture {
    fn drop(&mut self) {
        if let Some(idx) = self.index {
            // Only clean up if it hasn't fired (if fired, arena is already consumed)
            if !self.notify.fired.get() {
                let mut waiters = self.notify.waiters.take();
                if idx < waiters.len() {
                    waiters[idx] = None;
                }
                self.notify.waiters.set(waiters);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn notify_oneshot() {
        let local = task::LocalSet::new();
        local
            .run_until(async move {
                let notify = Notify::new();
                let notify_clone = Rc::clone(&notify);

                task::spawn_local(async move {
                    notify_clone.fire();
                });

                notify.wait().await;
            })
            .await;
    }
}
