//! A thread-safe, one-time notification primitive.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    task::{
        Context,
        Poll,
        Waker,
    },
};

use smallvec::SmallVec;

/// A thread-safe, one-time notification primitive.
pub struct Notify {
    fired:   AtomicBool,
    waiters: Mutex<SmallVec<[Option<Waker>; 8]>>,
}

impl Notify {
    /// Creates a new instance.
    pub fn new() -> Self {
        Self {
            fired:   AtomicBool::new(false),
            waiters: Mutex::new(SmallVec::new()),
        }
    }

    /// Fires the notification, waking up waiting tasks.
    pub fn fire(&self) {
        if !self.fired.swap(true, Ordering::Release) {
            let mut waiters = self.waiters.lock().unwrap();
            for waker in waiters.drain(..) {
                if let Some(w) = waker {
                    w.wake();
                }
            }
        }
    }

    /// Returns a future that resolves when the notification is triggered.
    pub fn wait(&self) -> WaitFuture<'_> {
        WaitFuture {
            notify: self,
            index:  None,
        }
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

/// Future that resolves when the notification is triggered.
pub struct WaitFuture<'a> {
    notify: &'a Notify,
    index:  Option<usize>,
}

impl<'a> Future for WaitFuture<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.notify.fired.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        let mut waiters = self.notify.waiters.lock().unwrap();

        // Double check after lock
        if self.notify.fired.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

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

        Poll::Pending
    }
}

impl<'a> Drop for WaitFuture<'a> {
    fn drop(&mut self) {
        if let Some(idx) = self.index {
            if !self.notify.fired.load(Ordering::Acquire) {
                let mut waiters = self.notify.waiters.lock().unwrap();
                if idx < waiters.len() {
                    waiters[idx] = None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::Duration;

    use super::*;

    #[tokio::test]
    async fn oneshot_notify() {
        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);

        let task = tokio::spawn(async move {
            notify_clone.wait().await;
            true
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        notify.fire();

        assert!(task.await.unwrap());
    }

    #[tokio::test]
    async fn oneshot_multiple_waiters() {
        let notify = Arc::new(Notify::new());

        let t1 = tokio::spawn({
            let n = Arc::clone(&notify);
            async move { n.wait().await }
        });
        let t2 = tokio::spawn({
            let n = Arc::clone(&notify);
            async move { n.wait().await }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        notify.fire();

        t1.await.unwrap();
        t2.await.unwrap();
    }
}
