//! Thread-safe, one-time notification primitive.

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

/// Thread-safe, one-time notification primitive.
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
            let wakers = {
                let mut waiters = self.waiters.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                waiters.drain(..).flatten().collect::<SmallVec<[_; 8]>>()
            };
            for waker in wakers {
                waker.wake();
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

        let new_waker = cx.waker().clone();
        let mut waiters = self
            .notify
            .waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Double check after lock
        if self.notify.fired.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        let index = self.index.unwrap_or_else(|| {
            waiters.iter().position(Option::is_none).unwrap_or_else(|| {
                waiters.push(None);
                waiters.len() - 1
            })
        });
        let old_waker = waiters[index].replace(new_waker);
        self.index = Some(index);
        drop(waiters);
        drop(old_waker);

        Poll::Pending
    }
}

impl<'a> Drop for WaitFuture<'a> {
    fn drop(&mut self) {
        if let Some(idx) = self.index {
            if !self.notify.fired.load(Ordering::Acquire) {
                let mut waiters = self
                    .notify
                    .waiters
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let old_waker = waiters.get_mut(idx).and_then(Option::take);
                drop(waiters);
                drop(old_waker);
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

    #[test]
    fn replaced_waker_can_reenter_notification_on_drop() {
        use std::task::Wake;
        struct FireOnDrop(Arc<Notify>);
        impl Wake for FireOnDrop {
            fn wake(self: Arc<Self>) {
                self.0.fire();
            }
        }
        impl Drop for FireOnDrop {
            fn drop(&mut self) {
                assert!(self.0.waiters.try_lock().is_ok(), "state still locked during callback");
                self.0.fire();
            }
        }
        let notify = Arc::new(Notify::new());
        let waker = Waker::from(Arc::new(FireOnDrop(Arc::clone(&notify))));
        let mut future = Box::pin(notify.wait());
        assert!(future.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        drop(waker);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert!(future.as_mut().poll(&mut cx).is_ready());
    }
}
