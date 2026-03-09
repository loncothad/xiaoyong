//! A thread-safe, reusable notification primitive.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{
            AtomicBool,
            AtomicUsize,
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

/// A thread-safe, reusable notification primitive.
pub struct Notify {
    generation: AtomicUsize,
    permit:     AtomicBool,
    waiters:    Mutex<SmallVec<[Option<Waker>; 8]>>,
}

impl Notify {
    /// Creates a new instance.
    pub fn new() -> Self {
        Self {
            generation: AtomicUsize::new(0),
            permit:     AtomicBool::new(false),
            waiters:    Mutex::new(SmallVec::new()),
        }
    }

    /// Triggers the notification, waking up tasks.
    pub fn notify(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.permit.store(true, Ordering::Release);

        let mut waiters = self.waiters.lock().unwrap();
        for waker in waiters.drain(..) {
            if let Some(w) = waker {
                w.wake();
            }
        }
    }

    /// Returns a future that resolves when the notification is triggered.
    pub fn wait(&self) -> WaitFuture<'_> {
        let start_generation = self.generation.load(Ordering::Acquire);
        WaitFuture {
            notify: self,
            start_generation,
            index: None,
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
    notify:           &'a Notify,
    start_generation: usize,
    index:            Option<usize>,
}

impl<'a> Future for WaitFuture<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.notify.generation.load(Ordering::Acquire) != self.start_generation {
            self.notify.permit.store(false, Ordering::Release);
            return Poll::Ready(());
        }

        if self.notify.permit.swap(false, Ordering::AcqRel) {
            return Poll::Ready(());
        }

        let mut waiters = self.notify.waiters.lock().unwrap();

        // Double check
        if self.notify.generation.load(Ordering::Acquire) != self.start_generation {
            self.notify.permit.store(false, Ordering::Release);
            return Poll::Ready(());
        }
        if self.notify.permit.swap(false, Ordering::AcqRel) {
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
            let mut waiters = self.notify.waiters.lock().unwrap();
            if idx < waiters.len() {
                waiters[idx] = None;
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
    async fn reusable_notify() {
        let notify = Arc::new(Notify::new());

        let t1 = tokio::spawn({
            let n = Arc::clone(&notify);
            async move { n.wait().await }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        notify.notify();
        t1.await.unwrap();

        let t2 = tokio::spawn({
            let n = Arc::clone(&notify);
            async move { n.wait().await }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        notify.notify();
        t2.await.unwrap();
    }
}
