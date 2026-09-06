//! Reusable broadcast notification with one coalesced permit.

use std::{
    pin::Pin,
    sync::Mutex,
    task::{
        Context,
        Poll,
        Waker,
    },
};

use smallvec::SmallVec;

#[derive(Default)]
struct Inner {
    generation: usize,
    permit:     bool,
    waiters:    SmallVec<[Option<Waker>; 8]>,
}

/// A thread-safe reusable notification primitive.
///
/// A notification completes all futures created before it. If no futures are
/// registered, it also stores one permit for the next waiter. Repeated stored
/// notifications coalesce into one permit.
pub struct Notify {
    inner: Mutex<Inner>,
}

impl Notify {
    /// Creates a new instance.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Completes existing waiters, or stores one permit if none are registered.
    pub fn notify(&self) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            inner.generation = inner.generation.wrapping_add(1);
            inner.permit = inner.waiters.iter().all(Option::is_none);
            std::mem::take(&mut inner.waiters)
        };
        for waker in wakers.into_iter().flatten() {
            waker.wake();
        }
    }

    /// Returns a future that resolves when a notification is available.
    pub fn wait(&self) -> WaitFuture<'_> {
        let start_generation = self.inner.lock().unwrap_or_else(|p| p.into_inner()).generation;
        WaitFuture {
            notify: self,
            start_generation,
            index: None,
            completed: false,
        }
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

/// Future that resolves when a notification is available.
pub struct WaitFuture<'a> {
    notify:           &'a Notify,
    start_generation: usize,
    index:            Option<usize>,
    completed:        bool,
}

impl Future for WaitFuture<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.completed {
            return Poll::Ready(());
        }
        // Clone before borrowing the state: a custom waker can run user code.
        let waker = cx.waker().clone();
        let notify = self.notify;
        let mut inner = notify.inner.lock().unwrap_or_else(|p| p.into_inner());
        if inner.generation != self.start_generation || inner.permit {
            // A waiter from an older generation must not consume a newer permit.
            if inner.generation == self.start_generation {
                inner.permit = false;
            }
            self.index = None;
            self.completed = true;
            return Poll::Ready(());
        }
        let index = self.index.unwrap_or_else(|| {
            inner.waiters.iter().position(Option::is_none).unwrap_or_else(|| {
                inner.waiters.push(None);
                inner.waiters.len() - 1
            })
        });
        let old = inner.waiters[index].replace(waker);
        self.index = Some(index);
        drop(inner);
        drop(old);
        Poll::Pending
    }
}

impl Drop for WaitFuture<'_> {
    fn drop(&mut self) {
        let old = {
            let mut inner = self.notify.inner.lock().unwrap_or_else(|p| p.into_inner());
            // Slots are reused after notify. Only remove our own generation.
            if inner.generation == self.start_generation {
                self.index.and_then(|index| inner.waiters[index].take())
            } else {
                None
            }
        };
        drop(old);
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

    #[test]
    fn old_waiter_cannot_remove_a_new_generation() {
        use std::{
            sync::{
                Arc as StdArc,
                atomic::{
                    AtomicUsize,
                    Ordering,
                },
            },
            task::{
                Wake,
                Waker,
            },
        };
        struct Counter(AtomicUsize);
        impl Wake for Counter {
            fn wake(self: StdArc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let notify = Notify::new();
        let counter = StdArc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(StdArc::clone(&counter));
        let mut cx = Context::from_waker(&waker);
        let mut old = Box::pin(notify.wait());
        assert!(old.as_mut().poll(&mut cx).is_pending());
        notify.notify();
        let mut new = Box::pin(notify.wait());
        assert!(new.as_mut().poll(&mut cx).is_pending());
        drop(old);
        counter.0.store(0, Ordering::Relaxed);
        notify.notify();
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert!(new.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn old_waiter_does_not_consume_a_later_stored_permit() {
        let notify = Notify::new();
        let mut old = Box::pin(notify.wait());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(old.as_mut().poll(&mut cx).is_pending());
        notify.notify();
        notify.notify();
        assert!(old.as_mut().poll(&mut cx).is_ready());
        assert!(Box::pin(notify.wait()).as_mut().poll(&mut cx).is_ready());
        assert!(Box::pin(notify.wait()).as_mut().poll(&mut cx).is_pending());
    }
}
