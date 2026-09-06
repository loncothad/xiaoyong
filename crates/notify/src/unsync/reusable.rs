//! Reusable broadcast notification with one coalesced permit.

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

#[derive(Default)]
struct Inner {
    generation: usize,
    permit:     bool,
    waiters:    SmallVec<[Option<Waker>; 8]>,
}

/// A single-threaded reusable notification primitive.
///
/// A notification completes all futures created before it. If no futures are
/// registered, it also stores one permit for the next waiter. Repeated stored
/// notifications coalesce into one permit.
pub struct Notify {
    inner: RefCell<Inner>,
}

impl Notify {
    /// Creates a new instance.
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            inner: RefCell::new(Inner::default()),
        })
    }

    /// Completes existing waiters, or stores one permit if none are registered.
    pub fn notify(&self) {
        let wakers = {
            let mut inner = self.inner.borrow_mut();
            inner.generation = inner.generation.wrapping_add(1);
            inner.permit = inner.waiters.iter().all(Option::is_none);
            std::mem::take(&mut inner.waiters)
        };
        for waker in wakers.into_iter().flatten() {
            waker.wake();
        }
    }

    /// Returns a future that resolves when a notification is available.
    pub fn wait(self: &Rc<Self>) -> WaitFuture {
        let start_generation = self.inner.borrow_mut().generation;
        WaitFuture {
            notify: Rc::clone(self),
            start_generation,
            index: None,
            completed: false,
        }
    }
}

/// Future that resolves when a notification is available.
pub struct WaitFuture {
    notify:           Rc<Notify>,
    start_generation: usize,
    index:            Option<usize>,
    completed:        bool,
}

impl Future for WaitFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.completed {
            return Poll::Ready(());
        }
        // Clone before borrowing the state: a custom waker can run user code.
        let waker = cx.waker().clone();
        let notify = Rc::clone(&self.notify);
        let mut inner = notify.inner.borrow_mut();
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

impl Drop for WaitFuture {
    fn drop(&mut self) {
        let old = {
            let mut inner = self.notify.inner.borrow_mut();
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
    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn notify_reusable() {
        let local = task::LocalSet::new();
        local
            .run_until(async move {
                let notify = Notify::new();

                let n1 = Rc::clone(&notify);
                task::spawn_local(async move {
                    n1.notify();
                });
                notify.wait().await;

                let n2 = Rc::clone(&notify);
                task::spawn_local(async move {
                    n2.notify();
                });
                notify.wait().await;
            })
            .await;
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
