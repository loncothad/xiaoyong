//! Lock-free swappable values with asynchronous change notification.

use core::{
    future::Future,
    pin::Pin,
    sync::atomic::{
        AtomicU64,
        Ordering,
    },
    task::{
        Context,
        Poll,
    },
};

use event_listener::{
    Event,
    EventListener,
};
use triomphe::Arc;

use super::arcswap;

/// A thread-safe [`Arc`] container that can asynchronously report changes.
///
/// Value access delegates to the lock-free [`arcswap::ArcSwap`]. Waiters are
/// registered with an event listener, avoiding a mutex-protected waiter list.
pub struct ArcSwap<T> {
    value:   arcswap::ArcSwap<T>,
    version: AtomicU64,
    changed: Event,
}

impl<T> ArcSwap<T> {
    /// Creates a container with `value` as its initial snapshot.
    #[must_use]
    pub fn new(value: Arc<T>) -> Self {
        Self {
            value:   arcswap::ArcSwap::new(value),
            version: AtomicU64::new(0),
            changed: Event::new(),
        }
    }

    /// Returns a lock-free snapshot of the current value.
    #[must_use]
    pub fn load(&self) -> Arc<T> {
        self.value.load()
    }

    /// Replaces the current value and wakes every change waiter.
    pub fn store(&self, value: Arc<T>) {
        drop(self.swap(value));
    }

    /// Replaces the current value, returns the old snapshot, and wakes every
    /// change waiter.
    pub fn swap(&self, value: Arc<T>) -> Arc<T> {
        let old = self.value.swap(value);
        self.version.fetch_add(1, Ordering::Release);
        self.changed.notify(usize::MAX);
        old
    }

    /// Returns a future that waits for a change after this call and then loads
    /// the current snapshot. Multiple intervening changes may be coalesced.
    #[must_use]
    pub fn wait_until_changed(&self) -> WaitUntilChanged<'_, T> {
        let start_version = self.version.load(Ordering::Acquire);
        // Register immediately. If a change races listener creation, the
        // version check in `poll` still observes it.
        let listener = self.changed.listen();
        WaitUntilChanged {
            swap: self,
            start_version,
            listener,
        }
    }
}

/// Future returned by [`ArcSwap::wait_until_changed`].
pub struct WaitUntilChanged<'a, T> {
    swap:          &'a ArcSwap<T>,
    start_version: u64,
    listener:      EventListener,
}

impl<T> Future for WaitUntilChanged<'_, T> {
    type Output = Arc<T>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.swap.version.load(Ordering::Acquire) != self.start_version {
            return Poll::Ready(self.swap.load());
        }

        match Pin::new(&mut self.listener).poll(context) {
            | Poll::Ready(()) => Poll::Ready(self.swap.load()),
            | Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: Default> Default for ArcSwap<T> {
    fn default() -> Self {
        Self::new(Arc::new(T::default()))
    }
}

impl<T> From<Arc<T>> for ArcSwap<T> {
    fn from(value: Arc<T>) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use super::*;

    #[tokio::test]
    async fn all_waiters_observe_the_next_value() {
        let swap = StdArc::new(ArcSwap::new(Arc::new(10)));
        let first = {
            let swap = StdArc::clone(&swap);
            tokio::spawn(async move { swap.wait_until_changed().await })
        };
        let second = {
            let swap = StdArc::clone(&swap);
            tokio::spawn(async move { swap.wait_until_changed().await })
        };
        tokio::task::yield_now().await;

        swap.store(Arc::new(20));
        assert!(matches!(first.await, Ok(value) if *value == 20));
        assert!(matches!(second.await, Ok(value) if *value == 20));
    }

    #[tokio::test]
    async fn change_before_first_poll_is_not_lost() {
        let swap = ArcSwap::new(Arc::new(1));
        let changed = swap.wait_until_changed();
        swap.store(Arc::new(2));
        assert_eq!(*changed.await, 2);
    }

    #[tokio::test]
    async fn canceled_waiter_does_not_affect_later_waiters() {
        let swap = ArcSwap::new(Arc::new(1));
        drop(swap.wait_until_changed());
        let changed = swap.wait_until_changed();
        swap.store(Arc::new(2));
        assert_eq!(*changed.await, 2);
    }
}
