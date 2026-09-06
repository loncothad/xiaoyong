//! A single-threaded, asynchronous reader-writer lock.

use core::{
    cell::{
        Cell,
        UnsafeCell,
    },
    future::Future,
    ops::{
        Deref,
        DerefMut,
    },
    pin::Pin,
    task::{
        Context,
        Poll,
        Waker,
    },
};

use smallvec::SmallVec;

type Waiter = (usize, WaiterType, Option<Waker>);

const UNLOCKED: usize = 0;
const WRITE_LOCKED: usize = usize::MAX;

#[derive(Debug, Clone, Copy, PartialEq)]
enum WaiterType {
    Read,
    Write,
}

/// Asynchronous, single-threaded Reader-Writer Lock.
///
/// **Thread Safety:** This lock is built on Cell and UnsafeCell and does not
/// use Rc. It automatically implements Send if T: Send, so it can be moved
/// across threads. However, its guards (RwLockReadGuard and RwLockWriteGuard)
/// are explicitly !Send.
pub struct RwLock<T: ?Sized> {
    state:   Cell<usize>,
    next_id: Cell<usize>,
    waiters: Cell<SmallVec<[Waiter; 8]>>,
    value:   UnsafeCell<T>,
}

impl<T> RwLock<T> {
    /// Creates a new instance.
    pub fn new(value: T) -> Self {
        Self {
            state:   Cell::new(UNLOCKED),
            next_id: Cell::new(0),
            waiters: Cell::new(SmallVec::new()),
            value:   UnsafeCell::new(value),
        }
    }
}

impl<T> RwLock<T> {
    /// Consumes the lock and returns its value without locking.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for RwLock<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Returns mutable access without locking, using the exclusive borrow.
    pub fn get_mut(&mut self) -> &mut T {
        self.value.get_mut()
    }

    /// Get a raw pointer to the underlying data.
    pub fn value_ptr(&self) -> *mut T {
        self.value.get()
    }

    /// Acquires the lock for reading asynchronously.
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        RwLockReadFuture {
            lock: self, id: None
        }
        .await
    }

    /// Acquires the lock for writing asynchronously.
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        RwLockWriteFuture {
            lock: self, id: None
        }
        .await
    }

    /// Attempts to acquire the lock for reading without blocking.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let s = self.state.get();
        let queue = self.waiters.take();
        let writer_is_waiting = queue.iter().any(|(_, kind, _)| *kind == WaiterType::Write);
        self.waiters.set(queue);
        if s != WRITE_LOCKED && !writer_is_waiting {
            assert!(s < WRITE_LOCKED - 1, "maximum number of readers exceeded");
            self.state.set(s + 1);
            Some(RwLockReadGuard {
                lock: self
            })
        } else {
            None
        }
    }

    /// Attempts to acquire the lock for writing without blocking.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        let queue = self.waiters.take();
        let has_waiters = !queue.is_empty();
        self.waiters.set(queue);
        if self.state.get() == UNLOCKED && !has_waiters {
            self.state.set(WRITE_LOCKED);
            Some(RwLockWriteGuard {
                lock: self
            })
        } else {
            None
        }
    }

    /// Wakes the next eligible tasks. If a writer is first, wakes it.
    /// If a reader is first, wakes ALL contiguous readers at the front.
    fn wake_next(&self) {
        let mut queue = self.waiters.take();
        let mut wakers = SmallVec::<[Waker; 8]>::new();
        match queue.first_mut() {
            | Some((_, WaiterType::Write, waker)) if self.state.get() == UNLOCKED => {
                if let Some(waker) = waker.take() {
                    wakers.push(waker);
                }
            },
            | Some((_, WaiterType::Read, _)) if self.state.get() != WRITE_LOCKED => {
                for (_, _, waker) in queue.iter_mut().take_while(|(_, kind, _)| *kind == WaiterType::Read) {
                    if let Some(waker) = waker.take() {
                        wakers.push(waker);
                    }
                }
            },
            | _ => {},
        }
        self.waiters.set(queue);
        for waker in wakers {
            waker.wake();
        }
    }
}

/// Future that resolves to a read guard when the lock is acquired for reading.
pub struct RwLockReadFuture<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    id:   Option<usize>,
}

impl<'a, T: ?Sized> Future for RwLockReadFuture<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Cloning may run user code. Observe lock state only after it returns.
        let waker = cx.waker().clone();
        let s = self.lock.state.get();

        let id = self.id.unwrap_or_else(|| {
            let new_id = self.lock.next_id.get();
            self.lock.next_id.set(new_id.wrapping_add(1));
            self.id = Some(new_id);
            new_id
        });

        let mut queue = self.lock.waiters.take();

        // Ensure no writers are ahead of us to prevent writer starvation
        let has_writer_ahead = queue
            .iter()
            .take_while(|(i, ..)| *i != id)
            .any(|(_, typ, _)| *typ == WaiterType::Write);

        if s != WRITE_LOCKED && !has_writer_ahead {
            assert!(s < WRITE_LOCKED - 1, "maximum number of readers exceeded");
            self.lock.state.set(s + 1);
            let removed = queue
                .iter()
                .position(|(i, ..)| *i == id)
                .map(|index| queue.remove(index));
            self.lock.waiters.set(queue);
            self.id = None;
            drop(removed);
            return Poll::Ready(RwLockReadGuard {
                lock: self.lock
            });
        }

        let old_waker = match queue.iter_mut().find(|(i, ..)| *i == id) {
            | Some(entry) => entry.2.replace(waker),
            | None => {
                queue.push((id, WaiterType::Read, Some(waker)));
                None
            },
        };

        self.lock.waiters.set(queue);
        drop(old_waker);
        Poll::Pending
    }
}

impl<'a, T: ?Sized> Drop for RwLockReadFuture<'a, T> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            let mut queue = self.lock.waiters.take();
            let was_first = queue.first().is_some_and(|(i, ..)| *i == id);
            let removed = queue
                .iter()
                .position(|(i, ..)| *i == id)
                .map(|index| queue.remove(index));
            self.lock.waiters.set(queue);
            drop(removed);

            // Pass the baton if we were blocking a wakeup chain
            if was_first {
                self.lock.wake_next();
            }
        }
    }
}

/// Future that resolves to a write guard when the lock is acquired for writing.
pub struct RwLockWriteFuture<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    id:   Option<usize>,
}

impl<'a, T: ?Sized> Future for RwLockWriteFuture<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Cloning may run user code. Observe lock state only after it returns.
        let waker = cx.waker().clone();
        let s = self.lock.state.get();

        let id = self.id.unwrap_or_else(|| {
            let new_id = self.lock.next_id.get();
            self.lock.next_id.set(new_id.wrapping_add(1));
            self.id = Some(new_id);
            new_id
        });

        let mut queue = self.lock.waiters.take();
        let is_first = queue.first().is_none_or(|(i, ..)| *i == id);

        if s == UNLOCKED && is_first {
            self.lock.state.set(WRITE_LOCKED);
            let removed = queue
                .iter()
                .position(|(i, ..)| *i == id)
                .map(|index| queue.remove(index));
            self.lock.waiters.set(queue);
            self.id = None;
            drop(removed);
            return Poll::Ready(RwLockWriteGuard {
                lock: self.lock
            });
        }

        let old_waker = match queue.iter_mut().find(|(i, ..)| *i == id) {
            | Some(entry) => entry.2.replace(waker),
            | None => {
                queue.push((id, WaiterType::Write, Some(waker)));
                None
            },
        };

        self.lock.waiters.set(queue);
        drop(old_waker);
        Poll::Pending
    }
}

impl<'a, T: ?Sized> Drop for RwLockWriteFuture<'a, T> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            let mut queue = self.lock.waiters.take();
            let was_first = queue.first().is_some_and(|(i, ..)| *i == id);
            let removed = queue
                .iter()
                .position(|(i, ..)| *i == id)
                .map(|index| queue.remove(index));
            self.lock.waiters.set(queue);
            drop(removed);

            if was_first {
                self.lock.wake_next();
            }
        }
    }
}

/// An RAII guard that provides shared read access to the protected data.
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

impl<'a, T: ?Sized> Deref for RwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: a read guard proves that no writer exists, and the lock is
        // single-threaded so concurrent access cannot occur on another thread.
        unsafe { &*self.lock.value.get() }
    }
}

impl<'a, T: ?Sized> Drop for RwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        let s = self.lock.state.get();
        self.lock.state.set(s - 1);

        if self.lock.state.get() == UNLOCKED {
            self.lock.wake_next();
        }
    }
}

/// An RAII guard that provides exclusive write access to the protected data.
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

impl<'a, T: ?Sized> Deref for RwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: the write guard has exclusive access to the protected value.
        unsafe { &*self.lock.value.get() }
    }
}

impl<'a, T: ?Sized> DerefMut for RwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: the write guard has exclusive access to the protected value.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<'a, T: ?Sized> Drop for RwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.state.set(UNLOCKED);
        self.lock.wake_next();
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn async_rwlock() {
        let local = task::LocalSet::new();
        local
            .run_until(async move {
                let rwlock = Rc::new(RwLock::new(0));
                let writer = Rc::clone(&rwlock);
                let task = task::spawn_local(async move {
                    let mut guard = writer.write().await;
                    *guard = 42;
                });
                assert!(task.await.is_ok());

                let guard = rwlock.read().await;
                assert_eq!(*guard, 42);
            })
            .await;
    }

    #[test]
    fn canceling_writer_wakes_readers_while_a_reader_is_held() {
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
        let lock = RwLock::new(42);
        let held = lock.try_read().unwrap();
        let mut writer = Box::pin(lock.write());
        let mut reader = Box::pin(lock.read());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(writer.as_mut().poll(&mut cx).is_pending());
        let counter = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut reader_cx = Context::from_waker(&waker);
        assert!(reader.as_mut().poll(&mut reader_cx).is_pending());
        drop(writer);
        assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        assert!(reader.as_mut().poll(&mut reader_cx).is_ready());
        assert_eq!(*held, 42);
    }

    #[test]
    fn try_write_respects_queued_writer() {
        let lock = RwLock::new(0);
        let held = lock.try_write().unwrap();
        let mut queued = Box::pin(lock.write());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(queued.as_mut().poll(&mut cx).is_pending());
        drop(held);
        assert!(lock.try_write().is_none());
        assert!(queued.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn exclusive_access_supports_unsized_values() {
        let mut lock = RwLock::from([1, 2, 3]);
        let unsized_lock: &mut RwLock<[i32]> = &mut lock;
        unsized_lock.get_mut()[1] = 20;
        assert_eq!(lock.into_inner(), [1, 20, 3]);
    }

    #[test]
    fn cancellation_preserves_waiters_registered_by_waker_drop() {
        use std::{
            cell::RefCell,
            sync::Arc,
            task::Wake,
        };
        thread_local! {
            static ON_DROP: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
        }
        struct Reenter;
        impl Wake for Reenter {
            fn wake(self: Arc<Self>) {
                drop(self);
            }
        }
        impl Drop for Reenter {
            fn drop(&mut self) {
                let callback = ON_DROP.with(|slot| slot.borrow_mut().take());
                if let Some(callback) = callback {
                    callback();
                }
            }
        }
        let lock = Rc::new(RwLock::new(0));
        let held = lock.try_write().unwrap();
        let waker = Waker::from(Arc::new(Reenter));
        let mut canceled = Box::pin(lock.write());
        assert!(canceled.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        drop(waker);
        let mut second = Box::pin(lock.write());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        type Waiter = Pin<Box<dyn Future<Output = ()>>>;
        let reentrant = Rc::new(RefCell::new(None::<Waiter>));
        ON_DROP.with(|slot| {
            let lock = Rc::clone(&lock);
            let reentrant = Rc::clone(&reentrant);
            *slot.borrow_mut() = Some(Box::new(move || {
                let mut future: Waiter = Box::pin(async move {
                    drop(lock.write().await);
                });
                assert!(
                    future
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                *reentrant.borrow_mut() = Some(future);
            }));
        });
        drop(canceled);
        let queue = lock.waiters.take();
        assert_eq!(queue.len(), 2);
        lock.waiters.set(queue);
        drop(held);
        assert!(second.as_mut().poll(&mut cx).is_ready());
        assert!(
            reentrant
                .borrow_mut()
                .as_mut()
                .unwrap()
                .as_mut()
                .poll(&mut cx)
                .is_ready()
        );
    }
}
