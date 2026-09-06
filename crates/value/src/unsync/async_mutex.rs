//! Single-threaded, asynchronous mutual exclusion lock.

use core::{
    cell::{
        Cell,
        UnsafeCell,
    },
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

type Waiter = (usize, Option<Waker>);

/// Asynchronous, single-threaded Mutex.
///
/// **Thread Safety:** This Mutex is built on Cell and UnsafeCell and does not
/// use Rc. It automatically implements Send if T: Send, so it can be moved
/// across threads. However, its guard (MutexGuard) is explicitly !Send.
pub struct Mutex<T: ?Sized> {
    is_locked: Cell<bool>,
    next_id:   Cell<usize>,
    waiters:   Cell<SmallVec<[Waiter; 8]>>,
    value:     UnsafeCell<T>,
}

impl<T> Mutex<T> {
    /// Creates a new instance.
    pub fn new(value: T) -> Self {
        Self {
            is_locked: Cell::new(false),
            next_id:   Cell::new(0),
            waiters:   Cell::new(SmallVec::new()),
            value:     UnsafeCell::new(value),
        }
    }
}

impl<T> Mutex<T> {
    /// Consumes the lock and returns its value without locking.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Returns mutable access without locking, using the exclusive borrow.
    pub fn get_mut(&mut self) -> &mut T {
        self.value.get_mut()
    }

    /// Get a raw pointer to the underlying data.
    pub fn value_ptr(&self) -> *mut T {
        self.value.get()
    }

    /// Acquire the lock.
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        LockFuture {
            mutex: self,
            id:    None,
        }
        .await
    }

    /// Try to acquire the lock without blocking.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        let queue = self.waiters.take();
        let has_waiters = !queue.is_empty();
        self.waiters.set(queue);
        if !self.is_locked.get() && !has_waiters {
            self.is_locked.set(true);
            Some(MutexGuard {
                mutex: self
            })
        } else {
            None
        }
    }
}

/// Future that resolves to a mutex guard when the lock is acquired.
pub struct LockFuture<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
    id:    Option<usize>,
}

impl<'a, T: ?Sized> Future for LockFuture<'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Cloning may run user code. Observe lock state only after it returns.
        let waker = cx.waker().clone();
        let mut queue = self.mutex.waiters.take();
        let is_next = match self.id {
            | Some(id) => queue.first().is_some_and(|(waiter_id, _)| *waiter_id == id),
            | None => queue.is_empty(),
        };

        if !self.mutex.is_locked.get() && is_next {
            self.mutex.is_locked.set(true);

            // Success: Clean up our queue entry if we were previously pending
            let removed = if let Some(id) = self.id {
                let removed = queue
                    .iter()
                    .position(|(w_id, _)| *w_id == id)
                    .map(|index| queue.remove(index));

                // Disable the Drop handler since we successfully acquired the lock
                self.id = None;
                removed
            } else {
                None
            };
            self.mutex.waiters.set(queue);
            drop(removed);

            Poll::Ready(MutexGuard {
                mutex: self.mutex
            })
        } else {
            // Assign a unique ID on the first Pending poll
            let id = self.id.unwrap_or_else(|| {
                let new_id = self.mutex.next_id.get();
                self.mutex.next_id.set(new_id.wrapping_add(1));
                self.id = Some(new_id);
                new_id
            });

            // Update the waker if we're already in the queue, else push
            let old_waker = match queue.iter_mut().find(|(i, _)| *i == id) {
                | Some(entry) => entry.1.replace(waker),
                | None => {
                    queue.push((id, Some(waker)));
                    None
                },
            };

            self.mutex.waiters.set(queue);
            drop(old_waker);
            Poll::Pending
        }
    }
}

impl<'a, T: ?Sized> Drop for LockFuture<'a, T> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            let mut queue = self.mutex.waiters.take();

            // Remove this specific future from the wait queue
            let removed = queue
                .iter()
                .position(|(w_id, _)| *w_id == id)
                .map(|index| queue.remove(index));

            let next_waker = if !self.mutex.is_locked.get() {
                queue.first_mut().and_then(|(_, waker)| waker.take())
            } else {
                None
            };
            self.mutex.waiters.set(queue);
            drop(removed);
            if let Some(waker) = next_waker {
                waker.wake();
            }
        }
    }
}

/// An RAII guard that provides mutable access to the protected data.
pub struct MutexGuard<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
}

impl<'a, T: ?Sized> Deref for MutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: the guard is only created after acquiring exclusive access.
        unsafe { &*self.mutex.value.get() }
    }
}

impl<'a, T: ?Sized> DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: the guard is only created after acquiring exclusive access.
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<'a, T: ?Sized> Drop for MutexGuard<'a, T> {
    fn drop(&mut self) {
        self.mutex.is_locked.set(false);

        let mut queue = self.mutex.waiters.take();
        let next_waker = queue.first_mut().and_then(|(_, waker)| waker.take());
        self.mutex.waiters.set(queue);

        if let Some(waker) = next_waker {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn async_mutex() {
        let local = task::LocalSet::new();
        local
            .run_until(async move {
                let mutex = Rc::new(Mutex::new(0));

                let m1 = Rc::clone(&mutex);
                task::spawn_local(async move {
                    let mut guard = m1.lock().await;
                    *guard += 1;
                })
                .await
                .unwrap();

                let guard = mutex.lock().await;
                assert_eq!(*guard, 1);
            })
            .await;
    }

    #[test]
    fn canceling_front_waiter_preserves_fifo_progress() {
        let lock = Mutex::from(42);
        let held = lock.try_lock().unwrap();
        let mut first = Box::pin(lock.lock());
        let mut second = Box::pin(lock.lock());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        drop(held);
        assert!(lock.try_lock().is_none());
        drop(first);
        let second_guard = match second.as_mut().poll(&mut cx) {
            | Poll::Ready(guard) => guard,
            | _ => panic!("next waiter should acquire"),
        };
        assert_eq!(*second_guard, 42);
    }

    #[test]
    fn exclusive_access_supports_unsized_values() {
        let mut lock = Mutex::from([1, 2, 3]);
        let unsized_lock: &mut Mutex<[i32]> = &mut lock;
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
        let lock = Rc::new(Mutex::new(0));
        let held = lock.try_lock().unwrap();
        let waker = Waker::from(Arc::new(Reenter));
        let mut canceled = Box::pin(lock.lock());
        assert!(canceled.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        drop(waker);
        let mut second = Box::pin(lock.lock());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        type Waiter = Pin<Box<dyn Future<Output = ()>>>;
        let reentrant = Rc::new(RefCell::new(None::<Waiter>));
        ON_DROP.with(|slot| {
            let lock = Rc::clone(&lock);
            let reentrant = Rc::clone(&reentrant);
            *slot.borrow_mut() = Some(Box::new(move || {
                let mut future: Waiter = Box::pin(async move {
                    drop(lock.lock().await);
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
