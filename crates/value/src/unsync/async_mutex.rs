//! A single-threaded, asynchronous mutual exclusion lock.

use std::{
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

/// An asynchronous, single-threaded Mutex.
///
/// **Thread Safety:** This Mutex is built on Cell and UnsafeCell and does not
/// use Rc. It automatically implements Send if T: Send, so it can be moved
/// across threads. However, its guard (MutexGuard) is explicitly !Send.
pub struct Mutex<T: ?Sized> {
    is_locked: Cell<bool>,
    next_id:   Cell<usize>,
    waiters:   Cell<SmallVec<[(usize, Waker); 8]>>,
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

impl<T: ?Sized> Mutex<T> {
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
        if !self.is_locked.get() {
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
        if !self.mutex.is_locked.get() {
            self.mutex.is_locked.set(true);

            // Success: Clean up our queue entry if we were previously pending
            if let Some(id) = self.id {
                let mut queue = self.mutex.waiters.take();
                queue.retain(|(w_id, _)| *w_id != id);
                self.mutex.waiters.set(queue);

                // Disable the Drop handler since we successfully acquired the lock
                self.id = None;
            }

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

            let mut queue = self.mutex.waiters.take();

            // Update the waker if we're already in the queue, else push
            match queue.iter_mut().find(|(i, _)| *i == id) {
                | Some(entry) => {
                    if !entry.1.will_wake(cx.waker()) {
                        entry.1 = cx.waker().clone();
                    }
                },
                | None => {
                    queue.push((id, cx.waker().clone()));
                },
            }

            self.mutex.waiters.set(queue);
            Poll::Pending
        }
    }
}

impl<'a, T: ?Sized> Drop for LockFuture<'a, T> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            let mut queue = self.mutex.waiters.take();

            // Remove this specific future from the wait queue
            queue.retain(|(w_id, _)| *w_id != id);

            // Prevent Lost Wakeups
            if !self.mutex.is_locked.get() {
                if let Some((_, next_waker)) = queue.first() {
                    next_waker.wake_by_ref();
                }
            }

            self.mutex.waiters.set(queue);
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
        unsafe { &*self.mutex.value.get() }
    }
}

impl<'a, T: ?Sized> DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<'a, T: ?Sized> Drop for MutexGuard<'a, T> {
    fn drop(&mut self) {
        self.mutex.is_locked.set(false);

        let queue = self.mutex.waiters.take();
        let next_waker = queue.first().map(|(_, waker)| waker.clone());
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
                    ()
                })
                .await
                .unwrap();

                let guard = mutex.lock().await;
                assert_eq!(*guard, 1);
            })
            .await;
    }
}
