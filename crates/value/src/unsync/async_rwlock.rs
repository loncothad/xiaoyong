//! A single-threaded, asynchronous reader-writer lock.

use std::{
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
    waiters: Cell<SmallVec<[(usize, WaiterType, Waker); 8]>>,
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

impl<T: ?Sized> RwLock<T> {
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
        if s != WRITE_LOCKED {
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
        if self.state.get() == UNLOCKED {
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
        let queue = self.waiters.take();

        match queue.first() {
            | Some((_, WaiterType::Write, waker)) => {
                waker.wake_by_ref();
            },
            | _ => {
                for (_, typ, waker) in queue.iter() {
                    if *typ == WaiterType::Read {
                        waker.wake_by_ref();
                    } else {
                        break;
                    }
                }
            },
        }

        self.waiters.set(queue);
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
            self.lock.state.set(s + 1);
            queue.retain(|(i, ..)| *i != id);
            self.lock.waiters.set(queue);
            self.id = None;
            return Poll::Ready(RwLockReadGuard {
                lock: self.lock
            });
        }

        match queue.iter_mut().find(|(i, ..)| *i == id) {
            | Some(entry) => {
                if !entry.2.will_wake(cx.waker()) {
                    entry.2 = cx.waker().clone();
                }
            },
            | None => {
                queue.push((id, WaiterType::Read, cx.waker().clone()));
            },
        }

        self.lock.waiters.set(queue);
        Poll::Pending
    }
}

impl<'a, T: ?Sized> Drop for RwLockReadFuture<'a, T> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            let mut queue = self.lock.waiters.take();
            let was_first = queue.first().map_or(false, |(i, ..)| *i == id);
            queue.retain(|(i, ..)| *i != id);
            self.lock.waiters.set(queue);

            // Pass the baton if we were blocking a wakeup chain
            if was_first && self.lock.state.get() == UNLOCKED {
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
        let s = self.lock.state.get();

        let id = self.id.unwrap_or_else(|| {
            let new_id = self.lock.next_id.get();
            self.lock.next_id.set(new_id.wrapping_add(1));
            self.id = Some(new_id);
            new_id
        });

        let mut queue = self.lock.waiters.take();
        let is_first = queue.first().map_or(true, |(i, ..)| *i == id);

        if s == UNLOCKED && is_first {
            self.lock.state.set(WRITE_LOCKED);
            queue.retain(|(i, ..)| *i != id);
            self.lock.waiters.set(queue);
            self.id = None;
            return Poll::Ready(RwLockWriteGuard {
                lock: self.lock
            });
        }

        match queue.iter_mut().find(|(i, ..)| *i == id) {
            | Some(entry) => {
                if !entry.2.will_wake(cx.waker()) {
                    entry.2 = cx.waker().clone();
                }
            },
            | None => {
                queue.push((id, WaiterType::Write, cx.waker().clone()));
            },
        }

        self.lock.waiters.set(queue);
        Poll::Pending
    }
}

impl<'a, T: ?Sized> Drop for RwLockWriteFuture<'a, T> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            let mut queue = self.lock.waiters.take();
            let was_first = queue.first().map_or(false, |(i, ..)| *i == id);
            queue.retain(|(i, ..)| *i != id);
            self.lock.waiters.set(queue);

            if was_first && self.lock.state.get() == UNLOCKED {
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
        unsafe { &*self.lock.value.get() }
    }
}

impl<'a, T: ?Sized> DerefMut for RwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
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

    use super::*;

    #[tokio::test]
    async fn async_rwlock() {
        let rwlock = Rc::new(RwLock::new(0));

        let r1 = Rc::clone(&rwlock);
        tokio::task::spawn_local(async move {
            let mut guard = r1.write().await;
            *guard = 42;
        })
        .await
        .unwrap();

        let guard = rwlock.read().await;
        assert_eq!(*guard, 42);
    }
}
