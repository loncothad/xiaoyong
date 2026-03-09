//! Single-threaded, bounded, multi-producer single-consumer (MPSC) channel.

use std::{
    cell::Cell,
    // collections::VecDeque,
    future::poll_fn,
    rc::Rc,
    task::{
        Poll,
        Waker,
    },
};

use smallvec::SmallVec;

/// Error returned when attempting to push into a full or closed channel.
#[derive(Debug, PartialEq, Eq)]
pub enum TryPushError<T> {
    Full(T),
    Closed(T),
}

/// Error returned when pushing into a closed channel.
#[derive(Debug, PartialEq, Eq)]
pub struct PushError<T>(pub T);

/// Error returned when attempting to push multiple items into a channel that
/// becomes full or is closed.
#[derive(Debug, PartialEq, Eq)]
pub enum TryPushManyError<T> {
    Full(Vec<T>),
    Closed(Vec<T>),
}

/// Error returned when pushing multiple items into a closed channel.
#[derive(Debug, PartialEq, Eq)]
pub struct PushManyError<T>(pub Vec<T>);

/// Error returned when attempting to pop from an empty or closed channel.
#[derive(Debug, PartialEq, Eq)]
pub enum TryPopError {
    Empty,
    Closed,
}

struct Shared<T> {
    queue:        Cell<Option<SmallVec<[T; 32]>>>,
    capacity:     usize,
    rx_waker:     Cell<Option<Waker>>,
    tx_wakers:    Cell<Vec<Waker>>,
    sender_count: Cell<usize>,
    closed:       Cell<bool>,
}

/// Single-threaded sender for the MPSC channel.
///
/// **Thread Safety:** This type uses Rc internally and is explicitly !Send.
/// It is designed for use within single-threaded async environments.
pub struct Sender<T> {
    shared: Rc<Shared<T>>,
}

/// Single-threaded receiver for the MPSC channel.
///
/// **Thread Safety:** This type uses Rc internally and is explicitly !Send.
pub struct Receiver<T> {
    shared: Rc<Shared<T>>,
}

/// Create a new bounded channel, returning the sender and receiver halves.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "Capacity must be strictly greater than 0");
    let shared = Rc::new(Shared {
        queue: Cell::new(Some(SmallVec::with_capacity(capacity))),
        capacity,
        rx_waker: Cell::new(None),
        tx_wakers: Cell::new(Vec::new()),
        sender_count: Cell::new(1),
        closed: Cell::new(false),
    });

    (
        Sender::<T> {
            shared: shared.clone()
        },
        Receiver {
            shared,
        },
    )
}

impl<T> Sender<T> {
    /// Try to push a value into the channel without blocking.
    pub fn try_push(&self, value: T) -> Result<(), TryPushError<T>> {
        if self.shared.closed.get() {
            return Err(TryPushError::Closed(value));
        }

        let mut q: SmallVec<[T; 32]> = match self.shared.queue.take() {
            | Some(q) => q,
            | None => return Err(TryPushError::Closed(value)),
        };

        if q.len() == self.shared.capacity {
            self.shared.queue.set(Some(q));
            return Err(TryPushError::Full(value));
        }

        q.push(value);
        self.shared.queue.set(Some(q));

        if let Some(waker) = self.shared.rx_waker.take() {
            waker.wake();
        }

        Ok(())
    }

    /// Push a value into the channel.
    pub async fn push(&self, value: T) -> Result<(), PushError<T>> {
        let mut val_opt = Some(value);
        poll_fn(|cx| {
            let v = val_opt.take().unwrap();
            match self.try_push(v) {
                | Ok(()) => Poll::Ready(Ok(())),
                | Err(TryPushError::Closed(v)) => Poll::Ready(Err(PushError(v))),
                | Err(TryPushError::Full(v)) => {
                    val_opt = Some(v); // Put the value back for the next poll

                    // Register the sender's waker to be notified on the next pop
                    let mut wakers = self.shared.tx_wakers.take();
                    wakers.push(cx.waker().clone());
                    self.shared.tx_wakers.set(wakers);

                    Poll::Pending
                },
            }
        })
        .await
    }

    /// Try to push multiple values into the channel without blocking.
    pub fn try_push_many<I>(&self, values: I) -> Result<(), TryPushManyError<T>>
    where
        I: IntoIterator<Item = T>,
    {
        let mut iter = values.into_iter();

        if self.shared.closed.get() {
            return Err(TryPushManyError::Closed(iter.collect()));
        }

        let mut q: SmallVec<[T; 32]> = match self.shared.queue.take() {
            | Some(q) => q,
            | None => return Err(TryPushManyError::Closed(iter.collect())),
        };

        let mut pushed_any = false;

        // Fill exact remaining capacity
        while q.len() < self.shared.capacity {
            if let Some(val) = iter.next() {
                q.push(val);
                pushed_any = true;
            } else {
                break;
            }
        }

        self.shared.queue.set(Some(q));

        if pushed_any {
            if let Some(waker) = self.shared.rx_waker.take() {
                waker.wake();
            }
        }

        let remainder: Vec<T> = iter.collect();
        if remainder.is_empty() {
            Ok(())
        } else {
            Err(TryPushManyError::Full(remainder))
        }
    }

    /// Push multiple values into the channel.
    pub async fn push_many<I>(&self, values: I) -> Result<(), PushManyError<T>>
    where
        I: IntoIterator<Item = T>,
    {
        let mut remainder = values.into_iter().collect::<Vec<T>>();
        if remainder.is_empty() {
            return Ok(());
        }

        poll_fn(|cx| {
            match self.try_push_many(std::mem::take(&mut remainder)) {
                | Ok(()) => Poll::Ready(Ok(())),
                | Err(TryPushManyError::Closed(rem)) => Poll::Ready(Err(PushManyError(rem))),
                | Err(TryPushManyError::Full(rem)) => {
                    remainder = rem;
                    let mut wakers = self.shared.tx_wakers.take();
                    wakers.push(cx.waker().clone());
                    self.shared.tx_wakers.set(wakers);
                    Poll::Pending
                },
            }
        })
        .await
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.sender_count.set(self.shared.sender_count.get() + 1);
        Sender::<T> {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let count = self.shared.sender_count.get() - 1;
        self.shared.sender_count.set(count);

        if count == 0 {
            if let Some(waker) = self.shared.rx_waker.take() {
                waker.wake();
            }
        }
    }
}

impl<T> Receiver<T> {
    /// Try to pop a value from the channel without blocking.
    pub fn try_pop(&mut self) -> Result<T, TryPopError> {
        let mut q: SmallVec<[T; 32]> = self.shared.queue.take().unwrap();

        if let Some(value) = (!q.is_empty()).then(|| q.remove(0)) {
            self.shared.queue.set(Some(q));

            let mut wakers = self.shared.tx_wakers.take();
            if !wakers.is_empty() {
                for waker in wakers.drain(..) {
                    waker.wake();
                }
            }
            self.shared.tx_wakers.set(wakers);

            return Ok(value);
        }

        self.shared.queue.set(Some(q));

        match self.shared.sender_count.get() {
            | 0 => Err(TryPopError::Closed),
            | _ => Err(TryPopError::Empty),
        }
    }

    /// Pop a value from the channel.
    pub async fn pop(&mut self) -> Option<T> {
        poll_fn(|cx| {
            match self.try_pop() {
                | Ok(value) => Poll::Ready(Some(value)),
                | Err(TryPopError::Closed) => Poll::Ready(None),
                | Err(TryPopError::Empty) => {
                    self.shared.rx_waker.set(Some(cx.waker().clone()));
                    Poll::Pending
                },
            }
        })
        .await
    }

    /// Try to pop multiple values from the channel without blocking.
    pub fn try_pop_many(&mut self, limit: usize) -> Result<Vec<T>, TryPopError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut q: SmallVec<[T; 32]> = self.shared.queue.take().unwrap();

        if q.is_empty() {
            self.shared.queue.set(Some(q));
            return if self.shared.sender_count.get() == 0 {
                Err(TryPopError::Closed)
            } else {
                Err(TryPopError::Empty)
            };
        }

        let count = limit.min(q.len());
        let mut results = Vec::with_capacity(count);
        for _ in 0 .. count {
            results.push((!q.is_empty()).then(|| q.remove(0)).unwrap());
        }

        self.shared.queue.set(Some(q));

        // Space freed up, wake blocked senders
        let mut wakers = self.shared.tx_wakers.take();
        if !wakers.is_empty() {
            for waker in wakers.drain(..) {
                waker.wake();
            }
        }
        self.shared.tx_wakers.set(wakers);

        Ok(results)
    }

    /// Pop multiple values from the channel.
    pub async fn pop_many(&mut self, limit: usize) -> Vec<T> {
        if limit == 0 {
            return Vec::new();
        }

        poll_fn(|cx| {
            match self.try_pop_many(limit) {
                | Ok(values) => Poll::Ready(values),
                | Err(TryPopError::Closed) => Poll::Ready(Vec::new()),
                | Err(TryPopError::Empty) => {
                    self.shared.rx_waker.set(Some(cx.waker().clone()));
                    Poll::Pending
                },
            }
        })
        .await
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.closed.set(true);
        self.shared.queue.take();

        // Wake all blocked senders so they can gracefully exit via PushError
        let mut wakers = self.shared.tx_wakers.take();
        for waker in wakers.drain(..) {
            waker.wake();
        }
        self.shared.tx_wakers.set(wakers);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn capacity_bound() {
        let (tx, mut rx) = channel::<i32>(2);

        tx.try_push(1).unwrap();
        tx.try_push(2).unwrap();

        // Ensure capacity blocks the 3rd push
        assert_eq!(tx.try_push(3), Err(TryPushError::Full(3)));

        assert_eq!(rx.pop().await, Some(1));

        // Space freed up, push should succeed now
        tx.try_push(3).unwrap();
        assert_eq!(rx.pop().await, Some(2));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_push_backpressure() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let (tx, mut rx) = channel::<i32>(1);
                tx.push(1).await.unwrap();

                let tx_clone = tx.clone();

                // This task will block because capacity is 1
                tokio::task::spawn_local(async move {
                    tx_clone.push(2).await.unwrap();
                });

                // Yield control so the spawned task reaches the `.await` and suspends
                tokio::task::yield_now().await;

                // First pop returns 1, making room and waking the tx_clone task
                assert_eq!(rx.pop().await, Some(1));

                // Second pop returns 2, successfully resolving the blocked push
                assert_eq!(rx.pop().await, Some(2));
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn try_push_many_capacity() {
        let (tx, mut rx) = channel::<i32>(4);

        // Should push 1, 2, 3, 4 and return 5, 6 as Full error
        let err = tx.try_push_many(vec![1, 2, 3, 4, 5, 6]).unwrap_err();
        assert_eq!(err, TryPushManyError::Full(vec![5, 6]));

        let vals = rx.try_pop_many(10).unwrap();
        assert_eq!(vals, vec![1, 2, 3, 4]);
    }
}
