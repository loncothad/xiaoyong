//! Thread-safe, bounded, single-producer single-consumer (SPSC) channel.

use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    future::poll_fn,
    mem::MaybeUninit,
    sync::atomic::{
        AtomicBool,
        AtomicUsize,
        Ordering,
    },
    task::Poll,
};

use crossbeam_utils::CachePadded;
use futures::task::AtomicWaker;
use triomphe::Arc;

/// Internal shared state of the SPSC queue.
struct Shared<T> {
    buffer:         Box<[UnsafeCell<MaybeUninit<T>>]>,
    capacity:       usize,
    // Written by Producer, read by Consumer
    head:           CachePadded<AtomicUsize>,
    // Written by Consumer, read by Producer
    tail:           CachePadded<AtomicUsize>,
    producer_waker: AtomicWaker,
    consumer_waker: AtomicWaker,
    producer_alive: AtomicBool,
    consumer_alive: AtomicBool,
}

// SAFETY: The queue is safe to send and share across threads if the underlying
// type is Send.
unsafe impl<T: Send> Send for Shared<T> {}
// SAFETY: the unique producer and consumer access disjoint logical slots, and
// publish progress using Acquire/Release atomics.
unsafe impl<T: Send> Sync for Shared<T> {}

impl<T> Shared<T> {
    // Use a period divisible by capacity so arbitrary capacities remain valid
    // at counter rollover. The second lap distinguishes full from empty.
    fn advance(&self, cursor: usize, amount: usize) -> usize {
        let until_wrap = self.capacity * 2 - cursor;
        if amount >= until_wrap {
            amount - until_wrap
        } else {
            cursor + amount
        }
    }

    fn distance(&self, head: usize, tail: usize) -> usize {
        if head >= tail {
            head - tail
        } else {
            head + (self.capacity * 2 - tail)
        }
    }
}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        let mut current = tail;

        // Clean up unconsumed elements to prevent memory leaks
        while current != head {
            let index = current % self.capacity;
            // SAFETY: slots in `[tail, head)` are initialized and unconsumed;
            // `Shared` has exclusive access during destruction.
            unsafe {
                self.buffer[index].get().read().assume_init_drop();
            }
            current = self.advance(current, 1);
        }
    }
}

/// The transmitting half of the SPSC channel.
///
/// Operations require exclusive access, enforcing one active producer.
///
/// ```compile_fail
/// let (producer, _) = xiaoyong_channels::sync::async_spsc_bounded::channel(1);
/// let shared = &producer;
/// shared.try_push(1).unwrap();
/// ```
pub struct Producer<T>(Arc<Shared<T>>);
/// The receiving half of the SPSC channel.
///
/// Operations require exclusive access, enforcing one active consumer.
///
/// ```compile_fail
/// let (_, consumer) = xiaoyong_channels::sync::async_spsc_bounded::channel::<i32>(1);
/// let shared = &consumer;
/// shared.try_pop();
/// ```
pub struct Consumer<T>(Arc<Shared<T>>);

/// Creates a new lock-free SPSC channel with a fixed capacity.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(capacity > 0, "Capacity must be greater than 0");
    assert!(capacity <= usize::MAX / 2, "Capacity is too large");

    let mut buffer = Vec::with_capacity(capacity);
    for _ in 0 .. capacity {
        buffer.push(UnsafeCell::new(MaybeUninit::uninit()));
    }

    let shared = Arc::new(Shared {
        buffer: buffer.into_boxed_slice(),
        capacity,
        head: CachePadded::new(AtomicUsize::new(0)),
        tail: CachePadded::new(AtomicUsize::new(0)),
        producer_waker: AtomicWaker::new(),
        consumer_waker: AtomicWaker::new(),
        producer_alive: AtomicBool::new(true),
        consumer_alive: AtomicBool::new(true),
    });

    (Producer(shared.clone()), Consumer(shared))
}

impl<T> Producer<T> {
    /// Returns `true` if the consumer has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        !self.0.consumer_alive.load(Ordering::Acquire)
    }

    /// Try to push a single item.
    pub fn try_push(&mut self, item: T) -> Result<(), T> {
        if !self.0.consumer_alive.load(Ordering::Acquire) {
            return Err(item);
        }
        let head = self.0.head.load(Ordering::Relaxed);
        let tail = self.0.tail.load(Ordering::Acquire);

        if self.0.distance(head, tail) >= self.0.capacity {
            return Err(item);
        }

        let index = head % self.0.capacity;
        // SAFETY: the unique producer owns the free slot at `head` until it
        // publishes the increment with a Release store.
        unsafe {
            (*self.0.buffer[index].get()).write(item);
        }

        self.0.head.store(self.0.advance(head, 1), Ordering::Release);
        self.0.consumer_waker.wake();
        Ok(())
    }

    /// Try to push as many items as possible from an
    /// iterator. Returns the number of items successfully pushed.
    pub fn try_push_many<I: Iterator<Item = T>>(&mut self, items: &mut I) -> usize {
        if !self.0.consumer_alive.load(Ordering::Acquire) {
            return 0;
        }
        let head = self.0.head.load(Ordering::Relaxed);
        let tail = self.0.tail.load(Ordering::Acquire);

        let available = self.0.capacity - self.0.distance(head, tail);
        if available == 0 {
            return 0;
        }

        let mut pushed = 0;
        for _ in 0 .. available {
            match items.next() {
                | Some(item) => {
                    let index = self.0.advance(head, pushed) % self.0.capacity;
                    // SAFETY: each offset below `available` is an exclusively
                    // producer-owned free slot.
                    unsafe {
                        (*self.0.buffer[index].get()).write(item);
                    }
                    pushed += 1;
                    self.0.head.store(self.0.advance(head, pushed), Ordering::Release);
                    self.0.consumer_waker.wake();
                },
                | None => break,
            }
        }

        pushed
    }

    /// Pushes one item, suspending while the queue is full.
    ///
    /// Returns the item if the consumer has been dropped.
    pub async fn push(&mut self, item: T) -> Result<(), T> {
        let mut item = Some(item);
        poll_fn(move |cx| {
            self.0.producer_waker.register(cx.waker());
            let value = item.take().expect("push future polled after completion");
            match self.try_push(value) {
                | Ok(()) => Poll::Ready(Ok(())),
                | Err(value) if self.is_closed() => Poll::Ready(Err(value)),
                | Err(value) => {
                    item = Some(value);
                    Poll::Pending
                },
            }
        })
        .await
    }

    /// Pushes all items, suspending while the queue is full.
    ///
    /// Returns the unpushed suffix if the consumer is dropped.
    pub async fn push_many<I: IntoIterator<Item = T>>(&mut self, items: I) -> Result<(), Vec<T>> {
        let mut remaining = items.into_iter().collect::<VecDeque<_>>();
        poll_fn(move |cx| {
            self.0.producer_waker.register(cx.waker());
            while let Some(item) = remaining.pop_front() {
                if let Err(item) = self.try_push(item) {
                    remaining.push_front(item);
                    break;
                }
            }
            if remaining.is_empty() {
                Poll::Ready(Ok(()))
            } else if self.is_closed() {
                Poll::Ready(Err(remaining.drain(..).collect()))
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

impl<T> Consumer<T> {
    /// Try to pop a single item.
    pub fn try_pop(&mut self) -> Option<T> {
        let tail = self.0.tail.load(Ordering::Relaxed);
        let head = self.0.head.load(Ordering::Acquire);

        if head == tail {
            return None;
        }

        let index = tail % self.0.capacity;
        // SAFETY: the Acquire load of `head` proves this slot was initialized,
        // and the unique consumer owns it until advancing `tail`.
        let item = unsafe { self.0.buffer[index].get().read().assume_init() };

        self.0.tail.store(self.0.advance(tail, 1), Ordering::Release);
        self.0.producer_waker.wake();
        Some(item)
    }

    /// Try to pop up to `limit` items.
    pub fn try_pop_many(&mut self, limit: usize) -> Vec<T> {
        let tail = self.0.tail.load(Ordering::Relaxed);
        let head = self.0.head.load(Ordering::Acquire);

        let available = self.0.distance(head, tail);
        let to_pop = available.min(limit);

        if to_pop == 0 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(to_pop);
        for i in 0 .. to_pop {
            let index = (self.0.advance(tail, i)) % self.0.capacity;
            // SAFETY: every offset below `to_pop` was published by the producer
            // and is exclusively owned by this consumer.
            let item = unsafe { self.0.buffer[index].get().read().assume_init() };
            result.push(item);
        }

        self.0.tail.store(self.0.advance(tail, to_pop), Ordering::Release);
        self.0.producer_waker.wake();
        result
    }

    /// Pops one item, suspending while the queue is empty.
    ///
    /// Returns `None` after the producer is dropped and buffered items drain.
    pub async fn pop(&mut self) -> Option<T> {
        poll_fn(|cx| {
            self.0.consumer_waker.register(cx.waker());
            if let Some(item) = self.try_pop() {
                return Poll::Ready(Some(item));
            }
            if self.is_closed() {
                // The close Acquire synchronizes with the final publication.
                return Poll::Ready(self.try_pop());
            }
            Poll::Pending
        })
        .await
    }

    /// Pop up to `limit` items, suspending until at least 1
    /// item is available.
    pub async fn pop_many(&mut self, limit: usize) -> Vec<T> {
        if limit == 0 {
            return Vec::new();
        }
        poll_fn(|cx| {
            self.0.consumer_waker.register(cx.waker());
            let items = self.try_pop_many(limit);
            if !items.is_empty() {
                return Poll::Ready(items);
            }
            if self.is_closed() {
                return Poll::Ready(self.try_pop_many(limit));
            }
            Poll::Pending
        })
        .await
    }

    /// Returns `true` if the producer has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        !self.0.producer_alive.load(Ordering::Acquire)
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.0.producer_alive.store(false, Ordering::Release);
        self.0.consumer_waker.wake();
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.0.consumer_alive.store(false, Ordering::Release);
        self.0.producer_waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    };

    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn try_push_pop() {
        let (mut producer, mut consumer) = channel(2);

        assert_eq!(producer.try_push(1), Ok(()));
        assert_eq!(producer.try_push(2), Ok(()));
        assert_eq!(producer.try_push(3), Err(3)); // Full

        assert_eq!(consumer.try_pop(), Some(1));
        assert_eq!(consumer.try_pop(), Some(2));
        assert_eq!(consumer.try_pop(), None); // Empty
    }

    #[tokio::test]
    async fn async_push_pop() {
        let (mut producer, mut consumer) = channel(2);

        assert!(producer.push(10).await.is_ok());
        assert!(producer.push(20).await.is_ok());

        assert_eq!(consumer.pop().await, Some(10));
        assert_eq!(consumer.pop().await, Some(20));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_batch_concurrency() {
        let (mut producer, mut consumer) = channel(100);
        let items_to_send = 5000;

        let producer_handle = task::spawn(async move {
            let data: Vec<usize> = (0 .. items_to_send).collect();
            assert!(producer.push_many(data).await.is_ok());
        });

        let consumer_handle = task::spawn(async move {
            let mut received = Vec::new();
            while received.len() < items_to_send {
                let mut batch = consumer.pop_many(50).await;
                received.append(&mut batch);
            }
            received
        });

        producer_handle.await.unwrap();
        let final_data = consumer_handle.await.unwrap();

        assert_eq!(final_data.len(), items_to_send);
        for (i, val) in final_data.into_iter().enumerate() {
            assert_eq!(i, val);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heavy_concurrency_single_elements() {
        let (mut producer, mut consumer) = channel(10);
        let items_to_send = 10_000;
        let sum = Arc::new(AtomicUsize::new(0));

        let sum_clone = sum.clone();
        let producer_handle = task::spawn(async move {
            for i in 1 ..= items_to_send {
                assert!(producer.push(i).await.is_ok());
            }
        });

        let consumer_handle = task::spawn(async move {
            for _ in 1 ..= items_to_send {
                if let Some(val) = consumer.pop().await {
                    sum_clone.fetch_add(val, Ordering::Relaxed);
                }
            }
        });

        let _ = tokio::join!(producer_handle, consumer_handle);

        let expected_sum = (items_to_send * (items_to_send + 1)) / 2;
        assert_eq!(sum.load(Ordering::Relaxed), expected_sum);
    }

    #[tokio::test]
    async fn endpoint_drop_wakes_peer() {
        let (mut producer, consumer) = channel::<i32>(1);
        drop(consumer);
        assert_eq!(producer.push(1).await, Err(1));

        let (producer, mut consumer) = channel::<i32>(1);
        drop(producer);
        assert_eq!(consumer.pop().await, None);
    }

    #[test]
    fn non_power_of_two_capacity_survives_counter_rollover() {
        let (mut tx, mut rx) = channel(3);
        // Seed an empty queue at the last position of the counter period.
        tx.0.head.store(5, Ordering::Relaxed);
        tx.0.tail.store(5, Ordering::Relaxed);
        assert_eq!(tx.try_push_many(&mut [1, 2, 3].into_iter()), 3);
        assert_eq!(tx.try_push(4), Err(4));
        assert_eq!(rx.try_pop_many(3), vec![1, 2, 3]);
        for value in 4 .. 40 {
            assert_eq!(tx.try_push(value), Ok(()));
            assert_eq!(rx.try_pop(), Some(value));
        }
    }

    #[test]
    fn panicking_batch_iterator_publishes_initialized_prefix() {
        use std::panic::{
            AssertUnwindSafe,
            catch_unwind,
        };
        let (mut tx, mut rx) = channel(3);
        let mut next = 0;
        let mut values = std::iter::from_fn(|| {
            next += 1;
            assert!(next != 2, "iterator panic");
            Some(String::from("first"))
        });
        assert!(catch_unwind(AssertUnwindSafe(|| tx.try_push_many(&mut values))).is_err());
        assert_eq!(rx.try_pop().as_deref(), Some("first"));
        assert_eq!(tx.try_push(String::from("second")), Ok(()));
        assert_eq!(rx.try_pop().as_deref(), Some("second"));
    }

    #[tokio::test]
    async fn pending_operations_wake_when_peer_drops() {
        use futures::poll;
        use tokio::time::{
            Duration,
            timeout,
        };
        let (mut tx, rx) = channel(1);
        tx.try_push(1).unwrap();
        let mut pending = Box::pin(tx.push(2));
        assert!(poll!(pending.as_mut()).is_pending());
        drop(rx);
        assert_eq!(timeout(Duration::from_secs(1), pending).await.unwrap(), Err(2));
        let (tx, mut rx) = channel::<i32>(1);
        let mut pending = Box::pin(rx.pop());
        assert!(poll!(pending.as_mut()).is_pending());
        drop(tx);
        assert_eq!(timeout(Duration::from_secs(1), pending).await.unwrap(), None);
    }

    #[tokio::test]
    async fn zero_batch_and_final_buffer_drain() {
        let (mut tx, mut rx) = channel(2);
        assert!(rx.pop_many(0).await.is_empty());
        tx.push_many([1, 2]).await.unwrap();
        drop(tx);
        assert_eq!(rx.pop_many(10).await, vec![1, 2]);
        assert_eq!(rx.pop().await, None);
    }
}
