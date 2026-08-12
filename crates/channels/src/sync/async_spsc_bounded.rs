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
            current = current.wrapping_add(1);
        }
    }
}

/// The transmitting half of the SPSC channel.
pub struct Producer<T>(Arc<Shared<T>>);
/// The receiving half of the SPSC channel.
pub struct Consumer<T>(Arc<Shared<T>>);

/// Creates a new lock-free SPSC channel with a fixed capacity.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(capacity > 0, "Capacity must be greater than 0");

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
    pub fn try_push(&self, item: T) -> Result<(), T> {
        if !self.0.consumer_alive.load(Ordering::Acquire) {
            return Err(item);
        }
        let head = self.0.head.load(Ordering::Relaxed);
        let tail = self.0.tail.load(Ordering::Acquire);

        if head.wrapping_sub(tail) >= self.0.capacity {
            return Err(item);
        }

        let index = head % self.0.capacity;
        // SAFETY: the unique producer owns the free slot at `head` until it
        // publishes the increment with a Release store.
        unsafe {
            (*self.0.buffer[index].get()).write(item);
        }

        self.0.head.store(head.wrapping_add(1), Ordering::Release);
        self.0.consumer_waker.wake();
        Ok(())
    }

    /// Try to push as many items as possible from an
    /// iterator. Returns the number of items successfully pushed.
    pub fn try_push_many<I: Iterator<Item = T>>(&self, items: &mut I) -> usize {
        if !self.0.consumer_alive.load(Ordering::Acquire) {
            return 0;
        }
        let head = self.0.head.load(Ordering::Relaxed);
        let tail = self.0.tail.load(Ordering::Acquire);

        let available = self.0.capacity - head.wrapping_sub(tail);
        if available == 0 {
            return 0;
        }

        let mut pushed = 0;
        for _ in 0 .. available {
            match items.next() {
                | Some(item) => {
                    let index = (head.wrapping_add(pushed)) % self.0.capacity;
                    // SAFETY: each offset below `available` is an exclusively
                    // producer-owned free slot.
                    unsafe {
                        (*self.0.buffer[index].get()).write(item);
                    }
                    pushed += 1;
                },
                | None => break,
            }
        }

        if pushed > 0 {
            self.0.head.store(head.wrapping_add(pushed), Ordering::Release);
            self.0.consumer_waker.wake();
        }

        pushed
    }

    /// Pushes one item, suspending while the queue is full.
    ///
    /// Returns the item if the consumer has been dropped.
    pub async fn push(&self, item: T) -> Result<(), T> {
        let mut item = Some(item);
        poll_fn(move |cx| {
            let val = item.take().expect("push future polled after completion");
            match self.try_push(val) {
                | Ok(()) => Poll::Ready(Ok(())),
                | Err(ret) => {
                    if !self.0.consumer_alive.load(Ordering::Acquire) {
                        return Poll::Ready(Err(ret));
                    }
                    item = Some(ret);
                    self.0.producer_waker.register(cx.waker());

                    // Double check to prevent missed wakeups
                    let head = self.0.head.load(Ordering::Relaxed);
                    let tail = self.0.tail.load(Ordering::Acquire);
                    if self.0.capacity - head.wrapping_sub(tail) > 0 {
                        cx.waker().wake_by_ref();
                    }
                    Poll::Pending
                },
            }
        })
        .await
    }

    /// Pushes all items, suspending while the queue is full.
    ///
    /// Returns the unpushed suffix if the consumer is dropped.
    pub async fn push_many<I: IntoIterator<Item = T>>(&self, items: I) -> Result<(), Vec<T>> {
        let mut remaining = items.into_iter().collect::<VecDeque<_>>();
        poll_fn(move |cx| {
            while let Some(item) = remaining.pop_front() {
                if let Err(item) = self.try_push(item) {
                    remaining.push_front(item);
                    break;
                }
            }

            if remaining.is_empty() {
                Poll::Ready(Ok(()))
            } else if !self.0.consumer_alive.load(Ordering::Acquire) {
                Poll::Ready(Err(remaining.drain(..).collect()))
            } else {
                self.0.producer_waker.register(cx.waker());

                let head = self.0.head.load(Ordering::Relaxed);
                let tail = self.0.tail.load(Ordering::Acquire);
                if self.0.capacity - head.wrapping_sub(tail) > 0 {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
        })
        .await
    }
}

impl<T> Consumer<T> {
    /// Try to pop a single item.
    pub fn try_pop(&self) -> Option<T> {
        let tail = self.0.tail.load(Ordering::Relaxed);
        let head = self.0.head.load(Ordering::Acquire);

        if head == tail {
            return None;
        }

        let index = tail % self.0.capacity;
        // SAFETY: the Acquire load of `head` proves this slot was initialized,
        // and the unique consumer owns it until advancing `tail`.
        let item = unsafe { self.0.buffer[index].get().read().assume_init() };

        self.0.tail.store(tail.wrapping_add(1), Ordering::Release);
        self.0.producer_waker.wake();
        Some(item)
    }

    /// Try to pop up to `limit` items.
    pub fn try_pop_many(&self, limit: usize) -> Vec<T> {
        let tail = self.0.tail.load(Ordering::Relaxed);
        let head = self.0.head.load(Ordering::Acquire);

        let available = head.wrapping_sub(tail);
        let to_pop = available.min(limit);

        if to_pop == 0 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(to_pop);
        for i in 0 .. to_pop {
            let index = (tail.wrapping_add(i)) % self.0.capacity;
            // SAFETY: every offset below `to_pop` was published by the producer
            // and is exclusively owned by this consumer.
            let item = unsafe { self.0.buffer[index].get().read().assume_init() };
            result.push(item);
        }

        self.0.tail.store(tail.wrapping_add(to_pop), Ordering::Release);
        self.0.producer_waker.wake();
        result
    }

    /// Pops one item, suspending while the queue is empty.
    ///
    /// Returns `None` after the producer is dropped and buffered items drain.
    pub async fn pop(&self) -> Option<T> {
        poll_fn(|cx| {
            match self.try_pop() {
                | Some(item) => Poll::Ready(Some(item)),
                | None if !self.0.producer_alive.load(Ordering::Acquire) => Poll::Ready(None),
                | None => {
                    self.0.consumer_waker.register(cx.waker());

                    let tail = self.0.tail.load(Ordering::Relaxed);
                    let head = self.0.head.load(Ordering::Acquire);
                    if head != tail {
                        cx.waker().wake_by_ref();
                    }
                    Poll::Pending
                },
            }
        })
        .await
    }

    /// Pop up to `limit` items, suspending until at least 1
    /// item is available.
    pub async fn pop_many(&self, limit: usize) -> Vec<T> {
        assert!(limit > 0, "Limit must be greater than 0");
        poll_fn(|cx| {
            let res = self.try_pop_many(limit);
            if !res.is_empty() {
                Poll::Ready(res)
            } else if !self.0.producer_alive.load(Ordering::Acquire) {
                Poll::Ready(Vec::new())
            } else {
                self.0.consumer_waker.register(cx.waker());

                let tail = self.0.tail.load(Ordering::Relaxed);
                let head = self.0.head.load(Ordering::Acquire);
                if head != tail {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
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
        let (producer, consumer) = channel(2);

        assert_eq!(producer.try_push(1), Ok(()));
        assert_eq!(producer.try_push(2), Ok(()));
        assert_eq!(producer.try_push(3), Err(3)); // Full

        assert_eq!(consumer.try_pop(), Some(1));
        assert_eq!(consumer.try_pop(), Some(2));
        assert_eq!(consumer.try_pop(), None); // Empty
    }

    #[tokio::test]
    async fn async_push_pop() {
        let (producer, consumer) = channel(2);

        assert!(producer.push(10).await.is_ok());
        assert!(producer.push(20).await.is_ok());

        assert_eq!(consumer.pop().await, Some(10));
        assert_eq!(consumer.pop().await, Some(20));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_batch_concurrency() {
        let (producer, consumer) = channel(100);
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
        let (producer, consumer) = channel(10);
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
        let (producer, consumer) = channel::<i32>(1);
        drop(consumer);
        assert_eq!(producer.push(1).await, Err(1));

        let (producer, consumer) = channel::<i32>(1);
        drop(producer);
        assert_eq!(consumer.pop().await, None);
    }
}
