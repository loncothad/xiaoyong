//! A thread-safe, bounded, multi-producer multi-consumer (MPMC) broadcast
//! channel.

use std::{
    cell::UnsafeCell,
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::atomic::{
        AtomicBool,
        AtomicU64,
        AtomicUsize,
        Ordering,
    },
    task::{
        Context,
        Poll,
    },
};

use crossbeam_utils::CachePadded;
use futures::task::AtomicWaker;
use triomphe::Arc;

const MAX_RECEIVERS: usize = 128;
const MAX_SENDERS: usize = 128;

/// Represents a single memory slot in the ring buffer.
/// Relies on a sequence lock to coordinate lock-free access.
struct Slot<T> {
    /// The sequence number dictates access rights.
    /// If seq == head, the slot is ready to be written.
    /// If seq == tail + 1, the slot is ready to be read.
    sequence: AtomicU64,
    data:     UnsafeCell<MaybeUninit<Arc<T>>>,
}

/// Tracks the state of an individual consumer in the broadcast group.
struct ReceiverState {
    active: AtomicBool,
    /// The specific read position of this receiver.
    tail:   AtomicU64,
    /// Waker for suspending the consumer when the queue is empty at its tail.
    waker:  AtomicWaker,
}

/// Tracks the state of an individual producer.
struct SenderState {
    active: AtomicBool,
    /// Waker for suspending the producer when the queue is full (head meets
    /// slowest tail).
    waker:  AtomicWaker,
}

/// The shared state allocated on the heap, referenced by all Senders and
/// Receivers.
pub struct Shared<T> {
    buffer: Box<[Slot<T>]>,
    mask:   u64,

    /// The global write position. Claimed via Compare-And-Swap (CAS).
    head: CachePadded<AtomicU64>,

    receivers: Box<[CachePadded<ReceiverState>]>,
    senders:   Box<[CachePadded<SenderState>]>,

    closed:       AtomicBool,
    sender_count: AtomicUsize,
}

// Safety: The Shared structure explicitly handles internal mutability and
// thread synchronization.
unsafe impl<T: Send + Sync> Send for Shared<T> {}
unsafe impl<T: Send + Sync> Sync for Shared<T> {}

/// The transmitting half of the channel.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    id:     usize,
}

/// The receiving half of the channel.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    id:     usize,
}

/// Create a new bounded channel, returning the sender and receiver halves.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0 && capacity.next_power_of_two() != 0, "'capacity' overflow");

    // Force capacity to a power of two to allow bitwise AND instead of modulo.
    let cap = capacity.next_power_of_two();
    let mask = (cap - 1) as u64;

    let mut buffer = Vec::with_capacity(cap);
    for _ in 0 .. cap {
        buffer.push(Slot {
            sequence: AtomicU64::new(0),
            data:     UnsafeCell::new(MaybeUninit::uninit()),
        });
    }

    let mut receivers = Vec::with_capacity(MAX_RECEIVERS);
    for _ in 0 .. MAX_RECEIVERS {
        receivers.push(CachePadded::new(ReceiverState {
            active: AtomicBool::new(false),
            tail:   AtomicU64::new(u64::MAX),
            waker:  AtomicWaker::new(),
        }));
    }

    let mut senders = Vec::with_capacity(MAX_SENDERS);
    for _ in 0 .. MAX_SENDERS {
        senders.push(CachePadded::new(SenderState {
            active: AtomicBool::new(false),
            waker:  AtomicWaker::new(),
        }));
    }

    let shared = Arc::new(Shared {
        buffer: buffer.into_boxed_slice(),
        mask,
        head: CachePadded::new(AtomicU64::new(0)),
        receivers: receivers.into_boxed_slice(),
        senders: senders.into_boxed_slice(),
        closed: AtomicBool::new(false),
        sender_count: AtomicUsize::new(1),
    });

    // Initialize the primary sender and receiver to index 0.
    shared.senders[0].active.store(true, Ordering::Relaxed);
    shared.receivers[0].active.store(true, Ordering::Relaxed);
    shared.receivers[0].tail.store(0, Ordering::Relaxed);

    (
        Sender {
            shared: shared.clone(),
            id:     0,
        },
        Receiver {
            shared,
            id: 0,
        },
    )
}

impl<T> Shared<T> {
    /// Determine the slowest active receiver.
    fn min_tail(&self) -> u64 {
        let mut min = u64::MAX;
        let mut any_active = false;
        for rx in self.receivers.iter() {
            if rx.active.load(Ordering::Acquire) {
                any_active = true;
                let t = rx.tail.load(Ordering::Acquire);
                if t < min {
                    min = t;
                }
            }
        }
        // If all receivers drop, advance min_tail to head so producers can freely
        // overwrite/drop data.
        if any_active {
            min
        } else {
            self.head.load(Ordering::Relaxed)
        }
    }

    fn wake_receivers(&self) {
        for rx in self.receivers.iter() {
            if rx.active.load(Ordering::Relaxed) {
                rx.waker.wake();
            }
        }
    }

    fn wake_producers(&self) {
        for tx in self.senders.iter() {
            if tx.active.load(Ordering::Relaxed) {
                tx.waker.wake();
            }
        }
    }

    /// Try to push a value into the channel without blocking.
    pub fn try_push(&self, value: T) -> Result<(), T> {
        let cap = self.mask + 1;
        let mut head = self.head.load(Ordering::Acquire);

        loop {
            // Check boundary: Head cannot wrap around past the slowest reader.
            if head.wrapping_sub(self.min_tail()) >= cap {
                return Err(value);
            }

            // Attempt to claim the current head sequence.
            match self
                .head
                .compare_exchange_weak(head, head + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                | Ok(_) => {
                    let slot = &self.buffer[(head & self.mask) as usize];
                    unsafe {
                        let arc = Arc::new(value);
                        if head >= cap {
                            // Safely drop the element from the previous wrap-around.
                            // We know no receiver is reading this because head < min_tail + cap.
                            drop((*slot.data.get()).assume_init_read());
                        }
                        (*slot.data.get()).write(arc);
                    }
                    // Release the slot to consumers by updating the sequence.
                    slot.sequence.store(head + 1, Ordering::Release);
                    self.wake_receivers();
                    return Ok(());
                },
                | Err(actual) => head = actual, // CAS failed, retry with updated head
            }
        }
    }

    /// Try to push multiple values into the channel without blocking.
    pub fn try_push_many(&self, mut values: Vec<T>) -> Result<(), Vec<T>> {
        let cap = self.mask + 1;
        let mut head = self.head.load(Ordering::Acquire);

        loop {
            let avail = cap.saturating_sub(head.wrapping_sub(self.min_tail()));
            if avail == 0 {
                return Err(values);
            }

            // Claim contiguous sequence block up to available capacity
            let claim = avail.min(values.len() as u64) as usize;

            if self
                .head
                .compare_exchange_weak(head, head + claim as u64, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                for (i, val) in values.drain(.. claim).enumerate() {
                    let curr_h = head + i as u64;
                    let slot = &self.buffer[(curr_h & self.mask) as usize];
                    unsafe {
                        let arc = Arc::new(val);
                        if curr_h >= cap {
                            drop((*slot.data.get()).assume_init_read());
                        }
                        (*slot.data.get()).write(arc);
                    }
                    slot.sequence.store(curr_h + 1, Ordering::Release);
                }
                self.wake_receivers();

                if values.is_empty() {
                    return Ok(());
                } else {
                    return Err(values); // Return unpushed remainder
                }
            } else {
                head = self.head.load(Ordering::Acquire);
            }
        }
    }

    /// Try to pop a value from the channel without blocking.
    pub fn try_pop(&self, rx_id: usize) -> Option<Arc<T>> {
        let rx = &self.receivers[rx_id];
        let tail = rx.tail.load(Ordering::Relaxed);

        let slot = &self.buffer[(tail & self.mask) as usize];
        let seq = slot.sequence.load(Ordering::Acquire);

        // Sequence == tail + 1 means the producer has fully committed the write
        if seq == tail + 1 {
            let data = unsafe { (*slot.data.get()).assume_init_ref().clone() };
            // Advance this receiver's individual tail
            rx.tail.store(tail + 1, Ordering::Release);
            self.wake_producers();
            Some(data)
        } else {
            None
        }
    }

    /// Try to pop multiple values from the channel without blocking.
    pub fn try_pop_many(&self, rx_id: usize, max: usize) -> Vec<Arc<T>> {
        let mut results = Vec::with_capacity(max);
        let rx = &self.receivers[rx_id];
        let mut tail = rx.tail.load(Ordering::Relaxed);

        // Fast-path contiguous reading without touching atomic tail on every item
        for _ in 0 .. max {
            let slot = &self.buffer[(tail & self.mask) as usize];
            if slot.sequence.load(Ordering::Acquire) == tail + 1 {
                results.push(unsafe { (*slot.data.get()).assume_init_ref().clone() });
                tail += 1;
            } else {
                break;
            }
        }

        if !results.is_empty() {
            // Commit all read positions at once
            rx.tail.store(tail, Ordering::Release);
            self.wake_producers();
        }
        results
    }
}

impl<T> Sender<T> {
    /// Push a value into the channel.
    pub async fn push(&self, value: T) -> Result<(), T> {
        PushFuture {
            sender: self,
            value:  Some(value),
        }
        .await
    }

    /// Push multiple values into the channel.
    pub async fn push_many(&self, values: Vec<T>) -> Result<(), Vec<T>> {
        PushManyFuture {
            sender: self,
            values: Some(values),
        }
        .await
    }
}

impl<T> Receiver<T> {
    /// Creates a new receiver.
    pub fn subscribe(&self) -> Option<Receiver<T>> {
        for (id, rx) in self.shared.receivers.iter().enumerate() {
            if !rx.active.load(Ordering::Acquire) {
                // Claim the receiver slot
                if rx
                    .active
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    rx.tail
                        .store(self.shared.head.load(Ordering::Acquire), Ordering::Release);
                    return Some(Receiver {
                        shared: self.shared.clone(),
                        id,
                    });
                }
            }
        }
        None // Max receivers reached
    }

    /// Asynchronously pops a value from the channel.
    pub async fn pop(&self) -> Option<Arc<T>> {
        PopFuture {
            receiver: self
        }
        .await
    }

    /// Asynchronously pops multiple values from the channel.
    pub async fn pop_many(&self, max: usize) -> Vec<Arc<T>> {
        PopManyFuture {
            receiver: self,
            max,
        }
        .await
    }
}

struct PushFuture<'a, T> {
    sender: &'a Sender<T>,
    value:  Option<T>,
}

impl<'a, T> Future for PushFuture<'a, T> {
    type Output = Result<(), T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: Struct contains no structurally pinned fields. Unpin projection is
        // safe.
        let this = unsafe { self.get_unchecked_mut() };
        let val = this.value.take().expect("polled after ready");

        match this.sender.shared.try_push(val) {
            | Ok(_) => Poll::Ready(Ok(())),
            | Err(v) => {
                if this.sender.shared.closed.load(Ordering::Acquire) {
                    return Poll::Ready(Err(v));
                }

                // Register waker BEFORE re-checking condition to avoid lost wakeups
                this.sender.shared.senders[this.sender.id].waker.register(cx.waker());

                match this.sender.shared.try_push(v) {
                    | Ok(_) => Poll::Ready(Ok(())),
                    | Err(v_retry) => {
                        this.value = Some(v_retry);
                        Poll::Pending
                    },
                }
            },
        }
    }
}

struct PushManyFuture<'a, T> {
    sender: &'a Sender<T>,
    values: Option<Vec<T>>,
}

impl<'a, T> Future for PushManyFuture<'a, T> {
    type Output = Result<(), Vec<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let vals = this.values.take().expect("polled after ready");

        match this.sender.shared.try_push_many(vals) {
            | Ok(_) => Poll::Ready(Ok(())),
            | Err(rem) => {
                if this.sender.shared.closed.load(Ordering::Acquire) {
                    return Poll::Ready(Err(rem));
                }

                this.sender.shared.senders[this.sender.id].waker.register(cx.waker());

                match this.sender.shared.try_push_many(rem) {
                    | Ok(_) => Poll::Ready(Ok(())),
                    | Err(rem_retry) => {
                        this.values = Some(rem_retry);
                        Poll::Pending
                    },
                }
            },
        }
    }
}

struct PopFuture<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<'a, T> Future for PopFuture<'a, T> {
    type Output = Option<Arc<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(val) = this.receiver.shared.try_pop(this.receiver.id) {
            return Poll::Ready(Some(val));
        }

        if this.receiver.shared.closed.load(Ordering::Acquire) {
            return Poll::Ready(None);
        }

        this.receiver.shared.receivers[this.receiver.id]
            .waker
            .register(cx.waker());

        if let Some(val) = this.receiver.shared.try_pop(this.receiver.id) {
            Poll::Ready(Some(val))
        } else {
            Poll::Pending
        }
    }
}

struct PopManyFuture<'a, T> {
    receiver: &'a Receiver<T>,
    max:      usize,
}

impl<'a, T> Future for PopManyFuture<'a, T> {
    type Output = Vec<Arc<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let res = this.receiver.shared.try_pop_many(this.receiver.id, this.max);
        if !res.is_empty() {
            return Poll::Ready(res);
        }

        if this.receiver.shared.closed.load(Ordering::Acquire) {
            return Poll::Ready(Vec::new());
        }

        this.receiver.shared.receivers[this.receiver.id]
            .waker
            .register(cx.waker());

        let res = this.receiver.shared.try_pop_many(this.receiver.id, this.max);
        if !res.is_empty() {
            Poll::Ready(res)
        } else {
            Poll::Pending
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Ordering::SeqCst);
        let mut new_id = None;
        for (id, tx) in self.shared.senders.iter().enumerate() {
            if !tx.active.load(Ordering::Acquire) {
                if tx
                    .active
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    new_id = Some(id);
                    break;
                }
            }
        }
        Sender {
            shared: self.shared.clone(),
            id:     new_id.expect("Max senders exceeded"),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared.senders[self.id].active.store(false, Ordering::Release);
        if self.shared.sender_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.shared.closed.store(true, Ordering::Release);
            self.shared.wake_receivers();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let rx = &self.shared.receivers[self.id];
        rx.active.store(false, Ordering::Release);
        // Advance out of bounds so this consumer no longer throttles min_tail
        rx.tail.store(u64::MAX, Ordering::Release);
        self.shared.wake_producers();
    }
}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Acquire);
        let cap = self.mask + 1;
        let start = head.saturating_sub(cap);

        // Ensure remaining undropped inner Arc values are freed.
        for i in start .. head {
            let slot = &self.buffer[(i & self.mask) as usize];
            if slot.sequence.load(Ordering::Acquire) == i + 1 {
                unsafe {
                    (*slot.data.get()).assume_init_drop();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn basic_push_pop() {
        let (tx, rx) = channel::<i32>(4);

        tx.push(10).await.unwrap();
        tx.push(20).await.unwrap();

        assert_eq!(*rx.pop().await.unwrap(), 10);
        assert_eq!(*rx.pop().await.unwrap(), 20);
    }

    #[tokio::test]
    async fn broadcast_semantics() {
        let (tx, rx1) = channel::<String>(4);
        let rx2 = rx1.subscribe().unwrap();

        tx.push("Hello".to_string()).await.unwrap();
        tx.push("World".to_string()).await.unwrap();

        assert_eq!(&*rx1.pop().await.unwrap(), "Hello");
        assert_eq!(&*rx2.pop().await.unwrap(), "Hello");
        assert_eq!(&*rx1.pop().await.unwrap(), "World");
        assert_eq!(&*rx2.pop().await.unwrap(), "World");
    }

    #[tokio::test]
    async fn push_pop_many() {
        let (tx, rx) = channel::<i32>(8);

        tx.push_many(vec![1, 2, 3]).await.unwrap();
        tx.push_many(vec![4, 5]).await.unwrap();

        let batch = rx.pop_many(4).await;
        assert_eq!(batch.len(), 4);
        assert_eq!(*batch[0], 1);
        assert_eq!(*batch[3], 4);

        let batch2 = rx.pop_many(4).await;
        assert_eq!(batch2.len(), 1);
        assert_eq!(*batch2[0], 5);
    }

    #[tokio::test]
    async fn async_wait_on_empty() {
        let (tx, rx) = channel::<i32>(4);

        let recv_task = tokio::spawn(async move { *rx.pop().await.unwrap() });

        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.push(99).await.unwrap();

        assert_eq!(recv_task.await.unwrap(), 99);
    }

    #[tokio::test]
    async fn async_wait_on_full() {
        let (tx, rx) = channel::<i32>(2);

        tx.push(1).await.unwrap();
        tx.push(2).await.unwrap();

        let tx_clone = tx.clone();
        let send_task = tokio::spawn(async move {
            tx_clone.push(3).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(*rx.pop().await.unwrap(), 1);

        send_task.await.unwrap();
        assert_eq!(*rx.pop().await.unwrap(), 2);
        assert_eq!(*rx.pop().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn channel_close_on_tx_drop() {
        let (tx, rx) = channel::<i32>(4);

        tx.push(1).await.unwrap();
        drop(tx);

        assert_eq!(*rx.pop().await.unwrap(), 1);
        assert!(rx.pop().await.is_none());
    }

    #[tokio::test]
    async fn slow_receiver_blocks_producer() {
        let (tx, rx1) = channel::<i32>(2);
        let rx2 = rx1.subscribe().unwrap();

        tx.push(1).await.unwrap();
        tx.push(2).await.unwrap();

        assert_eq!(*rx1.pop().await.unwrap(), 1);

        let push_res = timeout(Duration::from_millis(10), tx.push(3)).await;
        assert!(push_res.is_err());

        drop(rx2);

        tx.push(3).await.unwrap();
        assert_eq!(*rx1.pop().await.unwrap(), 2);
        assert_eq!(*rx1.pop().await.unwrap(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_producers() {
        let (tx, rx) = channel::<i32>(128);
        const NUM_PRODUCERS: usize = 4;
        const ITEMS_PER_PRODUCER: i32 = 1000;

        let mut producer_tasks = vec![];
        for i in 0 .. NUM_PRODUCERS {
            let tx_clone = tx.clone();
            producer_tasks.push(tokio::spawn(async move {
                for j in 0 .. ITEMS_PER_PRODUCER {
                    tx_clone.push((i as i32 * ITEMS_PER_PRODUCER) + j).await.unwrap();
                }
            }));
        }
        drop(tx);

        let mut received = 0;
        while let Some(_) = rx.pop().await {
            received += 1;
        }

        for task in producer_tasks {
            task.await.unwrap();
        }

        assert_eq!(received, NUM_PRODUCERS * ITEMS_PER_PRODUCER as usize);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_broadcast_consumers() {
        let (tx, rx1) = channel::<i32>(64);
        let rx2 = rx1.subscribe().unwrap();
        let rx3 = rx1.subscribe().unwrap();

        const ITEMS: i32 = 2000;

        let t1 = tokio::spawn(async move {
            let mut count = 0;
            while let Some(_) = rx1.pop().await {
                count += 1;
            }
            count
        });

        let t2 = tokio::spawn(async move {
            let mut count = 0;
            while let Some(_) = rx2.pop().await {
                count += 1;
            }
            count
        });

        let t3 = tokio::spawn(async move {
            let mut count = 0;
            while let Some(_) = rx3.pop().await {
                count += 1;
            }
            count
        });

        tokio::spawn(async move {
            for i in 0 .. ITEMS {
                tx.push(i).await.unwrap();
            }
        })
        .await
        .unwrap();

        assert_eq!(t1.await.unwrap(), ITEMS);
        assert_eq!(t2.await.unwrap(), ITEMS);
        assert_eq!(t3.await.unwrap(), ITEMS);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn mpmc_fuzz_test() {
        let (tx, rx1) = channel::<usize>(32); // Small capacity forces frequent async suspensions
        let mut receivers = vec![rx1];

        for _ in 0 .. 3 {
            receivers.push(receivers[0].subscribe().unwrap());
        }

        const PRODUCERS: usize = 4;
        const MESSAGES: usize = 5000;

        let mut send_tasks = vec![];
        for _ in 0 .. PRODUCERS {
            let t = tx.clone();
            send_tasks.push(tokio::spawn(async move {
                for _ in 0 .. MESSAGES {
                    t.push(1).await.unwrap();
                }
            }));
        }
        drop(tx);

        let mut recv_tasks = vec![];
        for rx in receivers {
            recv_tasks.push(tokio::spawn(async move {
                let mut sum = 0;
                while let Some(v) = rx.pop().await {
                    sum += *v;
                }
                sum
            }));
        }

        for t in send_tasks {
            t.await.unwrap();
        }

        let expected_total = PRODUCERS * MESSAGES;
        for t in recv_tasks {
            assert_eq!(t.await.unwrap(), expected_total);
        }
    }
}
