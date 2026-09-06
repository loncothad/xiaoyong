//! A thread-safe, bounded MPMC broadcast channel.
//!
//! A short mutex protects buffer ownership and receiver registration together.
//! Tasks wait for capacity or values using independent event listeners.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{
        Context,
        Poll,
    },
};

use event_listener::{
    Event,
    EventListener,
};
use futures::Stream;
use smallvec::SmallVec;
use triomphe::Arc;

const MAX_RECEIVERS: usize = 128;
const MAX_SENDERS: usize = 128;

struct State<T> {
    buffer:       VecDeque<Arc<T>>,
    // Each position is relative to the front of the buffer. This avoids
    // unbounded sequence counters and their rollover hazards.
    receivers:    [Option<usize>; MAX_RECEIVERS],
    sender_count: usize,
}

impl<T> State<T> {
    fn has_receivers(&self) -> bool {
        self.receivers.iter().any(Option::is_some)
    }

    fn reclaim(&mut self) -> SmallVec<[Arc<T>; 8]> {
        let consumed = self
            .receivers
            .iter()
            .flatten()
            .copied()
            .min()
            .unwrap_or(self.buffer.len());
        for position in self.receivers.iter_mut().flatten() {
            *position -= consumed;
        }
        self.buffer.drain(.. consumed).collect()
    }
}

/// Shared broadcast state. Access through sender and receiver handles.
pub struct Shared<T> {
    state:    Mutex<State<T>>,
    capacity: usize,
    readable: Event,
    writable: Event,
}

/// A broadcast sender. Clones share the same bounded buffer.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

/// A broadcast receiver with its own position in the buffer.
///
/// Also implements [`Stream`]. A subscription starts with the next sent value.
pub struct Receiver<T> {
    shared:   Arc<Shared<T>>,
    id:       usize,
    listener: Option<EventListener>,
}

// Pinning the handle does not pin the heap-allocated channel values.
impl<T> Unpin for Receiver<T> {}

/// Creates a bounded broadcast channel with one sender and one receiver.
///
/// Every receiver observes every value sent after its subscription. The slowest
/// receiver applies backpressure. At most 128 senders and 128 receivers may be
/// active at once.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    let mut receivers = [None; MAX_RECEIVERS];
    receivers[0] = Some(0);
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            buffer: VecDeque::with_capacity(capacity),
            receivers,
            sender_count: 1,
        }),
        capacity,
        readable: Event::new(),
        writable: Event::new(),
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver {
            shared,
            id: 0,
            listener: None,
        },
    )
}

impl<T> Shared<T> {
    /// Tries to send a value, returning it if full or without receivers.
    pub fn try_push(&self, value: T) -> Result<(), T> {
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.has_receivers() || state.buffer.len() == self.capacity {
                return Err(value);
            }
            state.buffer.push_back(Arc::new(value));
        }
        self.readable.notify(usize::MAX);
        Ok(())
    }

    /// Sends the prefix that fits, returning any unsent suffix.
    /// Empty batches always succeed.
    pub fn try_push_many(&self, mut values: Vec<T>) -> Result<(), Vec<T>> {
        if values.is_empty() {
            return Ok(());
        }
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.has_receivers() {
                return Err(values);
            }
            let count = values.len().min(self.capacity - state.buffer.len());
            state.buffer.extend(values.drain(.. count).map(Arc::new));
        }
        self.readable.notify(usize::MAX);
        if values.is_empty() {
            Ok(())
        } else {
            Err(values)
        }
    }

    /// Tries to receive a value for a registered receiver ID.
    pub fn try_pop(&self, rx_id: usize) -> Option<Arc<T>> {
        let (value, retired) = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let position = state.receivers.get(rx_id).copied().flatten()?;
            let value = Arc::clone(state.buffer.get(position)?);
            state.receivers[rx_id] = Some(position + 1);
            (value, state.reclaim())
        };
        self.writable.notify(usize::MAX);
        // User destructors must run after committing state and releasing the lock.
        drop(retired);
        Some(value)
    }

    /// Tries to receive at most `max` values for a registered receiver ID.
    pub fn try_pop_many(&self, rx_id: usize, max: usize) -> Vec<Arc<T>> {
        let (values, retired) = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let Some(position) = state.receivers.get(rx_id).copied().flatten() else {
                return Vec::new();
            };
            let count = max.min(state.buffer.len() - position);
            let values = state.buffer.iter().skip(position).take(count).cloned().collect();
            state.receivers[rx_id] = Some(position + count);
            (values, state.reclaim())
        };
        self.writable.notify(usize::MAX);
        drop(retired);
        values
    }
}

impl<T> Sender<T> {
    /// Tries to send a value, returning it if full or closed.
    pub fn try_push(&self, value: T) -> Result<(), T> {
        self.shared.try_push(value)
    }

    /// Sends the prefix that fits, returning any unsent suffix.
    pub fn try_push_many(&self, values: Vec<T>) -> Result<(), Vec<T>> {
        self.shared.try_push_many(values)
    }

    /// Returns whether all receivers have been dropped.
    pub fn is_closed(&self) -> bool {
        !self
            .shared
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .has_receivers()
    }

    /// Creates another sender, or returns `None` at the sender limit.
    pub fn try_clone(&self) -> Option<Self> {
        let mut state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.sender_count == MAX_SENDERS {
            return None;
        }
        state.sender_count += 1;
        Some(Self {
            shared: Arc::clone(&self.shared),
        })
    }

    /// Sends a value, waiting for capacity. Returns the value if closed.
    pub async fn push(&self, mut value: T) -> Result<(), T> {
        loop {
            // Each operation owns a listener: simultaneous sends on one handle
            // cannot overwrite each other's wake registration.
            let listener = self.shared.writable.listen();
            match self.try_push(value) {
                | Ok(()) => return Ok(()),
                | Err(unsent) => value = unsent,
            }
            if self.is_closed() {
                return Err(value);
            }
            listener.await;
        }
    }

    /// Sends all values, returning the unsent suffix if closed.
    /// Canceling this future leaves any already sent prefix in the channel.
    pub async fn push_many(&self, mut values: Vec<T>) -> Result<(), Vec<T>> {
        loop {
            let listener = self.shared.writable.listen();
            match self.try_push_many(values) {
                | Ok(()) => return Ok(()),
                | Err(unsent) => values = unsent,
            }
            if self.is_closed() {
                return Err(values);
            }
            listener.await;
        }
    }
}

impl<T> Receiver<T> {
    /// Subscribes to future values, or returns `None` at the receiver limit.
    pub fn subscribe(&self) -> Option<Self> {
        let mut state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
        let id = state.receivers.iter().position(Option::is_none)?;
        state.receivers[id] = Some(state.buffer.len());
        Some(Self {
            shared: Arc::clone(&self.shared),
            id,
            listener: None,
        })
    }

    /// Returns whether all senders have been dropped. Buffered values can
    /// remain.
    pub fn is_closed(&self) -> bool {
        self.shared.state.lock().unwrap_or_else(|p| p.into_inner()).sender_count == 0
    }

    /// Receives a value without waiting, or returns `None` when empty.
    pub fn try_pop(&mut self) -> Option<Arc<T>> {
        self.shared.try_pop(self.id)
    }

    /// Receives up to `max` values without waiting.
    pub fn try_pop_many(&mut self, max: usize) -> Vec<Arc<T>> {
        self.shared.try_pop_many(self.id, max)
    }

    /// Receives a value, or `None` after closure and draining the buffer.
    pub async fn pop(&mut self) -> Option<Arc<T>> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }

    /// Receives up to `max` values, waiting until at least one is available.
    /// A zero limit returns immediately.
    pub async fn pop_many(&mut self, max: usize) -> Vec<Arc<T>> {
        if max == 0 {
            return Vec::new();
        }
        let Some(first) = self.pop().await else {
            return Vec::new();
        };
        let mut values = vec![first];
        values.extend(self.try_pop_many(max - 1));
        values
    }
}

impl<T> Stream for Receiver<T> {
    type Item = Arc<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.listener.is_none() {
                self.listener = Some(self.shared.readable.listen());
            }
            if let Some(value) = self.try_pop() {
                self.listener = None;
                return Poll::Ready(Some(value));
            }
            if self.is_closed() {
                self.listener = None;
                // Recheck after observing closure to include the final send.
                return Poll::Ready(self.try_pop());
            }
            match Pin::new(self.listener.as_mut().expect("listener registered")).poll(cx) {
                | Poll::Ready(()) => self.listener = None,
                | Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.try_clone().expect("maximum number of channel senders exceeded")
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let closed = {
            let mut state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
            state.sender_count -= 1;
            state.sender_count == 0
        };
        if closed {
            self.shared.readable.notify(usize::MAX);
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let retired = {
            let mut state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
            state.receivers[self.id] = None;
            state.reclaim()
        };
        self.shared.writable.notify(usize::MAX);
        drop(retired);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn basic_push_pop() {
        let (tx, mut rx) = channel::<i32>(4);

        tx.push(10).await.unwrap();
        tx.push(20).await.unwrap();

        assert_eq!(*rx.pop().await.unwrap(), 10);
        assert_eq!(*rx.pop().await.unwrap(), 20);
    }

    #[tokio::test]
    async fn broadcast_semantics() {
        let (tx, mut rx1) = channel::<String>(4);
        let mut rx2 = rx1.subscribe().unwrap();

        tx.push("Hello".to_string()).await.unwrap();
        tx.push("World".to_string()).await.unwrap();

        assert_eq!(&*rx1.pop().await.unwrap(), "Hello");
        assert_eq!(&*rx2.pop().await.unwrap(), "Hello");
        assert_eq!(&*rx1.pop().await.unwrap(), "World");
        assert_eq!(&*rx2.pop().await.unwrap(), "World");
    }

    #[tokio::test]
    async fn push_pop_many() {
        let (tx, mut rx) = channel::<i32>(8);

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
        let (tx, mut rx) = channel::<i32>(4);

        let recv_task = tokio::spawn(async move { *rx.pop().await.unwrap() });

        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.push(99).await.unwrap();

        assert_eq!(recv_task.await.unwrap(), 99);
    }

    #[tokio::test]
    async fn async_wait_on_full() {
        let (tx, mut rx) = channel::<i32>(2);

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
        let (tx, mut rx) = channel::<i32>(4);

        tx.push(1).await.unwrap();
        drop(tx);

        assert_eq!(*rx.pop().await.unwrap(), 1);
        assert!(rx.pop().await.is_none());
    }

    #[tokio::test]
    async fn exact_non_power_of_two_capacity_and_zero_batch() {
        let (tx, mut rx) = channel::<i32>(3);
        assert!(tx.try_push(1).is_ok());
        assert!(tx.try_push(2).is_ok());
        assert!(tx.try_push(3).is_ok());
        assert_eq!(tx.try_push(4), Err(4));
        assert!(rx.pop_many(0).await.is_empty());
    }

    #[tokio::test]
    async fn send_fails_after_last_receiver_is_dropped() {
        let (tx, rx) = channel::<i32>(2);
        drop(rx);
        assert_eq!(tx.push(42).await, Err(42));
    }

    #[tokio::test]
    async fn slow_receiver_blocks_producer() {
        let (tx, mut rx1) = channel::<i32>(2);
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
        let (tx, mut rx) = channel::<i32>(128);
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
        while rx.pop().await.is_some() {
            received += 1;
        }

        for task in producer_tasks {
            task.await.unwrap();
        }

        assert_eq!(received, NUM_PRODUCERS * ITEMS_PER_PRODUCER as usize);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_broadcast_consumers() {
        let (tx, mut rx1) = channel::<i32>(64);
        let mut rx2 = rx1.subscribe().unwrap();
        let mut rx3 = rx1.subscribe().unwrap();

        const ITEMS: i32 = 2000;

        let t1 = tokio::spawn(async move {
            let mut count = 0;
            while rx1.pop().await.is_some() {
                count += 1;
            }
            count
        });

        let t2 = tokio::spawn(async move {
            let mut count = 0;
            while rx2.pop().await.is_some() {
                count += 1;
            }
            count
        });

        let t3 = tokio::spawn(async move {
            let mut count = 0;
            while rx3.pop().await.is_some() {
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
        for mut rx in receivers {
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

    #[tokio::test]
    async fn concurrent_sends_on_one_handle_keep_independent_wakers() {
        use futures::{
            StreamExt,
            poll,
            stream::FuturesUnordered,
        };
        let (tx, mut rx) = channel(1);
        tx.push(0).await.unwrap();
        let mut sends = (1 .. 4).map(|value| tx.push(value)).collect::<FuturesUnordered<_>>();
        assert!(poll!(sends.next()).is_pending());
        let collect = async {
            let mut values = Vec::new();
            for _ in 0 .. 4 {
                values.push(*rx.pop().await.unwrap());
            }
            values
        };
        let send_all = async {
            while let Some(result) = sends.next().await {
                result.unwrap();
            }
        };
        let (mut received, ()) = timeout(Duration::from_secs(2), async { tokio::join!(collect, send_all) })
            .await
            .unwrap();
        received.sort();
        assert_eq!(received, vec![0, 1, 2, 3]);
    }

    #[test]
    fn reused_subscription_starts_at_current_head() {
        let (tx, mut original) = channel(2);
        for value in 0 .. 50 {
            let mut subscriber = original.subscribe().unwrap();
            tx.try_push(value).unwrap();
            assert_eq!(*subscriber.try_pop().unwrap(), value);
            assert_eq!(*original.try_pop().unwrap(), value);
            drop(subscriber);
        }
    }

    #[tokio::test]
    async fn stream_supports_non_unpin_values_and_drains_after_close() {
        use std::marker::PhantomPinned;

        use futures::StreamExt;
        let (tx, mut rx) = channel(1);
        assert!(tx.try_push((42, PhantomPinned)).is_ok());
        drop(tx);
        assert_eq!(rx.next().await.unwrap().0, 42);
        assert!(rx.next().await.is_none());
        assert!(rx.next().await.is_none());
    }

    #[test]
    fn empty_batches_succeed_when_full_or_closed() {
        let (tx, rx) = channel(1);
        tx.try_push(1).unwrap();
        assert_eq!(tx.try_push_many(vec![]), Ok(()));
        drop(rx);
        assert_eq!(tx.try_push_many(vec![]), Ok(()));
    }

    #[test]
    fn subscriber_limit_recovers_after_drop() {
        let (_tx, rx) = channel::<i32>(1);
        let mut subscribers: Vec<_> = (1 .. MAX_RECEIVERS).map(|_| rx.subscribe().unwrap()).collect();
        assert!(rx.subscribe().is_none());
        subscribers.pop();
        assert!(rx.subscribe().is_some());
    }

    #[test]
    fn dropping_receiver_commits_state_before_running_destructors() {
        use std::panic::{
            AssertUnwindSafe,
            catch_unwind,
        };
        struct Bomb(bool);
        impl Drop for Bomb {
            fn drop(&mut self) {
                assert!(!self.0, "destructor panic");
            }
        }
        let (tx, rx) = channel(1);
        assert!(tx.try_push(Bomb(true)).is_ok());
        assert!(catch_unwind(AssertUnwindSafe(|| drop(rx))).is_err());
        assert!(tx.is_closed());
        assert!(tx.try_push(Bomb(false)).is_err());
        drop(tx);
    }
}
