//! A thread-safe, single-use channel for transferring a single value.

use std::{
    cell::UnsafeCell,
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::atomic::{
        AtomicU8,
        Ordering,
    },
    task::{
        Context,
        Poll,
    },
};

use futures::task::AtomicWaker;
use triomphe::Arc;

const INCOMPLETE: u8 = 0;
const READY: u8 = 1;
const TX_DROPPED: u8 = 2;
const RX_DROPPED: u8 = 3;

struct Inner<T> {
    state: AtomicU8,
    waker: AtomicWaker,
    data:  UnsafeCell<MaybeUninit<T>>,
}

// Safety: The state machine guarantees exclusive access to the UnsafeCell.
// T must be Send to transfer across thread boundaries safely.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

/// The transmitting half of the channel.
pub struct Sender<T> {
    inner: Option<Arc<Inner<T>>>,
}

/// The receiving half of the channel.
pub struct Receiver<T> {
    inner: Option<Arc<Inner<T>>>,
}

/// Creates a new bounded channel, returning the sender and receiver halves.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        state: AtomicU8::new(INCOMPLETE),
        waker: AtomicWaker::new(),
        data:  UnsafeCell::new(MaybeUninit::uninit()),
    });

    (
        Sender {
            inner: Some(inner.clone()),
        },
        Receiver {
            inner: Some(inner)
        },
    )
}

impl<T> Sender<T> {
    /// Send a value across the channel.
    pub fn send(mut self, val: T) -> Result<(), T> {
        let inner = self.inner.take().unwrap();

        // Safety: We hold the TX side and the state is INCOMPLETE.
        // No other thread can read or write `data` right now.
        unsafe {
            (*inner.data.get()).write(val);
        }

        // Commit the write with Release ordering so the RX side sees the data via
        // Acquire.
        match inner
            .state
            .compare_exchange(INCOMPLETE, READY, Ordering::Release, Ordering::Relaxed)
        {
            | Ok(_) => {
                inner.waker.wake();
                Ok(())
            },
            | Err(RX_DROPPED) => {
                // The receiver dropped early. Extract our value back out.
                // Safety: We just wrote it, and RX is gone, so we have exclusive access.
                let val = unsafe { (*inner.data.get()).assume_init_read() };
                Err(val)
            },
            | _ => unreachable!(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            if inner
                .state
                .compare_exchange(INCOMPLETE, TX_DROPPED, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                inner.waker.wake();
            }
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safe because Receiver is implicitly Unpin
        let this = unsafe { self.get_unchecked_mut() };

        let inner_ref = this.inner.as_ref().expect("polled after completion");

        let state = inner_ref.state.load(Ordering::Acquire);
        if state == READY {
            let inner = this.inner.take().unwrap();
            let val = unsafe { (*inner.data.get()).assume_init_read() };
            inner.state.store(RX_DROPPED, Ordering::Release);
            return Poll::Ready(Ok(val));
        }
        if state == TX_DROPPED {
            this.inner.take();
            return Poll::Ready(Err(()));
        }

        inner_ref.waker.register(cx.waker());

        match inner_ref.state.load(Ordering::Acquire) {
            | READY => {
                let inner = this.inner.take().unwrap();
                let val = unsafe { (*inner.data.get()).assume_init_read() };
                inner.state.store(RX_DROPPED, Ordering::Release);
                Poll::Ready(Ok(val))
            },
            | TX_DROPPED => {
                this.inner.take();
                Poll::Ready(Err(()))
            },
            | INCOMPLETE => Poll::Pending,
            | RX_DROPPED => unreachable!(),
            | _ => unreachable!(),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // AcqRel ensures we sync with the Sender's Release if they sent data.
            let prev = inner.state.swap(RX_DROPPED, Ordering::AcqRel);

            if prev == READY {
                // Sender sent the data, but we are dropping without reading it.
                // Safety: We acquired the memory barriers and are the final owners.
                unsafe {
                    (*inner.data.get()).assume_init_drop();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc as StdArc,
            atomic::{
                AtomicUsize,
                Ordering,
            },
        },
        time::Duration,
    };

    use super::*;

    #[tokio::test]
    async fn successful_send_receive() {
        let (tx, rx) = channel::<u32>();
        assert!(tx.send(42).is_ok());
        let result = rx.await;
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn tx_dropped_early() {
        let (tx, rx) = channel::<u32>();
        drop(tx);
        let result = rx.await;
        assert_eq!(result, Err(()));
    }

    #[tokio::test]
    async fn rx_dropped_early() {
        let (tx, rx) = channel::<u32>();
        drop(rx);
        let result = tx.send(42);
        // Expecting the value back as an error because rx is closed
        assert_eq!(result, Err(42));
    }

    #[tokio::test]
    async fn async_send_receive() {
        let (tx, rx) = channel::<String>();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = tx.send("hello".to_string());
        });

        let result = rx.await;
        assert_eq!(result, Ok("hello".to_string()));
    }

    #[tokio::test]
    async fn rx_drop_cleans_up_data() {
        // Tracking structure to verify data is dropped properly
        // if sent but never read by the receiver
        struct TrackDrop(StdArc<AtomicUsize>);

        impl Drop for TrackDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drop_count = StdArc::new(AtomicUsize::new(0));
        let (tx, rx) = channel::<TrackDrop>();

        // Send the data
        assert!(tx.send(TrackDrop(drop_count.clone())).is_ok());

        // Receiver drops without awaiting
        drop(rx);

        // Data should be cleaned up by the receiver's Drop implementation
        assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    }
}
