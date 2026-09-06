//! Single-threaded, single-use channel for transferring a single value.

use std::{
    cell::UnsafeCell,
    mem,
    pin::Pin,
    ptr,
    rc::Rc,
    task::{
        Context,
        Poll,
        Waker,
    },
};

/// Creates a new bounded channel, returning the sender and receiver halves.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let state = Rc::new(UnsafeCell::new(State::Incomplete {
        waker: None
    }));
    (
        Sender {
            inner: Rc::clone(&state),
        },
        Receiver {
            inner: state
        },
    )
}

enum State<T> {
    Incomplete { waker: Option<Waker> },
    Complete(T),
    Canceled,
}

/// Error returned when attempting to receive a value outside an async context.
#[derive(Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is still open, but no value has been sent yet.
    Empty,

    /// The sender was dropped before sending a value.
    Canceled,
}

/// A single-threaded receiver for the oneshot channel.
///
/// **Thread Safety:** This type uses Rc internally and is explicitly !Send.
pub struct Receiver<T> {
    inner: Rc<UnsafeCell<State<T>>>,
}

impl<T> Receiver<T> {
    /// Attempts to receive a value outside of an async context.
    pub fn try_get(&mut self) -> Result<T, TryRecvError> {
        let state_ptr = self.inner.get();

        // SAFETY: Single-threaded execution guarantees no data races.
        // We only replace the state if it's `Complete`. Replacing the state
        // with `Canceled` transfers ownership of `T` out of the cell without
        // running any external `Drop` or `Waker` code while the pointer is
        // dereferenced.
        unsafe {
            match &*state_ptr {
                | State::Complete(_) => {
                    match ptr::replace(state_ptr, State::Canceled) {
                        | State::Complete(val) => Ok(val),
                        | _ => unreachable!(),
                    }
                },
                | State::Incomplete {
                    ..
                } => Err(TryRecvError::Empty),
                | State::Canceled => Err(TryRecvError::Canceled),
            }
        }
    }
}

/// A single-threaded sender for the oneshot channel.
///
/// **Thread Safety:** This type uses Rc internally and is explicitly !Send.
pub struct Sender<T> {
    inner: Rc<UnsafeCell<State<T>>>,
}

impl<T> Sender<T> {
    /// Send a value across the channel.
    pub fn send(self, val: T) -> Result<(), T> {
        let state_ptr = self.inner.get();

        // SAFETY: The channel is single-threaded. We scope the mutable reference
        // tightly to prevent aliasing during potential re-entrancy from `wake()`.
        let waker = unsafe {
            let state = &mut *state_ptr;
            if let State::Canceled = state {
                return Err(val);
            }

            let old_state = ptr::replace(state_ptr, State::Complete(val));
            match old_state {
                | State::Incomplete {
                    waker,
                } => waker,
                | _ => unreachable!(),
            }
        };

        if let Some(w) = waker {
            w.wake();
        }

        Ok(())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let state_ptr = self.inner.get();

        // SAFETY: the channel is single-threaded. The state is replaced before
        // invoking the extracted waker, preventing aliased re-entrant access.
        let waker = unsafe {
            let state = &mut *state_ptr;
            if matches!(state, State::Incomplete { .. }) {
                match ptr::replace(state_ptr, State::Canceled) {
                    | State::Incomplete {
                        waker,
                    } => waker,
                    | _ => unreachable!(),
                }
            } else {
                None
            }
        };

        if let Some(w) = waker {
            w.wake();
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
/// Error returned when the operation is canceled.
pub struct Canceled;

impl<T> Future for Receiver<T> {
    type Output = Result<T, Canceled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // A waker's clone callback can send or cancel the channel. Run it
        // before reading the state so that change cannot be missed.
        let new_waker = cx.waker().clone();
        let state_ptr = self.inner.get();
        let old_waker;
        // SAFETY: the channel is single-threaded. No user code runs while
        // inspecting or replacing the state, and old wakers drop afterwards.
        unsafe {
            match &mut *state_ptr {
                | State::Complete(_) => {
                    if let State::Complete(value) = ptr::replace(state_ptr, State::Canceled) {
                        return Poll::Ready(Ok(value));
                    }
                    unreachable!()
                },
                | State::Canceled => return Poll::Ready(Err(Canceled)),
                | State::Incomplete {
                    waker,
                } => old_waker = waker.replace(new_waker),
            }
        }
        drop(old_waker);
        Poll::Pending
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // SAFETY: We replace the state with Canceled. The old state (which may contain
        // `T`) is returned and immediately dropped at the end of the statement,
        // outside of any internal unsafe references, preventing UB if `T::drop`
        // re-enters the channel.
        unsafe {
            mem::drop(ptr::replace(self.inner.get(), State::Canceled));
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn unsync_oneshot() {
        let local = task::LocalSet::new();
        local
            .run_until(async move {
                let (tx, rx) = channel::<i32>();

                task::spawn_local(async move {
                    tx.send(42).unwrap();
                });

                let val = rx.await.unwrap();
                assert_eq!(val, 42);
            })
            .await;
    }

    #[test]
    fn send_during_waker_clone_is_observed() {
        use std::{
            cell::RefCell,
            task::{
                RawWaker,
                RawWakerVTable,
            },
        };
        thread_local! {
            static ON_CLONE: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
        }
        fn raw_waker() -> RawWaker {
            fn clone(_: *const ()) -> RawWaker {
                let callback = ON_CLONE.with(|slot| slot.borrow_mut().take());
                if let Some(callback) = callback {
                    callback();
                }
                raw_waker()
            }
            fn noop(_: *const ()) {}
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        let (sender, mut receiver) = channel();
        ON_CLONE.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                sender.send(42).unwrap();
            }))
        });
        // SAFETY: the static vtable never dereferences its null data, owns no
        // resources, and accesses callbacks only through thread-local storage.
        let waker = unsafe { Waker::from_raw(raw_waker()) };
        let mut cx = Context::from_waker(&waker);
        assert_eq!(Pin::new(&mut receiver).poll(&mut cx), Poll::Ready(Ok(42)));
    }
}
