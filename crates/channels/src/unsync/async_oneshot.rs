//! A single-threaded, single-use channel for transferring a single value.

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

#[derive(Debug, PartialEq, Eq)]
/// Error returned when attempting to receive a value outside an async context.
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
        let state_ptr = self.inner.get();

        let old_waker: Option<Waker>;

        // Inspect state and extract waker if incomplete.
        unsafe {
            let state = &mut *state_ptr;
            match state {
                | State::Complete(_) => {
                    // Extract value and set to Canceled to avoid re-polling issues.
                    if let State::Complete(val) = ptr::replace(state_ptr, State::Canceled) {
                        return Poll::Ready(Ok(val));
                    }
                    unreachable!()
                },
                | State::Canceled => return Poll::Ready(Err(Canceled)),
                | State::Incomplete {
                    waker,
                } => {
                    // Take ownership of the waker to evaluate `will_wake` safely outside the
                    // borrow.
                    old_waker = waker.take();
                },
            }
        }

        // Evaluate waker (safe from re-entrancy UB).
        let new_waker = cx.waker();
        let waker_to_store = match old_waker {
            | Some(w) if w.will_wake(new_waker) => w,
            | _ => new_waker.clone(),
        };

        // Put the waker back if the state is still Incomplete.
        unsafe {
            let state = &mut *state_ptr;
            if let State::Incomplete {
                waker,
            } = state
            {
                *waker = Some(waker_to_store);
            }
        }

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
}
