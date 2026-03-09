//! Single-threaded, reusable notification primitive.

use std::{
    cell::Cell,
    pin::Pin,
    rc::Rc,
    task::{
        Context,
        Poll,
        Waker,
    },
};

use smallvec::SmallVec;

/// Single-threaded, reusable notification primitive.
///
/// **Thread Safety:** This type is designed for single-threaded executors and
/// is !Send.
pub struct Notify {
    generation: Cell<usize>,
    permit:     Cell<bool>,
    waiters:    Cell<SmallVec<[Option<Waker>; 8]>>,
}

impl Notify {
    /// Creates a new instance.
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            generation: Cell::new(0),
            permit:     Cell::new(false),
            waiters:    Cell::new(SmallVec::new()),
        })
    }

    /// Triggers the notification, waking up tasks.
    pub fn notify(&self) {
        self.generation.set(self.generation.get().wrapping_add(1));
        self.permit.set(true);

        let mut waiters = self.waiters.take();
        for waker in waiters.drain(..) {
            if let Some(w) = waker {
                w.wake();
            }
        }
        self.waiters.set(waiters);
    }

    /// Returns a future that resolves when the notification is triggered.
    pub fn wait(self: &Rc<Self>) -> WaitFuture {
        WaitFuture {
            notify:           Rc::clone(self),
            start_generation: self.generation.get(),
            index:            None,
        }
    }
}

/// Future that resolves when the notification is triggered.
pub struct WaitFuture {
    notify:           Rc<Notify>,
    start_generation: usize,
    index:            Option<usize>,
}

impl Future for WaitFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.notify.generation.get() != self.start_generation {
            self.notify.permit.set(false);
            return Poll::Ready(());
        }

        if self.notify.permit.get() {
            self.notify.permit.set(false);
            return Poll::Ready(());
        }

        let mut waiters = self.notify.waiters.take();
        if let Some(idx) = self.index {
            waiters[idx] = Some(cx.waker().clone());
        } else if let Some(idx) = waiters.iter().position(|w| w.is_none()) {
            // Reclaim a dropped future's slot to prevent heap spilling
            waiters[idx] = Some(cx.waker().clone());
            self.index = Some(idx);
        } else {
            let idx = waiters.len();
            waiters.push(Some(cx.waker().clone()));
            self.index = Some(idx);
        }
        self.notify.waiters.set(waiters);

        Poll::Pending
    }
}

impl Drop for WaitFuture {
    fn drop(&mut self) {
        if let Some(idx) = self.index {
            let mut waiters = self.notify.waiters.take();
            if idx < waiters.len() {
                waiters[idx] = None;
            }
            self.notify.waiters.set(waiters);
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::task;

    use super::*;

    #[tokio::test]
    async fn notify_reusable() {
        let local = task::LocalSet::new();
        local
            .run_until(async move {
                let notify = Notify::new();

                let n1 = Rc::clone(&notify);
                task::spawn_local(async move {
                    n1.notify();
                });
                notify.wait().await;

                let n2 = Rc::clone(&notify);
                task::spawn_local(async move {
                    n2.notify();
                });
                notify.wait().await;
            })
            .await;
    }
}
