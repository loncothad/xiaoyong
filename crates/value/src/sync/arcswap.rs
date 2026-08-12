//! Lock-free reads of atomically swappable reference-counted values.

use core::{
    array,
    hint,
    marker::PhantomData,
    ptr,
    sync::atomic::{
        AtomicBool,
        AtomicPtr,
        AtomicUsize,
        Ordering,
    },
};

use triomphe::{
    Arc,
    ArcBorrow,
};

const EPOCHS: usize = 2;

/// A thread-safe container whose current [`Arc`] value can be replaced.
///
/// Loads never acquire a lock. A short reader epoch keeps the loaded raw
/// pointer alive only until its `Arc` reference count has been incremented.
/// Writers are serialized with an atomic compare-and-exchange and reclaim the
/// replaced pointer after readers from the preceding epoch have left.
///
/// This is optimized for read-heavy workloads: a load performs no allocation,
/// cannot block, and only touches the epoch counters and the `Arc` reference
/// count.
pub struct ArcSwap<T> {
    pointer:       AtomicPtr<T>,
    epoch:         AtomicUsize,
    readers:       [AtomicUsize; EPOCHS],
    writer_active: AtomicBool,
    // Propagate the Send/Sync bounds of the Arc whose ownership is represented
    // by `pointer`.
    ownership:     PhantomData<Arc<T>>,
}

impl<T> ArcSwap<T> {
    /// Creates a container with `value` as its initial snapshot.
    #[must_use]
    pub fn new(value: Arc<T>) -> Self {
        Self {
            pointer:       AtomicPtr::new(Arc::into_raw(value).cast_mut()),
            epoch:         AtomicUsize::new(0),
            readers:       array::from_fn(|_| AtomicUsize::new(0)),
            writer_active: AtomicBool::new(false),
            ownership:     PhantomData,
        }
    }

    /// Returns a snapshot of the current value.
    ///
    /// This operation is lock-free.
    #[must_use]
    pub fn load(&self) -> Arc<T> {
        let reader = self.enter_reader();
        let pointer = self.pointer.load(Ordering::Acquire);
        debug_assert!(!pointer.is_null());

        // SAFETY: `pointer` originated in `Arc::into_raw` and retains full
        // allocation provenance. The reader epoch keeps that allocation alive
        // until `clone_arc` has incremented its reference count.
        let snapshot = unsafe { ArcBorrow::from_ptr(pointer) }.clone_arc();
        drop(reader);
        snapshot
    }

    /// Replaces the current value and drops the previous snapshot.
    pub fn store(&self, value: Arc<T>) {
        drop(self.swap(value));
    }

    /// Replaces the current value and returns the previous snapshot.
    ///
    /// Loads remain lock-free while this operation waits for any reader that
    /// was cloning the previous raw pointer.
    pub fn swap(&self, value: Arc<T>) -> Arc<T> {
        let new_pointer = Arc::into_raw(value).cast_mut();
        let writer = self.enter_writer();
        let old_pointer = self.pointer.swap(new_pointer, Ordering::AcqRel);

        // Readers validate the epoch after registering. Once this flips, no
        // new reader can successfully enter `old_epoch`; readers that raced the
        // flip unregister and retry in the new epoch.
        let old_epoch = self.epoch.fetch_xor(1, Ordering::SeqCst) & 1;
        while self.readers[old_epoch].load(Ordering::SeqCst) != 0 {
            hint::spin_loop();
        }

        // SAFETY: the pointer came from `Arc::into_raw`. All readers that could
        // have observed it have left their epoch, and the writer guard prevents
        // another writer from reclaiming it concurrently.
        let old_value = unsafe { Arc::from_raw(old_pointer) };
        drop(writer);
        old_value
    }

    fn enter_reader(&self) -> ReaderGuard<'_, T> {
        loop {
            let epoch = self.epoch.load(Ordering::SeqCst) & 1;
            let previous = self.readers[epoch].fetch_add(1, Ordering::SeqCst);
            // Match reference-counted pointer practice: abort well before the
            // counter could wrap, since wrapping would make reclamation unsound.
            if previous > isize::MAX as usize {
                std::process::abort();
            }

            if self.epoch.load(Ordering::SeqCst) & 1 == epoch {
                return ReaderGuard {
                    swap: self,
                    epoch,
                };
            }
            self.readers[epoch].fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn enter_writer(&self) -> WriterGuard<'_> {
        while self
            .writer_active
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            hint::spin_loop();
        }
        WriterGuard {
            active: &self.writer_active,
        }
    }
}

impl<T> Drop for ArcSwap<T> {
    fn drop(&mut self) {
        let pointer = *self.pointer.get_mut();
        if !pointer.is_null() {
            // SAFETY: `&mut self` proves no load or swap can be active, and this
            // is the sole strong reference represented by the raw pointer.
            unsafe { drop(Arc::from_raw(pointer)) };
            *self.pointer.get_mut() = ptr::null_mut();
        }
    }
}

struct ReaderGuard<'a, T> {
    swap:  &'a ArcSwap<T>,
    epoch: usize,
}

impl<T> Drop for ReaderGuard<'_, T> {
    fn drop(&mut self) {
        self.swap.readers[self.epoch].fetch_sub(1, Ordering::SeqCst);
    }
}

struct WriterGuard<'a> {
    active: &'a AtomicBool,
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

impl<T: Default> Default for ArcSwap<T> {
    fn default() -> Self {
        Self::new(Arc::new(T::default()))
    }
}

impl<T> From<Arc<T>> for ArcSwap<T> {
    fn from(value: Arc<T>) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc as StdArc,
            atomic::{
                AtomicBool,
                AtomicUsize,
                Ordering,
            },
        },
        thread,
    };

    use super::*;

    #[test]
    fn snapshots_remain_valid_after_swap() {
        let swap = ArcSwap::new(Arc::new(String::from("old")));
        let snapshot = swap.load();
        let old = swap.swap(Arc::new(String::from("new")));

        assert_eq!(&*snapshot, "old");
        assert_eq!(&*old, "old");
        assert_eq!(&*swap.load(), "new");
    }

    #[test]
    fn concurrent_readers_and_writers_observe_complete_values() {
        let swap = StdArc::new(ArcSwap::new(Arc::new((0_u64, 0_u64))));
        let running = StdArc::new(AtomicBool::new(true));
        let writes_per_writer = if cfg!(miri) {
            20
        } else {
            2_000
        };
        let mut readers = Vec::new();
        for _ in 0 .. 4 {
            let swap = StdArc::clone(&swap);
            let running = StdArc::clone(&running);
            readers.push(thread::spawn(move || {
                while running.load(Ordering::Acquire) {
                    let snapshot = swap.load();
                    assert_eq!(snapshot.1, snapshot.0.wrapping_mul(2));
                }
            }));
        }

        let mut writers = Vec::new();
        for writer_id in 0 .. 2 {
            let swap = StdArc::clone(&swap);
            writers.push(thread::spawn(move || {
                for offset in 1 ..= writes_per_writer {
                    let value = writer_id * writes_per_writer + offset;
                    swap.store(Arc::new((value, value.wrapping_mul(2))));
                }
            }));
        }
        for writer in writers {
            assert!(writer.join().is_ok());
        }
        running.store(false, Ordering::Release);
        for reader in readers {
            assert!(reader.join().is_ok());
        }
    }

    #[test]
    fn container_releases_exactly_one_owned_reference() {
        struct TrackDrop(StdArc<AtomicUsize>);
        impl Drop for TrackDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = StdArc::new(AtomicUsize::new(0));
        let swap = ArcSwap::new(Arc::new(TrackDrop(StdArc::clone(&drops))));
        let snapshot = swap.load();
        swap.store(Arc::new(TrackDrop(StdArc::clone(&drops))));
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(snapshot);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(swap);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }
}
