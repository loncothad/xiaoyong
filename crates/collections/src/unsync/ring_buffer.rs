//! An inline, fixed-capacity ring buffer.

use core::{
    fmt,
    iter::{
        Chain,
        FusedIterator,
    },
    mem::MaybeUninit,
    ops::{
        Index,
        IndexMut,
    },
    slice,
};

pub use super::array::CapacityError;

/// Borrowing iterator over a [`RingBuffer`].
pub type Iter<'a, T> = Chain<slice::Iter<'a, T>, slice::Iter<'a, T>>;

/// Mutably borrowing iterator over a [`RingBuffer`].
pub type IterMut<'a, T> = Chain<slice::IterMut<'a, T>, slice::IterMut<'a, T>>;

/// A double-ended queue that stores up to `CAPACITY` elements inline.
///
/// Elements are stored in insertion order without allocating. Pushing and
/// popping at either end takes constant time. Because the initialized region
/// can wrap around the backing array, [`as_slices`](Self::as_slices) exposes
/// its contents as two ordered slices.
pub struct RingBuffer<T, const CAPACITY: usize> {
    data: [MaybeUninit<T>; CAPACITY],
    head: usize,
    len:  usize,
}

impl<T, const CAPACITY: usize> RingBuffer<T, CAPACITY> {
    /// Creates an empty ring buffer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: core::array::from_fn(|_| MaybeUninit::uninit()),
            head: 0,
            len:  0,
        }
    }

    /// Returns the number of elements in the buffer.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns the maximum number of elements the buffer can hold.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        CAPACITY
    }

    /// Returns `true` when the buffer contains no elements.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns `true` when the buffer cannot accept another element.
    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.len == CAPACITY
    }

    /// Appends an element, returning it when the buffer is full.
    pub fn try_push_back(&mut self, value: T) -> Result<(), CapacityError<T>> {
        if self.is_full() {
            return Err(CapacityError::new(value));
        }

        let index = self.physical_index(self.len);
        self.data[index].write(value);
        self.len += 1;
        Ok(())
    }

    /// Appends an element.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is full.
    pub fn push_back(&mut self, value: T) {
        assert!(!self.is_full(), "fixed-capacity ring buffer is full");
        let index = self.physical_index(self.len);
        self.data[index].write(value);
        self.len += 1;
    }

    /// Prepends an element, returning it when the buffer is full.
    pub fn try_push_front(&mut self, value: T) -> Result<(), CapacityError<T>> {
        if self.is_full() {
            return Err(CapacityError::new(value));
        }

        self.head = self.previous_index(self.head);
        self.data[self.head].write(value);
        self.len += 1;
        Ok(())
    }

    /// Prepends an element.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is full.
    pub fn push_front(&mut self, value: T) {
        assert!(!self.is_full(), "fixed-capacity ring buffer is full");
        self.head = self.previous_index(self.head);
        self.data[self.head].write(value);
        self.len += 1;
    }

    /// Appends an element and returns the oldest element if it was displaced.
    ///
    /// For a zero-capacity buffer, the supplied value is returned unchanged.
    pub fn push_back_overwrite(&mut self, value: T) -> Option<T> {
        if CAPACITY == 0 {
            return Some(value);
        }
        if !self.is_full() {
            self.push_back(value);
            return None;
        }

        let index = self.head;
        self.head = self.next_index(self.head);
        // SAFETY: a full buffer has an initialized element at `head`. It is
        // moved out before the replacement value initializes the same slot.
        let replaced = unsafe { self.data[index].assume_init_read() };
        self.data[index].write(value);
        Some(replaced)
    }

    /// Prepends an element and returns the newest element if it was displaced.
    ///
    /// For a zero-capacity buffer, the supplied value is returned unchanged.
    pub fn push_front_overwrite(&mut self, value: T) -> Option<T> {
        if CAPACITY == 0 {
            return Some(value);
        }
        if !self.is_full() {
            self.push_front(value);
            return None;
        }

        self.head = self.previous_index(self.head);
        // SAFETY: when full, the slot immediately before `head` is the
        // initialized back element. It is moved out and immediately replaced.
        let replaced = unsafe { self.data[self.head].assume_init_read() };
        self.data[self.head].write(value);
        Some(replaced)
    }

    /// Removes and returns the first element, or `None` when empty.
    pub fn pop_front(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        let index = self.head;
        self.head = self.next_index(self.head);
        self.len -= 1;
        if self.len == 0 {
            self.head = 0;
        }
        // SAFETY: the old `head` was initialized and the buffer state was
        // updated first, so the moved value cannot be dropped twice.
        Some(unsafe { self.data[index].assume_init_read() })
    }

    /// Removes and returns the last element, or `None` when empty.
    pub fn pop_back(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        self.len -= 1;
        let index = self.physical_index(self.len);
        if self.len == 0 {
            self.head = 0;
        }
        // SAFETY: the old back slot was initialized and `len` was reduced
        // first, so the moved value cannot be dropped twice.
        Some(unsafe { self.data[index].assume_init_read() })
    }

    /// Returns a reference to the first element.
    #[must_use]
    pub fn front(&self) -> Option<&T> {
        self.get(0)
    }

    /// Returns a mutable reference to the first element.
    #[must_use]
    pub fn front_mut(&mut self) -> Option<&mut T> {
        self.get_mut(0)
    }

    /// Returns a reference to the last element.
    #[must_use]
    pub fn back(&self) -> Option<&T> {
        self.len.checked_sub(1).and_then(|index| self.get(index))
    }

    /// Returns a mutable reference to the last element.
    #[must_use]
    pub fn back_mut(&mut self) -> Option<&mut T> {
        self.len.checked_sub(1).and_then(|index| self.get_mut(index))
    }

    /// Returns a reference to the element at the logical `index`.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        let index = self.physical_index(index);
        // SAFETY: logical indices below `len` map to initialized slots.
        Some(unsafe { self.data[index].assume_init_ref() })
    }

    /// Returns a mutable reference to the element at the logical `index`.
    #[must_use]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if index >= self.len {
            return None;
        }
        let index = self.physical_index(index);
        // SAFETY: logical indices below `len` map to initialized slots, and
        // `&mut self` guarantees exclusive access.
        Some(unsafe { self.data[index].assume_init_mut() })
    }

    /// Returns the buffer contents as two slices in logical order.
    ///
    /// The second slice is empty when the initialized region has not wrapped.
    #[must_use]
    pub fn as_slices(&self) -> (&[T], &[T]) {
        let first_len = self.first_slice_len();
        let second_len = self.len - first_len;
        // SAFETY: `head..head + first_len` and `0..second_len` are the two
        // disjoint initialized regions. Empty slices are also valid here.
        unsafe {
            (
                slice::from_raw_parts(self.data.as_ptr().add(self.head).cast::<T>(), first_len),
                slice::from_raw_parts(self.data.as_ptr().cast::<T>(), second_len),
            )
        }
    }

    /// Returns the buffer contents as two mutable slices in logical order.
    ///
    /// The second slice is empty when the initialized region has not wrapped.
    #[must_use]
    pub fn as_mut_slices(&mut self) -> (&mut [T], &mut [T]) {
        let first_len = self.first_slice_len();
        let second_len = self.len - first_len;
        let (before_head, from_head) = self.data.split_at_mut(self.head);
        // SAFETY: the slices come from disjoint parts of the backing array and
        // cover only their respective initialized prefixes.
        unsafe {
            (
                slice::from_raw_parts_mut(from_head.as_mut_ptr().cast::<T>(), first_len),
                slice::from_raw_parts_mut(before_head.as_mut_ptr().cast::<T>(), second_len),
            )
        }
    }

    /// Returns an iterator over elements from front to back.
    pub fn iter(&self) -> Iter<'_, T> {
        let (first, second) = self.as_slices();
        first.iter().chain(second)
    }

    /// Returns a mutable iterator over elements from front to back.
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        let (first, second) = self.as_mut_slices();
        first.iter_mut().chain(second)
    }

    /// Removes all elements from the buffer.
    pub fn clear(&mut self) {
        let guard = ClearGuard(self);
        while let Some(value) = guard.0.pop_front() {
            drop(value);
        }
    }

    /// Appends an element, returning it when the buffer is full.
    pub fn try_push(&mut self, value: T) -> Result<(), CapacityError<T>> {
        self.try_push_back(value)
    }

    /// Appends an element.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is full.
    pub fn push(&mut self, value: T) {
        self.push_back(value);
    }

    /// Appends an element and returns the oldest element if it was displaced.
    pub fn push_overwrite(&mut self, value: T) -> Option<T> {
        self.push_back_overwrite(value)
    }

    /// Removes and returns the first element, or `None` when empty.
    pub fn pop(&mut self) -> Option<T> {
        self.pop_front()
    }

    fn first_slice_len(&self) -> usize {
        self.len.min(CAPACITY - self.head)
    }

    fn physical_index(&self, logical_index: usize) -> usize {
        debug_assert!(CAPACITY != 0);
        debug_assert!(logical_index < CAPACITY);
        let distance_to_end = CAPACITY - self.head;
        if logical_index >= distance_to_end {
            logical_index - distance_to_end
        } else {
            self.head + logical_index
        }
    }

    fn next_index(&self, index: usize) -> usize {
        debug_assert!(CAPACITY != 0);
        if index + 1 == CAPACITY {
            0
        } else {
            index + 1
        }
    }

    fn previous_index(&self, index: usize) -> usize {
        debug_assert!(CAPACITY != 0);
        if index == 0 {
            CAPACITY - 1
        } else {
            index - 1
        }
    }
}

struct ClearGuard<'a, T, const CAPACITY: usize>(&'a mut RingBuffer<T, CAPACITY>);

impl<T, const CAPACITY: usize> Drop for ClearGuard<'_, T, CAPACITY> {
    fn drop(&mut self) {
        while let Some(value) = self.0.pop_front() {
            drop(value);
        }
    }
}

impl<T, const CAPACITY: usize> Default for RingBuffer<T, CAPACITY> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone, const CAPACITY: usize> Clone for RingBuffer<T, CAPACITY> {
    fn clone(&self) -> Self {
        self.iter().cloned().collect()
    }
}

impl<T: fmt::Debug, const CAPACITY: usize> fmt::Debug for RingBuffer<T, CAPACITY> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self).finish()
    }
}

impl<T, const CAPACITY: usize> Drop for RingBuffer<T, CAPACITY> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<T, const CAPACITY: usize> Index<usize> for RingBuffer<T, CAPACITY> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("ring buffer index out of bounds")
    }
}

impl<T, const CAPACITY: usize> IndexMut<usize> for RingBuffer<T, CAPACITY> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index).expect("ring buffer index out of bounds")
    }
}

impl<'a, T, const CAPACITY: usize> IntoIterator for &'a RingBuffer<T, CAPACITY> {
    type IntoIter = Iter<'a, T>;
    type Item = &'a T;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T, const CAPACITY: usize> IntoIterator for &'a mut RingBuffer<T, CAPACITY> {
    type IntoIter = IterMut<'a, T>;
    type Item = &'a mut T;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// Owning iterator over a [`RingBuffer`].
pub struct IntoIter<T, const CAPACITY: usize> {
    buffer: RingBuffer<T, CAPACITY>,
}

impl<T, const CAPACITY: usize> Iterator for IntoIter<T, CAPACITY> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.buffer.pop_front()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.buffer.len();
        (len, Some(len))
    }
}

impl<T, const CAPACITY: usize> DoubleEndedIterator for IntoIter<T, CAPACITY> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.buffer.pop_back()
    }
}

impl<T, const CAPACITY: usize> ExactSizeIterator for IntoIter<T, CAPACITY> {}
impl<T, const CAPACITY: usize> FusedIterator for IntoIter<T, CAPACITY> {}

impl<T, const CAPACITY: usize> IntoIterator for RingBuffer<T, CAPACITY> {
    type IntoIter = IntoIter<T, CAPACITY>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            buffer: self
        }
    }
}

impl<T, const CAPACITY: usize> Extend<T> for RingBuffer<T, CAPACITY> {
    /// Appends every item from the iterator.
    ///
    /// # Panics
    ///
    /// Panics if the iterator contains more items than the remaining capacity.
    fn extend<I: IntoIterator<Item = T>>(&mut self, values: I) {
        for value in values {
            self.push_back(value);
        }
    }
}

impl<T, const CAPACITY: usize> FromIterator<T> for RingBuffer<T, CAPACITY> {
    /// Collects all items into a ring buffer.
    ///
    /// # Panics
    ///
    /// Panics if the iterator contains more than `CAPACITY` items.
    fn from_iter<I: IntoIterator<Item = T>>(values: I) -> Self {
        let mut buffer = Self::new();
        buffer.extend(values);
        buffer
    }
}

impl<T: PartialEq, const CAPACITY: usize, const OTHER_CAPACITY: usize> PartialEq<RingBuffer<T, OTHER_CAPACITY>>
    for RingBuffer<T, CAPACITY>
{
    fn eq(&self, other: &RingBuffer<T, OTHER_CAPACITY>) -> bool {
        self.iter().eq(other)
    }
}

impl<T: Eq, const CAPACITY: usize> Eq for RingBuffer<T, CAPACITY> {}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{
        AtomicUsize,
        Ordering,
    };
    use std::{
        collections::VecDeque,
        panic::{
            AssertUnwindSafe,
            catch_unwind,
        },
        string::String,
        sync::Arc,
        vec::Vec,
    };

    use super::*;

    #[test]
    fn fifo_operations_wrap_and_preserve_order() {
        let mut buffer = RingBuffer::<_, 4>::new();
        buffer.push(1);
        buffer.push(2);
        buffer.push(3);
        buffer.push(4);
        assert_eq!(buffer.pop(), Some(1));
        assert_eq!(buffer.pop(), Some(2));
        buffer.push(5);
        buffer.push(6);

        assert_eq!(buffer.as_slices(), (&[3, 4][..], &[5, 6][..]));
        assert_eq!(buffer.iter().copied().collect::<Vec<_>>(), [3, 4, 5, 6]);
        assert_eq!(buffer.into_iter().collect::<Vec<_>>(), [3, 4, 5, 6]);
    }

    #[test]
    fn supports_operations_at_both_ends() {
        let mut buffer = RingBuffer::<_, 4>::new();
        buffer.push_back(2);
        buffer.push_front(1);
        buffer.push_back(3);

        assert_eq!(buffer.front(), Some(&1));
        assert_eq!(buffer.back(), Some(&3));
        assert_eq!(buffer.pop_back(), Some(3));
        assert_eq!(buffer.pop_front(), Some(1));
        assert_eq!(buffer.pop_front(), Some(2));
        assert!(buffer.is_empty());
        assert_eq!(buffer.pop_back(), None);
    }

    #[test]
    fn failed_push_returns_ownership() {
        let mut buffer = RingBuffer::<String, 1>::new();
        buffer.push(String::from("first"));
        let result = buffer.try_push(String::from("second"));
        assert_eq!(result.map_err(CapacityError::into_inner), Err(String::from("second")));
    }

    #[test]
    fn overwrite_operations_return_displaced_values() {
        let mut buffer: RingBuffer<_, 3> = [1, 2, 3].into_iter().collect();
        assert_eq!(buffer.push_back_overwrite(4), Some(1));
        assert_eq!(buffer.iter().copied().collect::<Vec<_>>(), [2, 3, 4]);
        assert_eq!(buffer.push_front_overwrite(1), Some(4));
        assert_eq!(buffer.iter().copied().collect::<Vec<_>>(), [1, 2, 3]);
    }

    #[test]
    fn mutable_access_uses_logical_order_after_wrapping() {
        let mut buffer: RingBuffer<_, 3> = [1, 2, 3].into_iter().collect();
        assert_eq!(buffer.pop_front(), Some(1));
        buffer.push_back(4);

        buffer[1] = 30;
        for value in &mut buffer {
            *value *= 2;
        }
        assert_eq!(buffer.iter().copied().collect::<Vec<_>>(), [4, 60, 8]);
        assert_eq!(buffer.get(3), None);
    }

    #[test]
    fn owning_iterator_supports_both_ends() {
        let buffer: RingBuffer<_, 4> = [1, 2, 3, 4].into_iter().collect();
        let mut iterator = buffer.into_iter();
        assert_eq!(iterator.next(), Some(1));
        assert_eq!(iterator.next_back(), Some(4));
        assert_eq!(iterator.len(), 2);
        assert_eq!(iterator.collect::<Vec<_>>(), [2, 3]);
    }

    #[test]
    fn drops_each_element_exactly_once() {
        struct TrackDrop(Arc<AtomicUsize>);
        impl Drop for TrackDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let mut buffer = RingBuffer::<TrackDrop, 3>::new();
        for _ in 0 .. 3 {
            buffer.push(TrackDrop(Arc::clone(&dropped)));
        }
        drop(buffer.push_overwrite(TrackDrop(Arc::clone(&dropped))));
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        drop(buffer.pop_back());
        assert_eq!(dropped.load(Ordering::Relaxed), 2);
        drop(buffer);
        assert_eq!(dropped.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn clear_drops_remaining_elements_if_a_destructor_panics() {
        struct TrackDrop {
            dropped: Arc<AtomicUsize>,
            panic:   bool,
        }
        impl Drop for TrackDrop {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                assert!(!self.panic, "intentional destructor panic");
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let mut buffer = RingBuffer::<TrackDrop, 3>::new();
        buffer.push(TrackDrop {
            dropped: Arc::clone(&dropped),
            panic:   true,
        });
        for _ in 0 .. 2 {
            buffer.push(TrackDrop {
                dropped: Arc::clone(&dropped),
                panic:   false,
            });
        }

        let result = catch_unwind(AssertUnwindSafe(|| buffer.clear()));
        assert!(result.is_err());
        assert!(buffer.is_empty());
        assert_eq!(dropped.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn mixed_operations_match_vec_deque() {
        let mut buffer = RingBuffer::<u32, 7>::new();
        let mut model = VecDeque::with_capacity(7);
        let mut random = 0x7A5B_C2D3_u32;

        for _ in 0 .. 10_000 {
            random = random.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let value = random >> 8;
            match random & 7 {
                | 0 | 1 => {
                    let actual = buffer.try_push_back(value).map_err(CapacityError::into_inner);
                    let expected = if model.len() == 7 {
                        Err(value)
                    } else {
                        model.push_back(value);
                        Ok(())
                    };
                    assert_eq!(actual, expected);
                },
                | 2 => {
                    let actual = buffer.try_push_front(value).map_err(CapacityError::into_inner);
                    let expected = if model.len() == 7 {
                        Err(value)
                    } else {
                        model.push_front(value);
                        Ok(())
                    };
                    assert_eq!(actual, expected);
                },
                | 3 => assert_eq!(buffer.pop_front(), model.pop_front()),
                | 4 => assert_eq!(buffer.pop_back(), model.pop_back()),
                | 5 => {
                    let actual = buffer.push_back_overwrite(value);
                    let expected = if model.len() == 7 {
                        model.pop_front()
                    } else {
                        None
                    };
                    model.push_back(value);
                    assert_eq!(actual, expected);
                },
                | 6 => {
                    let actual = buffer.push_front_overwrite(value);
                    let expected = if model.len() == 7 {
                        model.pop_back()
                    } else {
                        None
                    };
                    model.push_front(value);
                    assert_eq!(actual, expected);
                },
                | _ => {
                    if let Some(front) = buffer.front_mut() {
                        *front ^= value;
                        *model.front_mut().expect("models have equal lengths") ^= value;
                    }
                },
            }

            assert_eq!(buffer.len(), model.len());
            assert!(buffer.iter().copied().eq(model.iter().copied()));
            let (first, second) = buffer.as_slices();
            assert_eq!(first.len() + second.len(), buffer.len());
        }
    }

    #[test]
    fn zero_capacity_is_supported() {
        let mut buffer = RingBuffer::<u8, 0>::new();
        assert!(buffer.is_empty());
        assert!(buffer.is_full());
        assert_eq!(buffer.as_slices(), (&[][..], &[][..]));
        assert_eq!(buffer.try_push(1).map_err(CapacityError::into_inner), Err(1));
        assert_eq!(buffer.push_overwrite(2), Some(2));
        assert_eq!(buffer.pop(), None);
    }

    #[test]
    fn clone_debug_and_equality_follow_logical_order() {
        let mut wrapped: RingBuffer<_, 3> = [1, 2, 3].into_iter().collect();
        assert_eq!(wrapped.pop(), Some(1));
        wrapped.push(4);
        let clone = wrapped.clone();

        assert_eq!(wrapped, clone);
        assert_eq!(wrapped, [2, 3, 4].into_iter().collect::<RingBuffer<_, 4>>());
        assert_eq!(std::format!("{wrapped:?}"), "[2, 3, 4]");
    }

    #[test]
    #[should_panic(expected = "fixed-capacity ring buffer is full")]
    fn collecting_does_not_discard_items() {
        let _buffer = (0 .. 3).collect::<RingBuffer<_, 2>>();
    }
}
