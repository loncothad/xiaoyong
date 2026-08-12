//! An inline, fixed-capacity vector.

use core::{
    fmt,
    iter::FusedIterator,
    mem::{
        ManuallyDrop,
        MaybeUninit,
    },
    ops::{
        Deref,
        DerefMut,
        Index,
        IndexMut,
    },
    ptr,
    slice,
};

/// Error returned when inserting into a full fixed-capacity collection.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CapacityError<T>(T);

impl<T> CapacityError<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(value)
    }

    /// Returns the value that could not be inserted.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for CapacityError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapacityError(..)")
    }
}

impl<T> fmt::Display for CapacityError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("fixed-capacity collection is full")
    }
}

impl<T> core::error::Error for CapacityError<T> {}

/// A vector-like collection that stores up to `CAPACITY` elements inline.
///
/// Unlike a heap-backed vector, `Array` never allocates or changes capacity.
/// Elements occupy the first [`len`](Self::len) initialized slots and retain
/// insertion order.
pub struct Array<T, const CAPACITY: usize> {
    data: [MaybeUninit<T>; CAPACITY],
    len:  usize,
}

impl<T, const CAPACITY: usize> Array<T, CAPACITY> {
    /// Creates an empty array.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: core::array::from_fn(|_| MaybeUninit::uninit()),
            len:  0,
        }
    }

    /// Returns the number of initialized elements.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns the maximum number of elements the array can hold.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        CAPACITY
    }

    /// Returns `true` when the array contains no elements.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns `true` when no more elements can be inserted.
    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.len == CAPACITY
    }

    /// Appends an element, returning it when the array is full.
    pub fn try_push(&mut self, value: T) -> Result<(), CapacityError<T>> {
        if self.is_full() {
            return Err(CapacityError::new(value));
        }

        self.data[self.len].write(value);
        self.len += 1;
        Ok(())
    }

    /// Appends an element.
    ///
    /// # Panics
    ///
    /// Panics if the array is full.
    pub fn push(&mut self, value: T) {
        assert!(!self.is_full(), "fixed-capacity array is full");
        self.data[self.len].write(value);
        self.len += 1;
    }

    /// Removes and returns the last element, or `None` when empty.
    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        self.len -= 1;
        // SAFETY: the old final slot was initialized and `len` was reduced so
        // it will not be dropped again.
        Some(unsafe { self.data[self.len].assume_init_read() })
    }

    /// Inserts an element at `index`, returning it when the array is full.
    ///
    /// # Panics
    ///
    /// Panics if `index > len`.
    pub fn try_insert(&mut self, index: usize, value: T) -> Result<(), CapacityError<T>> {
        assert!(index <= self.len, "insertion index out of bounds");
        if self.is_full() {
            return Err(CapacityError::new(value));
        }

        // SAFETY: `index..len` is initialized, the destination range is within
        // the allocation, and `ptr::copy` permits overlapping ranges.
        unsafe {
            let pointer = self.data.as_mut_ptr();
            ptr::copy(pointer.add(index), pointer.add(index + 1), self.len - index);
            pointer.add(index).write(MaybeUninit::new(value));
        }
        self.len += 1;
        Ok(())
    }

    /// Removes and returns the element at `index`, shifting later elements.
    ///
    /// # Panics
    ///
    /// Panics if `index >= len`.
    pub fn remove(&mut self, index: usize) -> T {
        assert!(index < self.len, "removal index out of bounds");

        // SAFETY: `index` is initialized. Reading moves the value out, then the
        // overlapping copy fills the hole. The old last slot becomes logically
        // uninitialized after `len` is reduced.
        unsafe {
            let pointer = self.data.as_mut_ptr();
            let value = pointer.add(index).read().assume_init();
            ptr::copy(pointer.add(index + 1), pointer.add(index), self.len - index - 1);
            self.len -= 1;
            value
        }
    }

    /// Removes and returns the element at `index` by moving the last element
    /// into its place. This does not preserve order.
    ///
    /// # Panics
    ///
    /// Panics if `index >= len`.
    pub fn swap_remove(&mut self, index: usize) -> T {
        assert!(index < self.len, "removal index out of bounds");
        let last = self.len - 1;
        self.len = last;

        // SAFETY: both indices were initialized before `len` was reduced. The
        // removed value is moved out, and the last value is moved into the hole
        // unless the two slots are the same.
        unsafe {
            let pointer = self.data.as_mut_ptr();
            let value = pointer.add(index).read().assume_init();
            if index != last {
                pointer.add(index).write(pointer.add(last).read());
            }
            value
        }
    }

    /// Shortens the array to `new_len`, dropping removed elements.
    pub fn truncate(&mut self, new_len: usize) {
        if new_len >= self.len {
            return;
        }

        let old_len = self.len;
        self.len = new_len;
        // SAFETY: the removed range was initialized. `len` is changed first so
        // a panic in an element destructor cannot cause a double drop.
        unsafe {
            ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                self.data.as_mut_ptr().add(new_len).cast::<T>(),
                old_len - new_len,
            ));
        }
    }

    /// Removes all elements.
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Returns the initialized elements as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: the first `len` slots are always initialized.
        unsafe { slice::from_raw_parts(self.data.as_ptr().cast::<T>(), self.len) }
    }

    /// Returns the initialized elements as a mutable slice.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: the first `len` slots are initialized and `&mut self` ensures
        // exclusive access.
        unsafe { slice::from_raw_parts_mut(self.data.as_mut_ptr().cast::<T>(), self.len) }
    }
}

impl<T, const CAPACITY: usize> Default for Array<T, CAPACITY> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone, const CAPACITY: usize> Clone for Array<T, CAPACITY> {
    fn clone(&self) -> Self {
        let mut clone = Self::new();
        for value in self {
            clone.push(value.clone());
        }
        clone
    }
}

impl<T: fmt::Debug, const CAPACITY: usize> fmt::Debug for Array<T, CAPACITY> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self).finish()
    }
}

impl<T, const CAPACITY: usize> Drop for Array<T, CAPACITY> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<T, const CAPACITY: usize> Deref for Array<T, CAPACITY> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T, const CAPACITY: usize> DerefMut for Array<T, CAPACITY> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T, I, const CAPACITY: usize> Index<I> for Array<T, CAPACITY>
where
    I: slice::SliceIndex<[T]>,
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl<T, I, const CAPACITY: usize> IndexMut<I> for Array<T, CAPACITY>
where
    I: slice::SliceIndex<[T]>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.as_mut_slice()[index]
    }
}

impl<'a, T, const CAPACITY: usize> IntoIterator for &'a Array<T, CAPACITY> {
    type IntoIter = slice::Iter<'a, T>;
    type Item = &'a T;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl<'a, T, const CAPACITY: usize> IntoIterator for &'a mut Array<T, CAPACITY> {
    type IntoIter = slice::IterMut<'a, T>;
    type Item = &'a mut T;

    fn into_iter(self) -> Self::IntoIter {
        self.as_mut_slice().iter_mut()
    }
}

/// Owning iterator for [`Array`].
pub struct IntoIter<T, const CAPACITY: usize> {
    array: ManuallyDrop<Array<T, CAPACITY>>,
    front: usize,
    back:  usize,
}

impl<T, const CAPACITY: usize> Iterator for IntoIter<T, CAPACITY> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            return None;
        }
        let index = self.front;
        self.front += 1;
        // SAFETY: `[front, back)` contains initialized, not-yet-yielded values.
        Some(unsafe { self.array.data[index].assume_init_read() })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.back - self.front;
        (remaining, Some(remaining))
    }
}

impl<T, const CAPACITY: usize> DoubleEndedIterator for IntoIter<T, CAPACITY> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            return None;
        }
        self.back -= 1;
        // SAFETY: `[front, back]` contained initialized, not-yet-yielded values.
        Some(unsafe { self.array.data[self.back].assume_init_read() })
    }
}

impl<T, const CAPACITY: usize> ExactSizeIterator for IntoIter<T, CAPACITY> {}
impl<T, const CAPACITY: usize> FusedIterator for IntoIter<T, CAPACITY> {}

impl<T, const CAPACITY: usize> Drop for IntoIter<T, CAPACITY> {
    fn drop(&mut self) {
        // SAFETY: only `[front, back)` remains initialized and owned by us.
        unsafe {
            ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                self.array.data.as_mut_ptr().add(self.front).cast::<T>(),
                self.back - self.front,
            ));
        }
    }
}

impl<T, const CAPACITY: usize> IntoIterator for Array<T, CAPACITY> {
    type IntoIter = IntoIter<T, CAPACITY>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        let back = self.len;
        IntoIter {
            array: ManuallyDrop::new(self),
            front: 0,
            back,
        }
    }
}

impl<T, const CAPACITY: usize> Extend<T> for Array<T, CAPACITY> {
    /// Extends the array with every item from the iterator.
    ///
    /// # Panics
    ///
    /// Panics if the iterator contains more items than the remaining capacity.
    fn extend<I: IntoIterator<Item = T>>(&mut self, values: I) {
        for value in values {
            self.push(value);
        }
    }
}

impl<T, const CAPACITY: usize> FromIterator<T> for Array<T, CAPACITY> {
    /// Collects all items into an array.
    ///
    /// # Panics
    ///
    /// Panics if the iterator contains more than `CAPACITY` items.
    fn from_iter<I: IntoIterator<Item = T>>(values: I) -> Self {
        let mut array = Self::new();
        array.extend(values);
        array
    }
}

impl<T: PartialEq, const CAPACITY: usize> PartialEq for Array<T, CAPACITY> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Eq, const CAPACITY: usize> Eq for Array<T, CAPACITY> {}

#[cfg(test)]
mod tests {
    use core::sync::atomic::{
        AtomicUsize,
        Ordering,
    };
    use std::{
        string::String,
        sync::Arc,
    };

    use super::*;

    #[test]
    fn vector_operations_preserve_invariants() {
        let mut array = Array::<i32, 4>::new();
        assert!(array.is_empty());
        array.push(1);
        array.push(3);
        assert!(array.try_insert(1, 2).is_ok());
        assert_eq!(array.as_slice(), &[1, 2, 3]);
        assert_eq!(array.remove(1), 2);
        assert_eq!(array.swap_remove(0), 1);
        assert_eq!(array.as_slice(), &[3]);
        assert_eq!(array.pop(), Some(3));
        assert_eq!(array.pop(), None);
    }

    #[test]
    fn capacity_error_returns_value() {
        let mut array = Array::<String, 1>::new();
        array.push(String::from("first"));
        let result = array.try_push(String::from("second"));
        assert_eq!(result.map_err(CapacityError::into_inner), Err(String::from("second")));
    }

    #[test]
    fn drops_every_element_exactly_once() {
        struct TrackDrop(Arc<AtomicUsize>);
        impl Drop for TrackDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let mut array = Array::<TrackDrop, 4>::new();
        for _ in 0 .. 4 {
            array.push(TrackDrop(Arc::clone(&dropped)));
        }
        drop(array.remove(1));
        array.truncate(1);
        assert_eq!(dropped.load(Ordering::Relaxed), 3);
        drop(array);
        assert_eq!(dropped.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn owning_iterator_supports_both_ends_and_drops_remainder() {
        let array: Array<_, 4> = [1, 2, 3, 4].into_iter().collect();
        let mut iterator = array.into_iter();
        assert_eq!(iterator.next(), Some(1));
        assert_eq!(iterator.next_back(), Some(4));
        assert_eq!(iterator.collect::<std::vec::Vec<_>>(), [2, 3]);
    }

    #[test]
    fn zero_capacity_is_supported() {
        let mut array = Array::<u8, 0>::new();
        assert!(array.is_empty());
        assert!(array.is_full());
        assert_eq!(array.try_push(1).map_err(CapacityError::into_inner), Err(1));
    }

    #[test]
    #[should_panic(expected = "fixed-capacity array is full")]
    fn collecting_does_not_silently_discard_items() {
        let _array = (0 .. 3).collect::<Array<_, 2>>();
    }
}
