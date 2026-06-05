use core::mem::MaybeUninit;

pub struct Array<T, const CAPACITY: usize> {
    data: [MaybeUninit<T>; CAPACITY],
    len:  usize,
}
