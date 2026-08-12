# xiaoyong-collections

Allocation-free collection types with storage embedded directly in the value.

## `Array`

`unsync::array::Array<T, N>` is a vector-like container with compile-time
capacity `N`. It supports indexed and slice access, ordered and unordered
removal, insertion, borrowed iteration, owning double-ended iteration, and
capacity-aware insertion through `try_push` and `try_insert`.

The type is `no_std`, performs no heap allocations, supports zero-sized and
zero-capacity arrays, and drops every initialized element exactly once.

## `RingBuffer`

`unsync::ring_buffer::RingBuffer<T, N>` is an allocation-free, fixed-capacity
double-ended queue. It provides constant-time insertion and removal at both
ends, FIFO `push`/`pop` aliases, explicit overwrite operations, logical-order
indexing and iteration, and two-slice access to wrapped storage.

Like `Array`, it is `no_std`, stores elements inline, supports zero capacity,
and returns ownership of values rejected because the buffer is full.
