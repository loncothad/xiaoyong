# xiaoyong

效用 (Xiào yòng) means "utility" in Chinese.

A collection of Rust utility crates. Each crate lives in [`crates/`](crates/)
and has its own README with API and usage details.

- [`xiaoyong-channels`](crates/channels): bounded asynchronous SPSC, MPSC,
  broadcast MPMC, and oneshot channels.
- [`xiaoyong-notify`](crates/notify): reusable, one-shot, and queued
  notifications plus a fair single-threaded semaphore.
- [`xiaoyong-value`](crates/value): mutexes, reader-writer locks, swappable
  reference-counted snapshots, and single-assignment values.
- [`xiaoyong-collections`](crates/collections): allocation-free fixed-capacity
  collections.

The workspace's minimum supported Rust version is 1.85.

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.

## Contributions

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion by you, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions.
