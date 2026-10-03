fmt:
    taplo fmt
    cargo +nightly fmt
    rumdl fmt .
    rumdl check --fix .

check:
    cargo check --workspace
    cargo check -p xiaoyong-value --features arc-swap
    cargo test --workspace --all-features
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
