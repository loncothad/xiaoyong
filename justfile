fmt:
    taplo fmt
    cargo +nightly fmt
    rumdl fmt .
    rumdl check --fix .

check:
    cargo +nightly check --workspace
    cargo +nightly check -p xiaoyong-value --features arc-swap
    cargo +nightly test --workspace --all-features
    cargo +nightly clippy --workspace --all-targets --all-features -- -D warnings
    RUSTDOCFLAGS="-D warnings" cargo +nightly doc --workspace --all-features --no-deps
