fmt:
    taplo fmt
    cargo +nightly fmt
    rumdl fmt .
    rumdl check --fix .

check:
    cargo +nightly check