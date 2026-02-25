set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

fmt:
    cargo fmt --all -- --check

check:
    cargo check --workspace --locked

clippy:
    cargo clippy --workspace --all-targets --locked

test:
    cargo test --workspace --locked

wasm-check:
    cargo check --target wasm32-unknown-unknown --locked -p gibblox-http --no-default-features --features wasm-client
    cargo check --target wasm32-unknown-unknown --locked -p gibblox-web-file -p gibblox-blockreader-messageport -p gibblox-casync-web -p gibblox-cache-store-opfs -p gibblox-web-worker -p gibblox-ext4

ci-rust: fmt check clippy test

ci-wasm: wasm-check

ci: ci-rust ci-wasm
