set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

# Bump workspace version (supports semver and semver-rc.N)
bump version:
    #!/usr/bin/env bash
    set -euo pipefail

    version="{{version}}"

    if [[ "$version" =~ _rc([0-9]+)$ ]]; then
        echo "Unsupported RC format '$version'. Use canonical semver '-rc.N' (for example: 1.2.3-rc.1)."
        exit 1
    fi

    if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?$ ]]; then
        echo "Unsupported version '$version'. Use semver (for example: 1.2.3 or 1.2.3-rc.1)."
        exit 1
    fi

    current="$(sed -nE '/^\[workspace\.package\]/,/^\[/{s/^version = "(.*)"$/\1/p}' Cargo.toml)"
    if [[ -z "$current" ]]; then
        echo "workspace.package version line not found in Cargo.toml"
        exit 1
    fi

    if [[ "$current" == "$version" ]]; then
        echo "Workspace version already set to $version"
        exit 0
    fi

    sed -i -E "/^\[workspace\.package\]/,/^\[/{s/^version = \".*\"$/version = \"$version\"/;}" Cargo.toml

    cargo generate-lockfile
    echo "Bumped workspace version to $version"

publish-dry-run:
    cargo publish --workspace --locked --dry-run

publish:
    cargo publish --workspace --locked

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
    cargo check --target wasm32-unknown-unknown --locked -p gibblox-cache-store-opfs

ci-rust: fmt check clippy test

ci-wasm: wasm-check

ci: ci-rust ci-wasm
