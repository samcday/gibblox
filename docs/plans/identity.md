# Identity Plan (config-backed pattern)

## Goal

Keep `BlockReader` identity behavior as-is, but standardize how identities are produced.

We do that by making each reader constructor accept a cheap config handle (`FooConfig`) that:

- describes how to open the reader,
- can be used to derive stable identity before opening,
- becomes the single source for cache/export/coalescing keys.

## Core Decision

- `BlockReader` keeps `write_identity`.
- We are not removing identity from runtime readers right now.
- We enforce a shared pattern: identity originates from config, and reader identity delegates to
  that config identity.

This is intentionally pragmatic and low-risk.

## Pattern

For each reader type, define a config struct and make constructor(s) accept it.

Examples:

- `HttpBlockReaderConfig`
- `CasyncReaderConfig` (already exists; extend for identity needs)
- `FileBlockReaderConfig`
- wrapper configs such as `XzBlockReaderConfig`, `GptBlockReaderConfig`, etc.

Each config should support:

1. canonical identity writing (deterministic fields only),
2. identity digest derivation (for keying),
3. reader construction.

## Partial Enforcement Through Code

Add a small trait for config-side identity in core (name can be `BlockReaderConfig`):

```rust
pub trait BlockReaderConfigIdentity {
    fn write_identity(&self, out: &mut dyn core::fmt::Write) -> core::fmt::Result;
}
```

Optional helper API:

```rust
pub fn config_identity_string(cfg: &impl BlockReaderConfigIdentity) -> alloc::string::String;
pub fn config_identity_digest32(cfg: &impl BlockReaderConfigIdentity) -> [u8; 32];
pub fn config_identity_id32(cfg: &impl BlockReaderConfigIdentity) -> u32;
```

Reader implementations then follow one rule:

- `impl BlockReader for FooReader { fn write_identity(...) { self.config.write_identity(...) } }`

This is the key invariant.

## Why This Works

Pipeline/resolver layers can build configs first (cheap), compute identity/keying material before
instantiation, and still use those same configs to open readers later.

That gives us one handle that does both jobs:

- pre-open reasoning (cache key, export ID, coalescing key),
- runtime initialization.

## Canonical Identity Rules (v1)

Identity should include byte-affecting descriptor fields and exclude policy-only knobs.

Include:

- source locators (`http` URL, `casync` index/chunk store, file path string),
- structural wrappers/selectors (partition selectors, compression/container layers),
- schema fields that alter bytes returned by reads.

Exclude:

- timeout/retry/concurrency tuning,
- logging/tracing toggles,
- transient runtime state.

## Pipeline Integration

Pipeline resolver flow should become:

1. decode/validate pipeline,
2. build config handle tree,
3. derive keying material from config handles,
4. open readers from those same handles.

For shared-worker coalescing:

- use config-derived stage keys for overlap detection,
- reuse matching stage configs/readers,
- dedupe overlapping in-flight reads per stage key.

## Cross-Crate Impact

### `gibblox-core`

- Add config-identity trait/helpers.
- Keep `BlockReader` unchanged.

### Reader crates (`gibblox-http`, `gibblox-casync`, `gibblox-file`, wrappers)

- Add/normalize `FooConfig` constructor pattern.
- Ensure reader `write_identity` delegates to stored config identity.

### `gibblox-pipeline` and web/native resolvers

- Construct config handles first.
- Compute cache/export/coalescing IDs from config handles before opening readers.

### Cache layers (`gibblox-cache*`)

- Prefer keying APIs that accept config-derived IDs directly.
- Keep reader-based fallback temporarily if needed during migration.

### `gibblox-web-worker`

- Startup remains check-in only.
- `open_pipeline` derives endpoint identity from config-handle graph, then opens pipeline.

## Rollout

1. Add config-identity trait/helpers in `gibblox-core`.
2. Migrate high-impact readers to config-backed constructors.
3. Update resolver paths to key from config handles pre-open.
4. Update cache/worker consumers to use config-derived IDs.
5. Backfill remaining reader crates to the same pattern.

## Validation

Add tests for:

- config identity determinism,
- config identity equals reader identity after open,
- cache key stability from config handles,
- shared-worker stage-key reuse for overlapping pipelines.

Run targeted checks as touched:

- `cargo fmt`
- `cargo check -p gibblox-core`
- `cargo check -p gibblox-pipeline`
- `cargo check -p gibblox-cache`
- `cargo check -p gibblox-web-worker`

## Acceptance Criteria

1. `BlockReader` still provides identity.
2. New/updated reader constructors accept explicit config structs.
3. Pipeline resolver can derive identity and keys from configs before reader instantiation.
4. Reader identity is produced from config identity, not ad-hoc runtime formatting.
5. Cache/export/coalescing consume consistent config-derived identity material.
