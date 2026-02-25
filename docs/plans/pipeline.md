# Pipeline Plan (v0)

## Goal

Define a reusable, transportable pipeline language for `gibblox` that can describe and open
composed read-only block sources across native and wasm targets.

The pipeline must be product-agnostic and stable enough to be shared across CLIs, services,
workers, and UI runtimes without duplicating composition logic in each consumer.

## Why This Exists

- Complex source chains are now common (`filesystem in partition table in container in compression`).
- The same chain logic is being reimplemented in multiple places.
- Worker/RPC boundaries need a typed payload, not ad hoc URL/query reconstruction.
- A first-class pipeline schema unlocks reproducible inputs, test fixtures, and offline tooling.

## Non-Negotiables

- Read-only behavior only (no write/format/mutation semantics in pipeline execution).
- Deterministic validation and deterministic error boundaries.
- Platform-agnostic core model (`no_std + alloc`) with platform bindings in leaf crates.
- Async-first execution path.
- Stable, versioned binary representation for transport/storage.

## v0 Scope

### In Scope

- A core pipeline descriptor AST.
- YAML authoring format via `serde`.
- Binary encode/decode format (versioned header + payload).
- Validation rules for shape/selector/depth constraints.
- Runtime resolvers for:
  - native (`std`) environments
  - wasm/web environments
- A small `gibblox-cli` for YAML <-> binary conversion and validation.

### Out of Scope (v0)

- Mutation/install workflows.
- Policy/ranking engines for choosing among many valid pipelines.
- Automatic format sniffing and source auto-discovery.
- New filesystem abstractions (this plan is about source pipelines, not filesystem APIs).

## Proposed Crate Layout

- `crates/gibblox-pipeline`
  - Core descriptor types.
  - Validation.
  - Binary framing constants and conversion structs (via `bin` module).
  - No runtime/network/filesystem bindings.
- `crates/gibblox-pipeline-std`
  - Native resolver from descriptor -> `Arc<dyn BlockReader>`.
  - Integrates `gibblox-http`, `gibblox-file`, `gibblox-casync-std`, and cache crates.
- `crates/gibblox-pipeline-web`
  - wasm resolver from descriptor -> `Arc<dyn BlockReader>`.
  - Integrates `gibblox-http` wasm client, `gibblox-web-file`, `gibblox-casync-web`, OPFS cache.
- `cli/` (new crate `gibblox-cli`)
  - `pipeline encode` (YAML -> binary)
  - `pipeline decode` (binary -> YAML)
  - `pipeline validate` (YAML or binary input)
  - optional `pipeline show --format json|yaml` canonical printer

## Descriptor Model (v0)

`PipelineSource` is a recursive enum with leaf and wrapper/selectors.

### Leaf Sources

- `http`
- `file`
- `casync` (`index`, optional `chunk_store`)

### Wrappers / Selectors

- `xz`
- `android_sparseimg`
- `mbr` (exactly one selector: `partuuid` or `index`)
- `gpt` (exactly one selector: `partlabel`, `partuuid`, or `index`)

### Optional v0.1 Extensions (deferred by default)

- `zip_entry`
- `offset`

These are likely useful soon but are not required to ship the first cut.

## YAML Shape

Prefer a one-key object style for readability and stable round-trips.
Wrapper/selectors keep nested source fields flattened for compact authoring.

Example:

```yaml
gpt:
  partlabel: rootfs
  android_sparseimg:
    xz:
      http: https://cdn.example.invalid/device.img.xz
```

Also valid:

```yaml
casync:
  index: https://cdn.example.invalid/indexes/rootfs.caibx
  chunk_store: https://cdn.example.invalid/chunks/
```

## Binary Representation

Binary format mirrors the YAML descriptor but uses an explicit transport enum.

- Header:
  - magic: 8 bytes (`GBXPIPE0` proposed)
  - format version: `u16` LE (`1` for v0)
- Payload:
  - `postcard` encoded `PipelineSourceBin`

This follows a simple, explicit framing model with deterministic decode failures:

- invalid magic -> `InvalidMagic`
- unsupported version -> `UnsupportedFormatVersion(v)`
- payload decode failure -> `Decode(err)`

## Validation Rules (v0)

- Maximum recursion depth: `16`.
- `casync.index`:
  - reject `.caidx`
  - allow `.caibx`
- `mbr` selector cardinality:
  - exactly one of `partuuid` or `index`
  - non-empty trimmed `partuuid`
- `gpt` selector cardinality:
  - exactly one of `partlabel`, `partuuid`, or `index`
  - non-empty trimmed `partlabel`/`partuuid`
- All string leaf values must be non-empty after trim.

## Resolver API Shape

Both resolver crates expose a similar interface:

```rust
pub async fn open_pipeline(
    source: &gibblox_pipeline::PipelineSource,
    opts: &OpenPipelineOptions,
) -> anyhow::Result<std::sync::Arc<dyn gibblox_core::BlockReader>>
```

`OpenPipelineOptions` holds runtime-specific policy knobs (cache settings, default block size,
web file handle lookup, etc.) without polluting the core descriptor.

## CLI Plan (`cli/`)

Create `gibblox-cli` as a minimal utility focused on pipeline artifacts.

### Commands

- `gibblox pipeline encode -i pipeline.yaml -o pipeline.gbxp`
- `gibblox pipeline decode -i pipeline.gbxp -o pipeline.yaml`
- `gibblox pipeline validate -i pipeline.yaml`
- `gibblox pipeline validate -i pipeline.gbxp --binary`

### Command Behavior

- `encode`
  - parse YAML -> validate -> write binary.
- `decode`
  - parse binary -> validate -> write canonical YAML.
- `validate`
  - parse selected input format and return structured error text.

### UX Notes

- Keep output quiet by default; print machine-friendly errors.
- Exit codes: `0` success, non-zero failure.
- No network reads during encode/decode/validate.

## Rollout Phases

### Phase 0: Schema + Plan

- Land this plan.
- Define public API goals and invariants.

### Phase 1: Core Descriptor + Binary Codec

- Implement `crates/gibblox-pipeline` types.
- Implement `PipelineSourceBin` and codec helpers.
- Add unit tests for round-trip and validation errors.

### Phase 2: Native Resolver

- Implement `crates/gibblox-pipeline-std`.
- Add integration tests for representative chains:
  - `http -> xz`
  - `file -> gpt(index)`
  - `casync(index+chunk_store)`
  - `xz -> android_sparseimg -> gpt(partuuid)`

### Phase 3: Web Resolver

- Implement `crates/gibblox-pipeline-web`.
- Add wasm tests for parse/validate/open behavior where practical.

### Phase 4: CLI

- Add `cli/` crate and wire commands.
- Add fixture tests for YAML/binary round-trip and failure cases.

### Phase 5: Worker/RPC Adoption

- Use pipeline binary payload across worker boundaries.
- Remove query-param based pipeline reconstruction.

## Testing Strategy

- Unit tests in `gibblox-pipeline`:
  - selector cardinality checks
  - depth limits
  - `.caidx` rejection
  - binary framing/version failures
  - YAML <-> binary round-trip equivalence
- Resolver tests:
  - deterministic open order for nested wrappers
  - expected identity strings include composition markers
  - explicit errors for unsupported/malformed pipeline nodes

## Validation Commands

- `cargo fmt`
- `cargo check -p gibblox-pipeline`
- `cargo test -p gibblox-pipeline`
- `cargo check -p gibblox-pipeline-std`
- `cargo check -p gibblox-pipeline-web`
- `cargo check -p gibblox-cli`

## Risks and Mitigations

- Risk: schema drift between YAML and binary forms.
  - Mitigation: single conversion boundary and strict round-trip tests.
- Risk: platform-specific behavior divergence.
  - Mitigation: shared core validation + parallel resolver fixture coverage.
- Risk: overloading descriptor with runtime policy.
  - Mitigation: keep descriptor declarative; put runtime policy in `OpenPipelineOptions`.

## Success Criteria

- A single pipeline descriptor opens equivalent reader chains on native and web.
- YAML and binary forms round-trip without semantic drift.
- CLI conversion tooling is stable enough for fixtures and CI inputs.
- Worker/RPC integration can pass a typed pipeline payload without ad hoc reconstruction.
- Implementation history is kept in conventional commits.
