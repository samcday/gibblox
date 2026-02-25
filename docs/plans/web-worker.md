# Web Worker Plan (first-class, drop-in runtime)

## Purpose

Build a first-class gibblox web worker runtime that any web consumer can adopt without custom
pipeline reconstruction logic.

The worker accepts a matured binary pipeline descriptor as the canonical source definition and
serves `BlockReader` access over MessagePort-based RPC.

This document is intentionally self-contained so a later agent can implement the work without
context from downstream repositories.

## Assumptions

- Pipeline descriptor format is mature and can describe all required downstream source chains.
- No compatibility shims are required for unreleased behavior.
- Existing MessagePort block RPC (`gibblox-blockreader-messageport`) remains the data plane.

## Problem Statement

Web consumers currently need custom startup glue for worker scripts, pipeline-open RPC fields,
and source construction. That causes:

- inconsistent startup protocols,
- duplicated lifecycle logic,
- no shared model for multi-consumer coalescing,
- weak drop-in ergonomics.

## Objectives

1. Provide a single, reusable worker startup contract with deterministic ready check-in.
2. Offer a drop-in client API for DedicatedWorker and SharedWorker usage.
3. Keep worker runtime product-agnostic.
4. Support cross-consumer coalescing of overlapping pipelines in shared-worker mode.
5. Preserve current read-only semantics and MessagePort RPC behavior.

## Non-Goals

- Defining canonical-origin deployment policy (covered in a separate document).
- Adding product-specific source heuristics or pipeline mutation behavior.
- Replacing MessagePort block RPC protocol.

## Architecture

### Runtime modes

- `DedicatedWorker` mode: one page/session owns one worker instance.
- `SharedWorker` mode: multiple clients attach to one worker instance on the same origin.

### Canonical-origin direction

This plan targets a future canonical-origin model, but does not define that deployment contract.
It only requires that worker startup/check-in and pipeline-open APIs are transport-neutral enough for that
future model.

In practice, shared-worker coalescing can be applied:

- directly for same-origin clients,
- and later for disparate client origins that route through a canonical-origin worker boundary.

## Public API Contract

### Host-side API (Rust wasm)

Expose a stable high-level API from `gibblox-web-worker`:

- spawn/start worker (dedicated or shared mode),
- await worker check-in (`ready`) after startup,
- call a small control RPC to open a pipeline endpoint,
- attach one or more `MessagePortBlockReaderClient` handles,
- read worker metadata and lifecycle state,
- clean shutdown.

The API must not require callers to know worker-internal command names.

### Worker-side command model

Define explicit control commands:

- `open_pipeline`: receives pipeline bytes and runtime config, returns a transferred
  `MessagePort` on success.
- `shutdown`: optional explicit teardown.

Define explicit state notifications:

- `ready` emitted when worker startup/check-in completes,
- `error` with structured message on startup or runtime failure.

All commands must be idempotent where practical and return deterministic errors when invalid for
the current worker state.

`open_pipeline` contract:

- input: binary pipeline payload (+ optional per-endpoint runtime options),
- success: transferred `MessagePort` whose remote end is configured exactly for that pipeline,
- failure: structured error and no port transfer.

Callers may immediately wrap the returned port with `MessagePortBlockReaderClient`.

## Pipeline Resolution Model

Worker request path (`open_pipeline`):

1. receive pipeline payload,
2. decode binary pipeline,
3. validate descriptor,
4. resolve to `Arc<dyn BlockReader>` using web resolver,
5. register/reuse resolved stages in worker registry,
6. create MessagePort-backed block RPC server endpoint,
7. transfer endpoint port to caller.

The resolver is considered an internal implementation detail of the worker runtime, but must be
cleanly separable (for testing and reuse).

## Multi-Consumer Coalescing Model

### Why coalescing matters

Two distinct pipelines can overlap heavily, for example both traverse:

`gpt -> android_sparseimg -> xz -> ...`

while diverging only at terminal filesystem/partition selection.

Without coalescing, each consumer can re-fetch and re-decode the same upstream bytes.

### Required coalescing behavior

In shared-worker mode, the runtime must coalesce at two levels:

1. **Pipeline/stage coalescing**
   - Normalize pipeline descriptors into deterministic stage keys.
   - Reuse already-open stage instances when keys match.
   - Share common prefixes between distinct pipelines.

2. **I/O coalescing**
   - Deduplicate concurrent overlapping read/fetch requests across consumers.
   - Reuse in-flight operations where possible before issuing new upstream reads.

### Implementation guidance

- Build an internal stage registry keyed by canonical stage identity.
- Track refcounts per stage so resources are released when no pipelines reference them.
- Keep per-stage in-flight request maps keyed by range/block spans.
- Preserve priority semantics from `ReadContext` while coalescing.

## Resource Management

Worker runtime must define explicit limits and eviction behavior:

- max active pipelines,
- max retained stage instances,
- memory watermarks for decoded/intermediate caches,
- idle timeout for pipeline/stage teardown in shared mode.

Configuration should be accepted at startup and/or `open_pipeline`, with sane defaults.

## Error Model

Startup, `open_pipeline`, and runtime errors must be structured and actionable:

- invalid payload,
- decode/validation failure,
- resolver/open failure,
- unsupported platform capability,
- internal worker state violation.

All errors surfaced to host callers should be stable text + machine-friendly kind where possible.

## Observability

Add tracing for operations that affect correctness or performance:

- startup check-in begin/success/failure,
- `open_pipeline` begin/success/failure,
- pipeline resolution latency,
- stage registry hit/miss,
- coalesced vs non-coalesced fetch counts,
- active client/session counts,
- teardown and eviction decisions.

## Implementation Phases

### Phase 1: Worker contract hardening

- Define and implement stable startup check-in + `open_pipeline` protocol.
- Provide dedicated-worker host API.
- Keep MessagePort block RPC unchanged.

### Phase 2: Shared-worker runtime

- Add shared-worker entrypoint and host API.
- Implement session management for multiple clients.

### Phase 3: Coalescing engine

- Add canonical stage-keying and stage registry.
- Add in-flight I/O coalescing across sessions.
- Add resource limits + eviction policies.

### Phase 4: Drop-in polish

- Finalize ergonomics and docs for external adopters.
- Add examples for dedicated and shared usage.
- Ensure failures are deterministic and easy to diagnose.

## Acceptance Criteria

1. A consumer can start the worker and receive deterministic check-in (`ready`) with no pipeline
   payload.
2. A consumer can send pipeline bytes via `open_pipeline` and receive either a configured
   `MessagePort` or a structured error.
3. A consumer can wrap returned ports with `MessagePortBlockReaderClient` with no custom glue.
4. Shared-worker mode supports concurrent clients safely.
5. Overlapping pipelines reuse shared upstream stages.
6. Overlapping concurrent reads are deduplicated.
7. Teardown releases resources predictably.

## Validation Plan

Run targeted checks during implementation:

- `cargo fmt`
- `cargo check -p gibblox-web-worker`
- `cargo check -p gibblox-blockreader-messageport`
- `cargo check -p gibblox-pipeline`
- `cargo check -p gibblox-pipeline-web` (if introduced/touched)
- `cargo test -p gibblox-web-worker` (as tests are added)

Add integration tests for:

- startup check-in success/failure,
- `open_pipeline` success/failure (port transfer and error shape),
- dedicated attach/read lifecycle,
- shared-worker multi-client attach/read lifecycle,
- stage coalescing correctness for overlapping pipelines,
- in-flight I/O dedupe behavior under concurrency.

## Deliverables

- First-class `gibblox-web-worker` API and runtime.
- Dedicated + shared worker support.
- Coalescing runtime for overlapping pipelines.
- Documentation/examples sufficient for downstream drop-in adoption.
