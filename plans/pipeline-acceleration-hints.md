# Pipeline Acceleration Indexes Plan (first pass)

## Goal

Add `gibblox pipeline optimize` to compute and embed optional stage indexes into a pipeline binary.

Immediate target: Android sparse image acceleration.

When an `android_sparseimg` stage already includes an index, runtime should trust it and skip sparse
chunk discovery work entirely.

## Scope for this slice

1. Rename the concept from **hints** to **indexes**.
2. Add optional Android sparse index fields to the pipeline schema.
3. Add `gibblox pipeline optimize` command.
4. Implement optimizer support for `android_sparseimg` stages only.
5. Implement Android sparse runtime consumption of embedded index data.

## Core contract

- Indexes are **optional** on each stage.
- If index data is missing, runtime uses existing constructor/lazy-init behavior.
- If index data is present, runtime consumes it directly and skips discovery/scan logic for that stage.
- If provided index data is invalid, runtime returns an error for that stage (no index-to-scan fallback).
- Pipeline identity remains based on semantic source definition, not indexes.

## Schema changes

### Binary format

- Keep `PIPELINE_BIN_FORMAT_VERSION` unchanged.
- Evolve schema in-place for current dev usage.

### Pipeline model

Add an optional index field to Android sparse stage:

- `PipelineSourceAndroidSparseImgSource { android_sparseimg: Box<PipelineSource>, index: Option<PipelineAndroidSparseIndex> }`

### Android sparse index payload

Add typed structs:

- `PipelineAndroidSparseIndex`
  - sparse header metadata (`file_hdr_sz`, `chunk_hdr_sz`, `blk_sz`, `total_blks`, `total_chunks`, `image_checksum`)
  - `chunks: Vec<PipelineAndroidSparseChunkIndex>`
- `PipelineAndroidSparseChunkIndex`
  - `chunk_index`
  - `chunk_type`
  - `chunk_sz`
  - `total_sz`
  - `chunk_offset`
  - `payload_offset`
  - `payload_size`
  - `output_start` / `output_end` (for mapped output chunks)
  - optional fill/crc payload metadata when relevant

Index data should include all sparse chunks so we can skip walking chunk headers at runtime.

## CLI contract

```text
gibblox pipeline optimize [INPUT] [-o OUTPUT]
```

First pass behavior:

- Input defaults to `-` (stdin), binary pipeline expected.
- Output defaults to `-` (stdout), binary pipeline.
- Decode -> optimize -> validate -> encode.
- Only Android sparse stage indexes are computed in this slice.

First-pass flags:

- `--force`: recompute and overwrite existing Android sparse indexes.

Command should print a short stderr report with stage counts and index updates.

## Optimizer design

Add std-only optimizer APIs in `gibblox-pipeline`:

- `optimize_pipeline(source: &mut PipelineSource, opts: &OptimizePipelineOptions) -> OptimizeReport`

Traversal strategy:

1. Walk pipeline tree recursively.
2. On `android_sparseimg`, compute index when missing (or when `--force`).
3. Leave other stages untouched in this slice.

## Runtime consumption design (Android sparse)

- Extend `gibblox-android-sparse` with an open path that accepts a precomputed index.
- Validate index structure and bounds against source size + sparse header.
- Build in-memory chunk map from index and serve reads without parsing chunk headers.
- Keep current lazy parsing path for `index == None`.

## Crate impact

### `crates/gibblox-pipeline`

- Add Android sparse index structs + serde + bin conversions.
- Add optimizer module/report types (Android sparse only).

### `cli`

- Add `pipeline optimize` subcommand and `--force` option.

### `crates/gibblox-android-sparse`

- Expose Android sparse index representation usable by pipeline optimizer/runtime.
- Add constructor path using precomputed index.

## Validation plan

- `cargo fmt`
- `cargo check -p gibblox-android-sparse`
- `cargo check -p gibblox-pipeline`
- `cargo check -p gibblox-cli`
- `cargo test -p gibblox-android-sparse`
- `cargo test -p gibblox-pipeline`
- `cargo test -p gibblox-cli`

Add/extend tests for:

- YAML + binary roundtrip with Android sparse index.
- optimizer populates Android sparse index.
- `--force` overwrites existing Android sparse index.
- runtime indexed Android sparse reads match lazy path output.
- runtime with invalid provided index fails deterministically.
- pipeline identity unchanged before vs after optimize.

## Acceptance criteria

1. `gibblox pipeline optimize` emits valid binary pipeline output.
2. Android sparse stages get index data populated when resolvable.
3. Android sparse runtime uses embedded index when present and skips header discovery.
4. Missing index still works via existing lazy path.
5. Invalid provided index fails stage open/read (no index-to-scan fallback).
6. Pipeline identity is unchanged by index population.
