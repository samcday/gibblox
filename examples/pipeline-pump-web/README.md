# gibblox pipeline pump (web example)

This example hosts a small `index.html` UI and wasm module that:

- accepts a pipeline descriptor by URL,
- accepts a local pipeline descriptor via file picker or drag-and-drop,
- sends the descriptor to `gibblox-web-worker` via `open_pipeline`,
- pumps `read_blocks` requests as fast as possible,
- prints live throughput and request-rate stats.

It expects a binary gibblox pipeline payload (the postcard-encoded descriptor used by
`gibblox-pipeline::decode_pipeline`).

## Run locally

From this directory:

```bash
wasm-pack build --target web --out-dir pkg
python -m http.server 4173
```

Then open <http://localhost:4173>.

## One-command server

From the workspace root:

```bash
cargo run --example browser-pump
```

This starts a local HTTP server at <http://127.0.0.1:4173> and serves:

- the web app (`/`),
- the worker/module assets (`/worker.js`, `/pkg/*`),
- a generated pipeline descriptor (`/pipeline.bin`),
- a seeded noise source with HTTP range support (`/noise.bin`).

You can override the preferred starting port with `GIBBLOX_PIPELINE_PUMP_PORT`, for example:

```bash
GIBBLOX_PIPELINE_PUMP_PORT=5000 cargo run --example browser-pump
```

`/pipeline.bin` and `/noise.bin` support `seed` and `size` query params, for example:

- `/pipeline.bin?seed=42&size=268435456`
- `/noise.bin?seed=42&size=268435456`

Notes:

- URL loading depends on browser CORS rules.
- The worker script is `worker.js`, loaded as a module worker.
- The `browser-pump` example tries to run `wasm-pack build --target web --out-dir pkg` on startup.
- This package is `publish = false`.
