# Pipelines

`gibblox` pipelines describe *how to open a read-only block source* by composing one or more
source/wrapper/selectors.

At a high level:

- A leaf source provides bytes (`http`, `file`, `casync`).
- Wrappers transform another source (`xz`, `android_sparseimg`).
- Selectors pick a partition from a container (`mbr`, `gpt`).

Pipelines are expressed as YAML for authoring and as a compact binary format for transport.

## YAML shape

Pipelines use a one-key-object style where each stage wraps the next stage.

```yaml
gpt:
  partlabel: rootfs
  android_sparseimg:
    xz:
      http: https://cdn.example.invalid/device.img.xz
```

That reads as:

1. Fetch `device.img.xz` over HTTP.
2. Decompress the XZ stream.
3. Interpret the result as an Android sparse image.
4. Select the GPT partition labeled `rootfs`.

## Common examples

### Plain HTTP image

```yaml
http: https://cdn.example.invalid/images/rootfs.img
```

### Local file in GPT partition index 2

```yaml
gpt:
  index: 2
  file: ./artifacts/disk.img
```

### casync image with separate chunk store

```yaml
casync:
  index: https://cdn.example.invalid/indexes/rootfs.caibx
  chunk_store: https://cdn.example.invalid/chunks/
```

### casync image with implicit chunk store

```yaml
casync:
  index: https://cdn.example.invalid/indexes/rootfs.caibx
```

When `chunk_store` is omitted, the CLI derives it from the index location.

## CLI usage

The `gibblox-cli` has a `pipeline` command group for working with pipeline definitions.

### Validate YAML

```bash
cargo run -p gibblox-cli -- pipeline validate pipeline.yaml
```

### Encode YAML to binary (`.gbxp`)

```bash
cargo run -p gibblox-cli -- pipeline encode pipeline.yaml -o pipeline.gbxp
```

### Decode binary back to YAML

```bash
cargo run -p gibblox-cli -- pipeline decode pipeline.gbxp -o pipeline.yaml
```

### Validate a binary pipeline

```bash
cargo run -p gibblox-cli -- pipeline validate pipeline.gbxp --binary
```

### Resolve and stream a pipeline

With no subcommand, `gibblox-cli` treats the input as a pipeline (YAML or binary), validates it,
opens it, and streams the resolved bytes to output:

```bash
# to stdout
cargo run -p gibblox-cli -- pipeline.yaml > image.bin

# to file
cargo run -p gibblox-cli -- pipeline.yaml --output image.bin
```

## Notes

- Pipelines are strictly read-only.
- Validation is deterministic and rejects malformed selector combinations.
- Encode/decode/validate commands do not perform network I/O; they only parse and validate
  descriptors.
