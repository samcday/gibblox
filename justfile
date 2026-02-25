set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

# Bump workspace version (supports semver and semver-rc.N)
# Also updates workspace crate versions in [workspace.dependencies].
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

    before="$(cksum Cargo.toml)"

    python - "$version" <<'PY'
    import re
    import sys
    from pathlib import Path
    import tomllib

    target = sys.argv[1]
    root = Path("Cargo.toml")
    text = root.read_text(encoding="utf-8")
    doc = tomllib.loads(text)

    workspace = doc.get("workspace")
    if not isinstance(workspace, dict):
        raise SystemExit("workspace table not found in Cargo.toml")

    members = workspace.get("members")
    if not isinstance(members, list):
        raise SystemExit("workspace.members not found in Cargo.toml")

    member_names = set()
    for member in members:
        manifest = Path(member) / "Cargo.toml"
        if not manifest.exists():
            continue
        member_doc = tomllib.loads(manifest.read_text(encoding="utf-8"))
        package = member_doc.get("package")
        if isinstance(package, dict):
            name = package.get("name")
            if isinstance(name, str):
                member_names.add(name)

    new_text, package_updates = re.subn(
        r'(?ms)(^\[workspace\.package\]\n(?:.*\n)*?^version\s*=\s*")[^"]*(")',
        lambda m: f"{m.group(1)}{target}{m.group(2)}",
        text,
        count=1,
    )
    if package_updates != 1:
        raise SystemExit("workspace.package version line not found in Cargo.toml")

    lines = new_text.splitlines()
    in_workspace_deps = False
    seen = set()

    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_workspace_deps = stripped == "[workspace.dependencies]"
            continue
        if not in_workspace_deps:
            continue

        key_match = re.match(r"^([A-Za-z0-9_.-]+)\s*=\s*\{", stripped)
        if key_match is None:
            continue

        dep_name = key_match.group(1)
        if dep_name not in member_names:
            continue

        seen.add(dep_name)
        if re.search(r'\bversion\s*=\s*"[^"]*"', line):
            lines[idx] = re.sub(
                r'(\bversion\s*=\s*")[^"]*(")',
                lambda m: f"{m.group(1)}{target}{m.group(2)}",
                line,
                count=1,
            )
        else:
            lines[idx] = re.sub(
                r"\{",
                '{ version = "' + target + '", ',
                line,
                count=1,
            )

    missing = sorted(member_names - seen)
    if missing:
        raise SystemExit(
            "Missing workspace.dependencies entries for workspace members: "
            + ", ".join(missing)
        )

    result = "\n".join(lines)
    if new_text.endswith("\n"):
        result += "\n"

    if result != text:
        root.write_text(result, encoding="utf-8")
    PY

    after="$(cksum Cargo.toml)"
    if [[ "$before" == "$after" ]]; then
        echo "Workspace version and workspace crate dependency versions already set to $version"
        exit 0
    fi

    cargo generate-lockfile
    echo "Bumped workspace version and workspace crate dependency versions to $version"

publish-dry-run:
    #!/usr/bin/env bash
    set -euo pipefail

    metadata_file="$(mktemp)"
    trap 'rm -f "$metadata_file"' EXIT

    cargo metadata --format-version 1 >"$metadata_file"

    mapfile -t packages < <(python - "$metadata_file" <<'PY'
    import collections
    import json
    import pathlib
    import sys

    metadata_path = pathlib.Path(sys.argv[1])
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    root = pathlib.Path(".").resolve()

    def in_repo(path: pathlib.Path) -> bool:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False

    local_names = set()
    packages = {pkg["name"]: pkg for pkg in metadata["packages"]}

    for pkg in metadata["packages"]:
        manifest_path = pathlib.Path(pkg["manifest_path"]).resolve()
        if pkg.get("source") is not None:
            continue
        if pkg.get("publish") == []:
            continue
        if not in_repo(manifest_path):
            continue
        local_names.add(pkg["name"])

    deps = {name: set() for name in local_names}
    reverse = {name: set() for name in local_names}

    for name in local_names:
        for dep in packages[name].get("dependencies", []):
            if dep.get("kind") is None and dep["name"] in local_names:
                deps[name].add(dep["name"])
                reverse[dep["name"]].add(name)

    indegree = {name: len(deps[name]) for name in local_names}
    queue = collections.deque(sorted(name for name, degree in indegree.items() if degree == 0))
    order = []

    while queue:
        name = queue.popleft()
        order.append(name)
        for dependent in sorted(reverse[name]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                queue.append(dependent)

    if len(order) != len(local_names):
        raise SystemExit("publish-dry-run aborted: local publish graph has a cycle")

    print("\n".join(order))
    PY
    )

    for package in "${packages[@]}"; do
        patch_file="$(mktemp)"
        python - "$metadata_file" "$package" "$patch_file" <<'PY'
    import json
    import pathlib
    import sys

    metadata_path = pathlib.Path(sys.argv[1])
    current_package = sys.argv[2]
    patch_path = pathlib.Path(sys.argv[3])

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    root = pathlib.Path(".").resolve()

    def in_repo(path: pathlib.Path) -> bool:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False

    local_packages = {}
    for pkg in metadata["packages"]:
        manifest_path = pathlib.Path(pkg["manifest_path"]).resolve()
        if pkg.get("source") is not None:
            continue
        if pkg.get("publish") == []:
            continue
        if not in_repo(manifest_path):
            continue
        local_packages[pkg["name"]] = pkg

    if current_package not in local_packages:
        raise SystemExit(f"package {current_package!r} is not a local publishable package")

    needed = set()
    stack = [
        dep["name"]
        for dep in local_packages[current_package].get("dependencies", [])
        if dep.get("kind") in (None, "build") and dep["name"] in local_packages
    ]

    while stack:
        name = stack.pop()
        if name in needed:
            continue
        needed.add(name)
        for dep in local_packages[name].get("dependencies", []):
            if dep.get("kind") in (None, "build") and dep["name"] in local_packages:
                stack.append(dep["name"])

    lines = ["[patch.crates-io]"]
    for name in sorted(needed):
        manifest_path = pathlib.Path(local_packages[name]["manifest_path"]).resolve()
        lines.append(name + ' = { path = "' + manifest_path.parent.as_posix() + '" }')

    patch_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    PY

        echo "==> cargo publish -p $package --locked --dry-run"
        cargo publish -p "$package" --locked --dry-run --config "$patch_file"
        rm -f "$patch_file"
    done

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
