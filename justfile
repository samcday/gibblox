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
            if dep.get("kind") in (None, "build") and dep["name"] in local_names:
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

        echo "==> cargo package -p $package --locked"
        cargo package -p "$package" --locked --config "$patch_file"
        rm -f "$patch_file"
    done

publish:
    #!/usr/bin/env bash
    set -euo pipefail

    metadata_file="$(mktemp)"
    trap 'rm -f "$metadata_file"' EXIT

    cargo metadata --format-version 1 >"$metadata_file"

    mapfile -t package_specs < <(python - "$metadata_file" <<'PY'
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
            if dep.get("kind") in (None, "build") and dep["name"] in local_names:
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
        raise SystemExit("publish aborted: local publish graph has a cycle")

    for name in order:
        print(f"{name} {packages[name]['version']}")
    PY
    )

    retry_delays=(0 60 900 1800)

    is_already_published() {
        local package_name="$1"
        local package_version="$2"

        python - "$package_name" "$package_version" <<'PY'
    import sys
    import urllib.error
    import urllib.request

    package_name = sys.argv[1]
    package_version = sys.argv[2]
    url = f"https://crates.io/api/v1/crates/{package_name}/{package_version}"

    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            if response.status == 200:
                raise SystemExit(0)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            raise SystemExit(1)
        print(
            f"warning: crates.io lookup for {package_name}@{package_version} failed with HTTP {error.code}; continuing with publish attempt",
            file=sys.stderr,
        )
        raise SystemExit(1)
    except Exception as error:
        print(
            f"warning: crates.io lookup for {package_name}@{package_version} failed ({error}); continuing with publish attempt",
            file=sys.stderr,
        )
        raise SystemExit(1)

    raise SystemExit(1)
    PY
    }

    classify_publish_failure() {
        local log_file="$1"

        python - "$log_file" <<'PY'
    import pathlib
    import re
    import sys

    text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")

    already_published_patterns = [
        r"already exists on crates\.io index",
    ]
    transient_patterns = [
        r"status 429",
        r"Too Many Requests",
        r"status 5\d\d",
        r"timed out",
        r"timeout",
        r"spurious network error",
        r"connection reset",
        r"connection refused",
        r"temporary failure",
        r"not yet available at registry",
        r"no matching package named `[^`]+` found",
    ]

    if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in already_published_patterns):
        raise SystemExit(0)

    if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in transient_patterns):
        raise SystemExit(1)

    raise SystemExit(2)
    PY
    }

    for package_spec in "${package_specs[@]}"; do
        package_name="${package_spec%% *}"
        package_version="${package_spec#* }"

        if is_already_published "$package_name" "$package_version"; then
            echo "Skipping ${package_name}@${package_version}: already published on crates.io."
            continue
        fi

        package_done=false

        for attempt_idx in "${!retry_delays[@]}"; do
            attempt=$((attempt_idx + 1))
            total_attempts="${#retry_delays[@]}"
            delay="${retry_delays[$attempt_idx]}"

            if (( delay > 0 )); then
                echo "Waiting ${delay}s before retrying ${package_name}@${package_version} (attempt ${attempt}/${total_attempts})"
                sleep "$delay"
            fi

            echo "==> cargo publish -p ${package_name} --locked (attempt ${attempt}/${total_attempts})"
            log_file="$(mktemp)"

            set +e
            cargo publish -p "$package_name" --locked 2>&1 | tee "$log_file"
            publish_status="${PIPESTATUS[0]}"
            set -e

            if [[ "$publish_status" -eq 0 ]]; then
                rm -f "$log_file"
                package_done=true
                break
            fi

            if is_already_published "$package_name" "$package_version"; then
                echo "Detected ${package_name}@${package_version} on crates.io after failed publish attempt; continuing."
                rm -f "$log_file"
                package_done=true
                break
            fi

            set +e
            classify_publish_failure "$log_file"
            failure_kind="$?"
            set -e
            rm -f "$log_file"

            if [[ "$failure_kind" -eq 0 ]]; then
                echo "Skipping ${package_name}@${package_version}: already published on crates.io."
                package_done=true
                break
            fi

            if [[ "$failure_kind" -eq 1 ]] && (( attempt < total_attempts )); then
                echo "Transient publish failure for ${package_name}@${package_version}; retrying."
                continue
            fi

            echo "Publish failed for ${package_name}@${package_version}."
            exit "$publish_status"
        done

        if [[ "$package_done" != true ]]; then
            echo "Publish failed for ${package_name}@${package_version} after ${#retry_delays[@]} attempts."
            exit 1
        fi
    done

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
