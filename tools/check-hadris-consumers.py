#!/usr/bin/env python3
"""Compile standalone consumers with fresh locks and isolated feature resolution."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CASES = {
    "fat": ("gobblytes-fat",),
    "mbr": ("gibblox-mbr",),
    "combined": ("gobblytes-fat", "gibblox-mbr"),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", help="Cargo target triple (defaults to the native host)")
    args = parser.parse_args()
    env = os.environ.copy()
    env.setdefault("CARGO_TARGET_DIR", str(ROOT / "target" / "hadris-consumers"))
    for case, dependencies in CASES.items():
        with tempfile.TemporaryDirectory(prefix=f"gibblox-{case}-consumer-") as directory:
            consumer = Path(directory)
            # Keep the repository's compiler/MSRV while isolating Cargo inputs.
            shutil.copyfile(ROOT / "rust-toolchain.toml", consumer / "rust-toolchain.toml")
            # The separate workspace and fresh lock are essential: neither the
            # repository's locked hadris versions nor another member's features
            # may conceal what a downstream library consumer actually resolves.
            manifest = [
                "[package]",
                f'name = "gibblox-{case}-consumer"',
                'version = "0.0.0"',
                'edition = "2024"',
                "[workspace]",
                "[dependencies]",
            ]
            for dependency in dependencies:
                path = json.dumps(str(ROOT / "crates" / dependency))
                manifest.append(f"{dependency} = {{ path = {path}, default-features = false }}")
            (consumer / "Cargo.toml").write_text("\n".join(manifest) + "\n")
            (consumer / "src").mkdir()
            (consumer / "src" / "lib.rs").write_text(
                "#![no_std]\n" + "".join(
                    f"pub use {dependency.replace('-', '_')};\n" for dependency in dependencies
                )
            )
            command = ["cargo", "check", "--manifest-path", str(consumer / "Cargo.toml")]
            if args.target:
                command.extend(["--target", args.target])
            print(f"==> fresh {case} consumer ({args.target or 'native'})", flush=True)
            subprocess.run(command, cwd=consumer, env=env, check=True)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.returncode)
