#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT_DIR="$SCRIPT_DIR/build"

mkdir -p "$OUT_DIR"

find "$SCRIPT_DIR" -mindepth 1 -maxdepth 1 -type d | while read -r dir; do
    name="$(basename "$dir")"
    if [[ "$name" == "build" ]]; then
        continue
    fi
    echo "Building $name..."
    go build -o "$OUT_DIR/$name" "$REPO_ROOT/testbins/$name"
done

echo "Done. Binaries in $OUT_DIR/"
