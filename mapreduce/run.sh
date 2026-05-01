#!/usr/bin/env bash
# Build the mapper and reducer binaries, then run the wordcount MapReduce driver.
#
# Options (set as env vars or flags):
#   MAPPERS=4 REDUCERS=3 ./mapreduce/run.sh
#   ./mapreduce/run.sh -mappers 8 -reducers 4
#
# The leader is assumed to be running on localhost:8080.
# Override with LEADER= and PORT=.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Configurable defaults ────────────────────────────────────────────────────
MAPPERS="${MAPPERS:-4}"
REDUCERS="${REDUCERS:-3}"
LEADER="${LEADER:-localhost}"
PORT="${PORT:-8080}"
INPUT="${INPUT:-$ROOT/data/shakespeare.txt}"
TOP="${TOP:-20}"

# Remaining args (e.g. -mappers 8 -reducers 4) are passed straight to the driver.
EXTRA_ARGS=("$@")

MAPPER_BIN="$SCRIPT_DIR/bin/mapper"
REDUCER_BIN="$SCRIPT_DIR/bin/reducer"

# ── Build ────────────────────────────────────────────────────────────────────
mkdir -p "$SCRIPT_DIR/bin"

echo "Building mapper..."
go build -o "$MAPPER_BIN" "$ROOT/mapreduce/mapper/"

echo "Building reducer..."
go build -o "$REDUCER_BIN" "$ROOT/mapreduce/reducer/"

echo "Building driver..."
go build -o "$SCRIPT_DIR/bin/driver" "$ROOT/mapreduce/driver/"

echo ""

# ── Run ──────────────────────────────────────────────────────────────────────
exec "$SCRIPT_DIR/bin/driver" \
    -mapper   "$MAPPER_BIN"  \
    -reducer  "$REDUCER_BIN" \
    -input    "$INPUT"       \
    -mappers  "$MAPPERS"     \
    -reducers "$REDUCERS"    \
    -leader   "$LEADER"      \
    -port     "$PORT"        \
    -top      "$TOP"         \
    "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"
