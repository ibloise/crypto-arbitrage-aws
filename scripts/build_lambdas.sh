#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"

if [[ $# -eq 0 ]]; then
    set -- all
fi

exec "$PYTHON_BIN" "$ROOT_DIR/tools/build_lambdas.py" "$@"
