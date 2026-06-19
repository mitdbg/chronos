#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

export PYTHONPATH="${REPO_ROOT}/packages/chronos-core/src${PYTHONPATH:+:${PYTHONPATH}}"

cd "${REPO_ROOT}"
python3 bench/fs/fs_benchmarks.py "$@"
