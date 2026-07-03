#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
if [[ -n "${PYTHON:-}" ]]; then
  PYTHON_BIN="${PYTHON}"
elif [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
  PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

export PYTHONPATH="${REPO_ROOT}/packages/chronos-core/src${PYTHONPATH:+:${PYTHONPATH}}"

cd "${REPO_ROOT}"
"${PYTHON_BIN}" bench/fs/fs_benchmarks.py "$@"
