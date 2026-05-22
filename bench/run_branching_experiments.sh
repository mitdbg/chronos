#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Run Janus branching backend benchmarks for SQLite and/or PostgreSQL.

Usage:
  bench/run_branching_experiments.sh [sqlite|postgres|both] [extra benchmark args...]

Environment:
  JANUS_BRANCH_POSTGRES_DSN   PostgreSQL URL used for the postgres run.
  JANUS_BRANCH_DATABASE_URL   Fallback PostgreSQL URL if JANUS_BRANCH_POSTGRES_DSN is unset.
  PYTHON                      Python executable. Default: python3.

Defaults:
  dataset sizes:      100000
  depths:             1,4,8
  read ops:           5000
  write ops:          5000
  branch mutations:   100

Examples:
  bench/run_branching_experiments.sh sqlite

  JANUS_BRANCH_POSTGRES_DSN=postgresql://postgres:postgres@localhost:55433/janus_branch_test \
    bench/run_branching_experiments.sh postgres

  bench/run_branching_experiments.sh both --backends interval,copy
USAGE
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON:-python3}"
MODE="${1:-both}"

if [[ "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

case "${MODE}" in
  sqlite|postgres|both)
    shift || true
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    usage >&2
    exit 2
    ;;
esac

DEFAULT_ARGS=(
  --dataset-sizes 100000
  --depths 1,4,8
  --read-ops 5000
  --write-ops 5000
  --branch-mutations 100
)

EXTRA_ARGS=("$@")
PYTHONPATH_VALUE="${ROOT_DIR}/packages/janus-core/src${PYTHONPATH:+:${PYTHONPATH}}"
RUN_STAMP="$(date +%Y%m%d-%H%M%S)"

run_one() {
  local label="$1"
  local database_url="$2"
  local output_dir="${ROOT_DIR}/.benchmarks/${label}-${RUN_STAMP}"

  echo "==> Running ${label} branching benchmark"
  echo "    output: ${output_dir}"
  echo "    database: ${database_url}"

  (
    cd "${ROOT_DIR}"
    JANUS_BRANCH_DATABASE_URL="${database_url}" \
      PYTHONPATH="${PYTHONPATH_VALUE}" \
      "${PYTHON_BIN}" bench/branching_backends.py \
        "${DEFAULT_ARGS[@]}" \
        --output-dir "${output_dir}" \
        "${EXTRA_ARGS[@]}"
  )
}

if [[ "${MODE}" == "sqlite" || "${MODE}" == "both" ]]; then
  run_one "sqlite-branching" "sqlite:///:memory:"
fi

if [[ "${MODE}" == "postgres" || "${MODE}" == "both" ]]; then
  POSTGRES_DSN="${JANUS_BRANCH_POSTGRES_DSN:-${JANUS_BRANCH_DATABASE_URL:-}}"
  if [[ -z "${POSTGRES_DSN}" ]]; then
    echo "postgres mode requires JANUS_BRANCH_POSTGRES_DSN or JANUS_BRANCH_DATABASE_URL" >&2
    exit 2
  fi
  run_one "postgres-branching" "${POSTGRES_DSN}"
fi
