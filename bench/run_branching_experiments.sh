#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Run Janus branching backend benchmarks for SQLite and/or PostgreSQL.

Usage:
  bench/run_branching_experiments.sh [sqlite|postgres|both] [extra benchmark args...]

Environment:
  JANUS_BRANCH_POSTGRES_DSN     PostgreSQL URL used for the postgres run.
  JANUS_BRANCH_DATABASE_URL     Fallback PostgreSQL URL if JANUS_BRANCH_POSTGRES_DSN is unset.
  JANUS_BENCH_POSTGRES_IMAGE    Docker image. Default: postgres:16-alpine.
  JANUS_BENCH_POSTGRES_NAME     Docker container name. Default: janus-branch-postgres.
  JANUS_BENCH_POSTGRES_PORT     Host port. Default: 55433.
  JANUS_BENCH_POSTGRES_DB       Database name. Default: janus_branch_test.
  JANUS_BENCH_POSTGRES_PASSWORD Password. Default: postgres.
  JANUS_BENCH_POSTGRES_KEEP     Set to 1 to leave a script-started container running.
  PYTHON                        Python executable. Default: python3.

Defaults:
  mode:               postgres
  dataset sizes:      100000
  depths:             1,4,8
  read ops:           5000
  write ops:          5000
  branch mutations:   100

Examples:
  bench/run_branching_experiments.sh

  bench/run_branching_experiments.sh sqlite

  bench/run_branching_experiments.sh postgres --backends interval,copy

  bench/run_branching_experiments.sh both --backends interval,copy
USAGE
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON:-python3}"
MODE="${1:-postgres}"
POSTGRES_CONTAINER="${JANUS_BENCH_POSTGRES_NAME:-janus-branch-postgres}"
POSTGRES_IMAGE="${JANUS_BENCH_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_PORT="${JANUS_BENCH_POSTGRES_PORT:-55433}"
POSTGRES_DB="${JANUS_BENCH_POSTGRES_DB:-janus_branch_test}"
POSTGRES_PASSWORD="${JANUS_BENCH_POSTGRES_PASSWORD:-postgres}"
POSTGRES_STARTED_BY_SCRIPT=0

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

cleanup() {
  if [[ "${POSTGRES_STARTED_BY_SCRIPT}" == "1" && "${JANUS_BENCH_POSTGRES_KEEP:-0}" != "1" ]]; then
    echo "==> Stopping PostgreSQL container ${POSTGRES_CONTAINER}"
    docker stop "${POSTGRES_CONTAINER}" >/dev/null || true
  fi
}

trap cleanup EXIT

wait_for_postgres() {
  for _ in $(seq 1 60); do
    if docker exec "${POSTGRES_CONTAINER}" pg_isready -U postgres -d "${POSTGRES_DB}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "PostgreSQL container did not become ready in time" >&2
  return 1
}

postgres_container_running() {
  docker ps --format '{{.Names}}' | grep -Fxq "${POSTGRES_CONTAINER}"
}

postgres_container_exists() {
  docker ps -a --format '{{.Names}}' | grep -Fxq "${POSTGRES_CONTAINER}"
}

start_postgres_container() {
  if postgres_container_running; then
    echo "==> Reusing running PostgreSQL container ${POSTGRES_CONTAINER}"
    wait_for_postgres
    return
  fi

  if postgres_container_exists; then
    echo "==> Removing stopped PostgreSQL container ${POSTGRES_CONTAINER}"
    docker rm "${POSTGRES_CONTAINER}" >/dev/null
  fi

  echo "==> Starting PostgreSQL container ${POSTGRES_CONTAINER} on port ${POSTGRES_PORT}"
  docker run --rm -d \
    --name "${POSTGRES_CONTAINER}" \
    -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
    -e POSTGRES_DB="${POSTGRES_DB}" \
    -p "${POSTGRES_PORT}:5432" \
    "${POSTGRES_IMAGE}" >/dev/null
  POSTGRES_STARTED_BY_SCRIPT=1
  wait_for_postgres
}

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
  if [[ -n "${JANUS_BRANCH_POSTGRES_DSN:-}" || -n "${JANUS_BRANCH_DATABASE_URL:-}" ]]; then
    POSTGRES_DSN="${JANUS_BRANCH_POSTGRES_DSN:-${JANUS_BRANCH_DATABASE_URL:-}}"
  else
    start_postgres_container
    POSTGRES_DSN="postgresql://postgres:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
  fi
  run_one "postgres-branching" "${POSTGRES_DSN}"
fi
