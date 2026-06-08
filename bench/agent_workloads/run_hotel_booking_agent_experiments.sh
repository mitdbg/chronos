#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Run the hotel-booking agent transaction benchmark.

Usage:
  bench/agent_workloads/run_hotel_booking_agent_experiments.sh [postgres] [extra args...]

Environment:
  CHRONOS_BRANCH_POSTGRES_DSN      Existing PostgreSQL URL. If unset, the runner starts a container.
  CHRONOS_BENCH_POSTGRES_IMAGE     Default: postgres:18-alpine.
  CHRONOS_BENCH_POSTGRES_NAME      Default: chronos-hotel-agent-postgres.
  CHRONOS_BENCH_POSTGRES_PORT      Default: 55449.
  CHRONOS_BENCH_POSTGRES_DB        Default: chronos_hotel_agent.
  CHRONOS_BENCH_POSTGRES_PASSWORD  Default: postgres.
  CHRONOS_BENCH_POSTGRES_KEEP      Set to 1 to keep a runner-started PostgreSQL container.
  CHRONOS_HOTEL_BRANCH_BACKEND     Chronos branch backend. Default: interval.
  PYTHON                           Python executable. Default: python3.

Defaults:
  model:             openrouter/deepseek/deepseek-v4-flash
  backends:          big_txn,saga,branch
  conflict mix:      disjoint,arithmetic,capacity,policy
  requests:          8
  parallel agents:   4
  max agent steps:   5
  max retries:       1

Examples:
  bench/agent_workloads/run_hotel_booking_agent_experiments.sh --requests 12 --parallel-agents 4
  bench/agent_workloads/run_hotel_booking_agent_experiments.sh postgres --quick
  bench/agent_workloads/run_hotel_booking_agent_experiments.sh postgres --requests 32 --parallel-agents 8 --scripted-agent
  bench/agent_workloads/run_hotel_booking_agent_experiments.sh postgres --requests 12 --conflict-mix disjoint,policy
  bench/agent_workloads/run_hotel_booking_agent_experiments.sh --requests 12 --max-retries 2
USAGE
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON:-python3}"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

MODE="postgres"
if [[ $# -gt 0 && "${1}" != -* ]]; then
  MODE="$1"
  shift
fi

case "${MODE}" in
  postgres)
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    usage >&2
    exit 2
    ;;
esac

POSTGRES_CONTAINER="${CHRONOS_BENCH_POSTGRES_NAME:-chronos-hotel-agent-postgres}"
POSTGRES_IMAGE="${CHRONOS_BENCH_POSTGRES_IMAGE:-postgres:18-alpine}"
POSTGRES_PORT="${CHRONOS_BENCH_POSTGRES_PORT:-55449}"
POSTGRES_DB="${CHRONOS_BENCH_POSTGRES_DB:-chronos_hotel_agent}"
POSTGRES_PASSWORD="${CHRONOS_BENCH_POSTGRES_PASSWORD:-postgres}"
POSTGRES_STARTED_BY_SCRIPT=0

cleanup() {
  if [[ "${POSTGRES_STARTED_BY_SCRIPT}" == "1" && "${CHRONOS_BENCH_POSTGRES_KEEP:-0}" != "1" ]]; then
    docker rm -f "${POSTGRES_CONTAINER}" >/dev/null || true
  fi
}
trap cleanup EXIT

container_running() {
  docker ps --format '{{.Names}}' | grep -Fxq "$1"
}

container_exists() {
  docker ps -a --format '{{.Names}}' | grep -Fxq "$1"
}

wait_for_postgres() {
  for _ in $(seq 1 60); do
    if docker exec "${POSTGRES_CONTAINER}" pg_isready -U postgres -d "${POSTGRES_DB}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  docker logs --tail 120 "${POSTGRES_CONTAINER}" >&2 || true
  echo "PostgreSQL container did not become ready" >&2
  return 1
}

start_postgres_container() {
  if container_running "${POSTGRES_CONTAINER}"; then
    wait_for_postgres
    return
  fi
  if container_exists "${POSTGRES_CONTAINER}"; then
    docker rm -f "${POSTGRES_CONTAINER}" >/dev/null
  fi
  docker run --rm -d \
    --name "${POSTGRES_CONTAINER}" \
    -e "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}" \
    -e "POSTGRES_DB=${POSTGRES_DB}" \
    -p "${POSTGRES_PORT}:5432" \
    "${POSTGRES_IMAGE}" \
    >/dev/null
  POSTGRES_STARTED_BY_SCRIPT=1
  wait_for_postgres
}

if [[ -z "${CHRONOS_BRANCH_POSTGRES_DSN:-}" ]]; then
  start_postgres_container
  export CHRONOS_BRANCH_POSTGRES_DSN="postgresql://postgres:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}"
fi

export PYTHONPATH="${ROOT_DIR}/packages/chronos-core/src${PYTHONPATH:+:${PYTHONPATH}}"
cd "${ROOT_DIR}"
exec "${PYTHON_BIN}" "bench/agent_workloads/hotel_booking_agent.py" \
  --postgres-url "${CHRONOS_BRANCH_POSTGRES_DSN}" \
  "$@"
