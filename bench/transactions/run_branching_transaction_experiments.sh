#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Run single-threaded branching-transaction benchmarks.

Usage:
  bench/transactions/run_branching_transaction_experiments.sh [postgres|doltgres|postgres-doltgres] [extra args...]

Backends:
  postgres            chronos,native_txn
  doltgres            doltgres
  postgres-doltgres   chronos,doltgres,native_txn

Environment:
  CHRONOS_BRANCH_POSTGRES_DSN      Existing PostgreSQL URL. If unset, the runner starts a container.
  CHRONOS_BRANCH_DOLTGRES_DSN      Existing Doltgres URL. If unset, the runner starts a container.
  CHRONOS_BENCH_POSTGRES_IMAGE     Default: postgres:18-alpine.
  CHRONOS_BENCH_POSTGRES_NAME      Default: chronos-branch-txn-postgres.
  CHRONOS_BENCH_POSTGRES_PORT      Default: 55443.
  CHRONOS_BENCH_POSTGRES_DB        Default: chronos_branch_txn.
  CHRONOS_BENCH_POSTGRES_PASSWORD  Default: postgres.
  CHRONOS_BENCH_POSTGRES_KEEP      Set to 1 to keep a runner-started PostgreSQL container.
  CHRONOS_BENCH_DOLTGRES_IMAGE     Default: dolthub/doltgresql:latest.
  CHRONOS_BENCH_DOLTGRES_NAME      Default: chronos-branch-txn-doltgres.
  CHRONOS_BENCH_DOLTGRES_PORT      Default: 55447.
  CHRONOS_BENCH_DOLTGRES_PASSWORD  Default: password.
  CHRONOS_BENCH_DOLTGRES_KEEP      Set to 1 to keep a runner-started Doltgres container.
  CHRONOS_BENCH_DB_MEMORY          Docker memory limit. Default: 10g.
  CHRONOS_BENCH_DB_BUFFER          PostgreSQL shared_buffers / Doltgres GOMEMLIMIT. Default: 5g.
  CHRONOS_BENCH_DOCKER_DEVICE      Required Docker storage backing device. Default: /dev/nvme0n1p2.
  CHRONOS_BENCH_MEMORY_SAMPLE_INTERVAL
                                  Seconds between Docker memory samples. Default: 1.0.
  CHRONOS_BENCH_OUTPUT_DIR         Output directory. Default: .benchmarks/branching-transaction-<timestamp>.
  PYTHON                           Python executable. Default: python3.

Defaults:
  dataset sizes: 10000,100000,1000000
  iterations:    5
  changes:       100 point updates per branch transaction
  read count:    100 point reads per branch transaction
  Chronos interval_child_width: 2

Examples:
  bench/transactions/run_branching_transaction_experiments.sh postgres-doltgres
  bench/transactions/run_branching_transaction_experiments.sh postgres --quick
  bench/transactions/run_branching_transaction_experiments.sh postgres-doltgres --dataset-sizes 100000,1000000 --iterations 10
USAGE
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON:-python3}"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

MODE="postgres-doltgres"
if [[ $# -gt 0 && "${1}" != -* ]]; then
  MODE="$1"
  shift
fi

case "${MODE}" in
  postgres|doltgres|postgres-doltgres)
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    usage >&2
    exit 2
    ;;
esac

POSTGRES_CONTAINER="${CHRONOS_BENCH_POSTGRES_NAME:-chronos-branch-txn-postgres}"
POSTGRES_IMAGE="${CHRONOS_BENCH_POSTGRES_IMAGE:-postgres:18-alpine}"
POSTGRES_PORT="${CHRONOS_BENCH_POSTGRES_PORT:-55443}"
POSTGRES_DB="${CHRONOS_BENCH_POSTGRES_DB:-chronos_branch_txn}"
POSTGRES_PASSWORD="${CHRONOS_BENCH_POSTGRES_PASSWORD:-postgres}"
POSTGRES_STARTED_BY_SCRIPT=0

DOLTGRES_CONTAINER="${CHRONOS_BENCH_DOLTGRES_NAME:-chronos-branch-txn-doltgres}"
DOLTGRES_IMAGE="${CHRONOS_BENCH_DOLTGRES_IMAGE:-dolthub/doltgresql:latest}"
DOLTGRES_PORT="${CHRONOS_BENCH_DOLTGRES_PORT:-55447}"
DOLTGRES_PASSWORD="${CHRONOS_BENCH_DOLTGRES_PASSWORD:-password}"
DOLTGRES_STARTED_BY_SCRIPT=0

DB_MEMORY="${CHRONOS_BENCH_DB_MEMORY:-10g}"
DB_BUFFER="${CHRONOS_BENCH_DB_BUFFER:-5g}"
POSTGRES_SHM_SIZE="${CHRONOS_BENCH_POSTGRES_SHM_SIZE:-${DB_MEMORY}}"
DOCKER_STORAGE_DEVICE="${CHRONOS_BENCH_DOCKER_DEVICE:-/dev/nvme0n1p2}"
DISALLOWED_DB_STORAGE_PREFIXES=(
  "/mnt/dbfork-nvme-xfs"
  "/mnt/dbfork-xfs"
)

cleanup() {
  if [[ "${POSTGRES_STARTED_BY_SCRIPT}" == "1" && "${CHRONOS_BENCH_POSTGRES_KEEP:-0}" != "1" ]]; then
    docker rm -f "${POSTGRES_CONTAINER}" >/dev/null || true
  fi
  if [[ "${DOLTGRES_STARTED_BY_SCRIPT}" == "1" && "${CHRONOS_BENCH_DOLTGRES_KEEP:-0}" != "1" ]]; then
    docker rm -f "${DOLTGRES_CONTAINER}" >/dev/null || true
  fi
}
trap cleanup EXIT

container_running() {
  docker ps --format '{{.Names}}' | grep -Fxq "$1"
}

container_exists() {
  docker ps -a --format '{{.Names}}' | grep -Fxq "$1"
}

ensure_docker_storage_device() {
  local docker_root source
  docker_root="$(docker info --format '{{.DockerRootDir}}')"
  source="$(findmnt -n -T "${docker_root}" -o SOURCE)"
  if [[ "${source}" != "${DOCKER_STORAGE_DEVICE}" ]]; then
    echo "Docker data root ${docker_root} is backed by ${source}, expected ${DOCKER_STORAGE_DEVICE}." >&2
    echo "Refusing to start benchmark containers on the wrong storage device." >&2
    return 1
  fi
}

ensure_container_storage_allowed() {
  local container="$1"
  local source prefix
  while IFS= read -r source; do
    [[ -z "${source}" ]] && continue
    for prefix in "${DISALLOWED_DB_STORAGE_PREFIXES[@]}"; do
      if [[ "${source}" == "${prefix}" || "${source}" == "${prefix}/"* ]]; then
        echo "Container ${container} has a bind mount under disallowed storage ${prefix}: ${source}" >&2
        echo "Stop/remove it before rerunning so this benchmark uses Docker storage on ${DOCKER_STORAGE_DEVICE}." >&2
        return 1
      fi
    done
  done < <(docker inspect --format '{{range .Mounts}}{{println .Source}}{{end}}' "${container}")
}

postgres_buffer_setting() {
  local value="$1"
  local number unit
  number="$(echo "${value}" | sed -E 's/^([0-9]+).*/\1/')"
  unit="$(echo "${value}" | sed -E 's/^[0-9]+[[:space:]]*//')"
  unit="$(echo "${unit}" | tr '[:lower:]' '[:upper:]' | tr -d '[:space:]')"
  case "${unit}" in
    K|KB|KIB) echo "${number}kB" ;;
    M|MB|MIB) echo "${number}MB" ;;
    G|GB|GIB) echo "${number}GB" ;;
    T|TB|TIB) echo "${number}TB" ;;
    *) echo "${value}" ;;
  esac
}

go_memory_limit_from_buffer() {
  local value="$1"
  local number unit
  number="$(echo "${value}" | sed -E 's/^([0-9]+).*/\1/')"
  unit="$(echo "${value}" | sed -E 's/^[0-9]+[[:space:]]*//')"
  unit="$(echo "${unit}" | tr '[:lower:]' '[:upper:]' | tr -d '[:space:]')"
  case "${unit}" in
    K|KB|KIB) echo "${number}KiB" ;;
    M|MB|MIB) echo "${number}MiB" ;;
    G|GB|GIB) echo "${number}GiB" ;;
    T|TB|TIB) echo "${number}TiB" ;;
    *) echo "${value}" ;;
  esac
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

wait_for_doltgres() {
  for _ in $(seq 1 90); do
    if docker exec "${DOLTGRES_CONTAINER}" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  docker logs --tail 120 "${DOLTGRES_CONTAINER}" >&2 || true
  echo "Doltgres container did not become ready" >&2
  return 1
}

start_postgres_container() {
  ensure_docker_storage_device
  if container_running "${POSTGRES_CONTAINER}"; then
    ensure_container_storage_allowed "${POSTGRES_CONTAINER}"
    wait_for_postgres
    return
  fi
  if container_exists "${POSTGRES_CONTAINER}"; then
    docker rm -f "${POSTGRES_CONTAINER}" >/dev/null
  fi
  docker run --rm -d \
    --name "${POSTGRES_CONTAINER}" \
    --memory "${DB_MEMORY}" \
    --shm-size "${POSTGRES_SHM_SIZE}" \
    -e "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}" \
    -e "POSTGRES_DB=${POSTGRES_DB}" \
    -p "${POSTGRES_PORT}:5432" \
    "${POSTGRES_IMAGE}" \
    -c "shared_buffers=$(postgres_buffer_setting "${DB_BUFFER}")" \
    >/dev/null
  POSTGRES_STARTED_BY_SCRIPT=1
  wait_for_postgres
}

start_doltgres_container() {
  ensure_docker_storage_device
  if container_running "${DOLTGRES_CONTAINER}"; then
    ensure_container_storage_allowed "${DOLTGRES_CONTAINER}"
    wait_for_doltgres
    return
  fi
  if container_exists "${DOLTGRES_CONTAINER}"; then
    docker rm -f "${DOLTGRES_CONTAINER}" >/dev/null
  fi
  docker run --rm -d \
    --name "${DOLTGRES_CONTAINER}" \
    --memory "${DB_MEMORY}" \
    -e "DOLTGRES_PASSWORD=${DOLTGRES_PASSWORD}" \
    -e "GOMEMLIMIT=$(go_memory_limit_from_buffer "${DB_BUFFER}")" \
    -p "${DOLTGRES_PORT}:5432" \
    "${DOLTGRES_IMAGE}" \
    >/dev/null
  DOLTGRES_STARTED_BY_SCRIPT=1
  wait_for_doltgres
}

RUN_STAMP="$(date +%Y%m%d-%H%M%S)"
OUTPUT_DIR="${CHRONOS_BENCH_OUTPUT_DIR:-${ROOT_DIR}/.benchmarks/branching-transaction-${RUN_STAMP}}"

ARGS=("$@")
case "${MODE}" in
  postgres)
    BACKENDS="native_txn,chronos"
    ;;
  doltgres)
    BACKENDS="doltgres"
    ;;
  postgres-doltgres)
    BACKENDS="native_txn,chronos,doltgres"
    ;;
esac

if [[ "${MODE}" == "postgres" || "${MODE}" == "postgres-doltgres" ]]; then
  if [[ -z "${CHRONOS_BRANCH_POSTGRES_DSN:-}" ]]; then
    start_postgres_container
    export CHRONOS_BRANCH_POSTGRES_DSN="postgresql://postgres:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
  fi
fi

if [[ "${MODE}" == "doltgres" || "${MODE}" == "postgres-doltgres" ]]; then
  if [[ -z "${CHRONOS_BRANCH_DOLTGRES_DSN:-}" ]]; then
    start_doltgres_container
    export CHRONOS_BRANCH_DOLTGRES_DSN="postgresql://postgres:${DOLTGRES_PASSWORD}@localhost:${DOLTGRES_PORT}/postgres"
  fi
fi

export PYTHONPATH="${ROOT_DIR}/packages/chronos-core/src${PYTHONPATH:+:${PYTHONPATH}}"

"${PYTHON_BIN}" "${ROOT_DIR}/bench/transactions/branching_transactions.py" \
  --backends "${BACKENDS}" \
  --output-dir "${OUTPUT_DIR}" \
  --postgres-container "${POSTGRES_CONTAINER}" \
  --doltgres-container "${DOLTGRES_CONTAINER}" \
  "${ARGS[@]}"

echo "RESULT_DIR=${OUTPUT_DIR}"
