#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Run Chronos branching backend benchmarks for SQLite, PostgreSQL, and/or Doltgres.

Usage:
  bench/branching/run_branching_experiments.sh [sqlite|postgres|doltgres|postgres-doltgres|both|all] [extra benchmark args...]

Environment:
  CHRONOS_BRANCH_POSTGRES_DSN     PostgreSQL URL used for the postgres run.
  CHRONOS_BRANCH_DATABASE_URL     Fallback PostgreSQL URL if CHRONOS_BRANCH_POSTGRES_DSN is unset.
  CHRONOS_BRANCH_DOLTGRES_DSN     Doltgres URL used for the doltgres run.
  CHRONOS_BENCH_DB_MEMORY         Docker memory limit for started PostgreSQL and Doltgres containers. Default: 10g.
  CHRONOS_BENCH_DB_BUFFER         Benchmark buffer budget. Sets PostgreSQL shared_buffers and Doltgres GOMEMLIMIT. Default: 5g.
  CHRONOS_BENCH_POSTGRES_SHM_SIZE Docker /dev/shm size for PostgreSQL. Default: CHRONOS_BENCH_DB_MEMORY.
  CHRONOS_BENCH_POSTGRES_IMAGE    Docker image. Default: postgres:16-alpine.
  CHRONOS_BENCH_POSTGRES_NAME     Docker container name. Default: chronos-branch-postgres.
  CHRONOS_BENCH_POSTGRES_PORT     Host port. Default: 55433.
  CHRONOS_BENCH_POSTGRES_DB       Database name. Default: chronos_branch_test.
  CHRONOS_BENCH_POSTGRES_PASSWORD Password. Default: postgres.
  CHRONOS_BENCH_POSTGRES_MAINTENANCE_WORK_MEM
                                  PostgreSQL maintenance_work_mem. Default: 512MB.
  CHRONOS_BENCH_POSTGRES_MAX_PARALLEL_MAINTENANCE_WORKERS
                                  PostgreSQL max_parallel_maintenance_workers. Default: 4.
  CHRONOS_BENCH_POSTGRES_MAX_PARALLEL_WORKERS
                                  PostgreSQL max_parallel_workers. Default: 8.
  CHRONOS_BENCH_POSTGRES_MAX_WORKER_PROCESSES
                                  PostgreSQL max_worker_processes. Default: 16.
  CHRONOS_BENCH_POSTGRES_MIN_PARALLEL_TABLE_SCAN_SIZE
                                  PostgreSQL min_parallel_table_scan_size. Default: 0.
  CHRONOS_BENCH_POSTGRES_MIN_PARALLEL_INDEX_SCAN_SIZE
                                  PostgreSQL min_parallel_index_scan_size. Default: 0.
  CHRONOS_BENCH_POSTGRES_KEEP     Set to 1 to leave a script-started container running.
  CHRONOS_BENCH_DOLTGRES_IMAGE    Docker image. Default: dolthub/doltgresql:latest.
  CHRONOS_BENCH_DOLTGRES_NAME     Docker container name. Default: chronos-branch-doltgres.
  CHRONOS_BENCH_DOLTGRES_PORT     Host port. Default: 55437.
  CHRONOS_BENCH_DOLTGRES_PASSWORD Password. Default: password.
  CHRONOS_BENCH_DOLTGRES_KEEP     Set to 1 to leave a script-started container running.
  CHRONOS_BENCH_OUTPUT_DIR        Output root for all benchmark result folders.
                                  Default: .benchmarks/branching-<timestamp>.
  PYTHON                        Python executable. Default: python3.

Defaults:
  mode:               postgres-doltgres
  dataset sizes:      10000000
  depths:             1,4,8,16,32
  widths:             1,4,8,16,32
  read ops:           1000
  range read ops:     100
  write ops:          1000
  warmup ops:         200
  post-branch warmup: auto
  branch mutations:   500

Examples:
  bench/branching/run_branching_experiments.sh

  bench/branching/run_branching_experiments.sh sqlite

  bench/branching/run_branching_experiments.sh postgres --backends interval,copy,orpheus

  bench/branching/run_branching_experiments.sh doltgres --backends doltgres

  bench/branching/run_branching_experiments.sh postgres-doltgres --backends interval,doltgres,copy,orpheus

  bench/branching/run_branching_experiments.sh all

  bench/branching/run_branching_experiments.sh both --backends interval,copy,orpheus,litetree
USAGE
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ -n "${PYTHON:-}" ]]; then
  PYTHON_BIN="${PYTHON}"
elif [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
  PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

MODE="postgres-doltgres"
if [[ $# -gt 0 && "${1}" != -* ]]; then
  MODE="$1"
  shift
fi
POSTGRES_CONTAINER="${CHRONOS_BENCH_POSTGRES_NAME:-chronos-branch-postgres}"
POSTGRES_IMAGE="${CHRONOS_BENCH_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_PORT="${CHRONOS_BENCH_POSTGRES_PORT:-55433}"
POSTGRES_DB="${CHRONOS_BENCH_POSTGRES_DB:-chronos_branch_test}"
POSTGRES_PASSWORD="${CHRONOS_BENCH_POSTGRES_PASSWORD:-postgres}"
POSTGRES_STARTED_BY_SCRIPT=0
DB_MEMORY="${CHRONOS_BENCH_DB_MEMORY:-10g}"
DB_BUFFER="${CHRONOS_BENCH_DB_BUFFER:-5g}"
POSTGRES_SHM_SIZE="${CHRONOS_BENCH_POSTGRES_SHM_SIZE:-${DB_MEMORY}}"
POSTGRES_MAINTENANCE_WORK_MEM="${CHRONOS_BENCH_POSTGRES_MAINTENANCE_WORK_MEM:-512MB}"
POSTGRES_MAX_PARALLEL_MAINTENANCE_WORKERS="${CHRONOS_BENCH_POSTGRES_MAX_PARALLEL_MAINTENANCE_WORKERS:-4}"
POSTGRES_MAX_PARALLEL_WORKERS="${CHRONOS_BENCH_POSTGRES_MAX_PARALLEL_WORKERS:-8}"
POSTGRES_MAX_WORKER_PROCESSES="${CHRONOS_BENCH_POSTGRES_MAX_WORKER_PROCESSES:-16}"
POSTGRES_MIN_PARALLEL_TABLE_SCAN_SIZE="${CHRONOS_BENCH_POSTGRES_MIN_PARALLEL_TABLE_SCAN_SIZE:-0}"
POSTGRES_MIN_PARALLEL_INDEX_SCAN_SIZE="${CHRONOS_BENCH_POSTGRES_MIN_PARALLEL_INDEX_SCAN_SIZE:-0}"
DOLTGRES_CONTAINER="${CHRONOS_BENCH_DOLTGRES_NAME:-chronos-branch-doltgres}"
DOLTGRES_IMAGE="${CHRONOS_BENCH_DOLTGRES_IMAGE:-dolthub/doltgresql:latest}"
DOLTGRES_PORT="${CHRONOS_BENCH_DOLTGRES_PORT:-55437}"
DOLTGRES_PASSWORD="${CHRONOS_BENCH_DOLTGRES_PASSWORD:-password}"
DOLTGRES_STARTED_BY_SCRIPT=0

case "${MODE}" in
  sqlite|postgres|doltgres|postgres-doltgres|both|all)
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    usage >&2
    exit 2
    ;;
esac

DEFAULT_ARGS=(
  --dataset-sizes 5000000
  --depths 1,4,8,16,32
  --widths 1,4,8,16
  --benchmark-shapes depth,width
  --read-ops 500
  --range-read-ops 100
  --write-ops 500
  --warmup-ops 200
  --post-branch-warmup auto
  --branch-mutations 1000
)

EXTRA_ARGS=("$@")
PYTHONPATH_VALUE="${ROOT_DIR}/packages/chronos-core/src${PYTHONPATH:+:${PYTHONPATH}}"
RUN_STAMP="$(date +%Y%m%d-%H%M%S)"
RUN_OUTPUT_DIR="${CHRONOS_BENCH_OUTPUT_DIR:-${ROOT_DIR}/.benchmarks/branching-${RUN_STAMP}}"
case "${RUN_OUTPUT_DIR}" in
  /*) ;;
  *) RUN_OUTPUT_DIR="$(pwd)/${RUN_OUTPUT_DIR}" ;;
esac
REQUESTED_BACKENDS=""
FILTERED_EXTRA_ARGS=()
SQLITE_SUPPORTED_BACKENDS="interval,litetree,copy"
SQLITE_DEFAULT_BACKENDS="${SQLITE_SUPPORTED_BACKENDS}"
POSTGRES_SUPPORTED_BACKENDS="interval,orpheus,copy"
POSTGRES_DEFAULT_BACKENDS="${POSTGRES_SUPPORTED_BACKENDS}"
DOLTGRES_SUPPORTED_BACKENDS="doltgres"
DOLTGRES_DEFAULT_BACKENDS="${DOLTGRES_SUPPORTED_BACKENDS}"

for ((i = 0; i < ${#EXTRA_ARGS[@]}; i++)); do
  arg="${EXTRA_ARGS[$i]}"
  case "${arg}" in
    --backends=*)
      REQUESTED_BACKENDS="${arg#--backends=}"
      ;;
    --backends)
      i=$((i + 1))
      REQUESTED_BACKENDS="${EXTRA_ARGS[$i]:-}"
      ;;
    *)
      FILTERED_EXTRA_ARGS+=("${arg}")
      ;;
  esac
done

if [[ " ${FILTERED_EXTRA_ARGS[*]} " == *" --quick "* ]]; then
  DEFAULT_ARGS=()
fi

cleanup() {
  if [[ "${POSTGRES_STARTED_BY_SCRIPT}" == "1" && "${CHRONOS_BENCH_POSTGRES_KEEP:-0}" != "1" ]]; then
    echo "==> Removing PostgreSQL container ${POSTGRES_CONTAINER}"
    docker rm -f "${POSTGRES_CONTAINER}" >/dev/null || true
  fi
  if [[ "${DOLTGRES_STARTED_BY_SCRIPT}" == "1" && "${CHRONOS_BENCH_DOLTGRES_KEEP:-0}" != "1" ]]; then
    echo "==> Removing Doltgres container ${DOLTGRES_CONTAINER}"
    docker rm -f "${DOLTGRES_CONTAINER}" >/dev/null || true
  fi
}

trap cleanup EXIT

wait_for_postgres() {
  for _ in $(seq 1 60); do
    if ! postgres_container_running; then
      echo "PostgreSQL container exited before becoming ready" >&2
      docker logs --tail 120 "${POSTGRES_CONTAINER}" >&2 || true
      return 1
    fi
    if docker exec "${POSTGRES_CONTAINER}" pg_isready -U postgres -d "${POSTGRES_DB}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "PostgreSQL container did not become ready in time" >&2
  return 1
}

postgres_buffer_setting() {
  local value="$1"
  local number unit
  number="$(echo "${value}" | sed -E 's/^([0-9]+).*/\1/')"
  unit="$(echo "${value}" | sed -E 's/^[0-9]+[[:space:]]*//')"
  unit="$(echo "${unit}" | tr '[:lower:]' '[:upper:]' | tr -d '[:space:]')"
  case "${unit}" in
    K|KB|KIB)
      echo "${number}kB"
      ;;
    M|MB|MIB)
      echo "${number}MB"
      ;;
    G|GB|GIB)
      echo "${number}GB"
      ;;
    T|TB|TIB)
      echo "${number}TB"
      ;;
    B|"")
      echo "${number}${unit}"
      ;;
    *)
      echo "${value}"
      ;;
  esac
}

go_memory_limit_from_buffer() {
  local value="$1"
  local number unit
  number="$(echo "${value}" | sed -E 's/^([0-9]+).*/\1/')"
  unit="$(echo "${value}" | sed -E 's/^[0-9]+[[:space:]]*//')"
  unit="$(echo "${unit}" | tr '[:lower:]' '[:upper:]' | tr -d '[:space:]')"
  case "${unit}" in
    KB|K)
      echo "${number}KiB"
      ;;
    MB|M)
      echo "${number}MiB"
      ;;
    GB|G)
      echo "${number}GiB"
      ;;
    TB|T)
      echo "${number}TiB"
      ;;
    KIB|MIB|GIB|TIB|B)
      echo "${number}${unit}"
      ;;
    "")
      echo "${number}"
      ;;
    *)
      echo "${value}"
      ;;
  esac
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
  echo "    memory limit: ${DB_MEMORY}"
  echo "    shared_buffers: $(postgres_buffer_setting "${DB_BUFFER}")"
  echo "    maintenance_work_mem: ${POSTGRES_MAINTENANCE_WORK_MEM}"
  echo "    max_parallel_maintenance_workers: ${POSTGRES_MAX_PARALLEL_MAINTENANCE_WORKERS}"
  echo "    max_parallel_workers: ${POSTGRES_MAX_PARALLEL_WORKERS}"
  echo "    max_worker_processes: ${POSTGRES_MAX_WORKER_PROCESSES}"
  echo "    shm size: ${POSTGRES_SHM_SIZE}"
  docker run -d \
    --name "${POSTGRES_CONTAINER}" \
    --memory "${DB_MEMORY}" \
    --shm-size "${POSTGRES_SHM_SIZE}" \
    -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
    -e POSTGRES_DB="${POSTGRES_DB}" \
    -p "${POSTGRES_PORT}:5432" \
    "${POSTGRES_IMAGE}" \
    postgres \
      -c "shared_buffers=$(postgres_buffer_setting "${DB_BUFFER}")" \
      -c "maintenance_work_mem=${POSTGRES_MAINTENANCE_WORK_MEM}" \
      -c "max_parallel_maintenance_workers=${POSTGRES_MAX_PARALLEL_MAINTENANCE_WORKERS}" \
      -c "max_parallel_workers=${POSTGRES_MAX_PARALLEL_WORKERS}" \
      -c "max_worker_processes=${POSTGRES_MAX_WORKER_PROCESSES}" \
      -c "min_parallel_table_scan_size=${POSTGRES_MIN_PARALLEL_TABLE_SCAN_SIZE}" \
      -c "min_parallel_index_scan_size=${POSTGRES_MIN_PARALLEL_INDEX_SCAN_SIZE}" >/dev/null
  POSTGRES_STARTED_BY_SCRIPT=1
  wait_for_postgres
}

wait_for_doltgres() {
  for _ in $(seq 1 90); do
    if ! doltgres_container_running; then
      echo "Doltgres container exited before becoming ready" >&2
      docker logs --tail 120 "${DOLTGRES_CONTAINER}" >&2 || true
      return 1
    fi
    if docker exec "${DOLTGRES_CONTAINER}" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Doltgres container did not become ready in time" >&2
  return 1
}

doltgres_container_running() {
  docker ps --format '{{.Names}}' | grep -Fxq "${DOLTGRES_CONTAINER}"
}

doltgres_container_exists() {
  docker ps -a --format '{{.Names}}' | grep -Fxq "${DOLTGRES_CONTAINER}"
}

start_doltgres_container() {
  if doltgres_container_running; then
    echo "==> Reusing running Doltgres container ${DOLTGRES_CONTAINER}"
    wait_for_doltgres
    return
  fi

  if doltgres_container_exists; then
    echo "==> Removing stopped Doltgres container ${DOLTGRES_CONTAINER}"
    docker rm "${DOLTGRES_CONTAINER}" >/dev/null
  fi

  echo "==> Starting Doltgres container ${DOLTGRES_CONTAINER} on port ${DOLTGRES_PORT}"
  echo "    memory limit: ${DB_MEMORY}"
  echo "    GOMEMLIMIT: $(go_memory_limit_from_buffer "${DB_BUFFER}")"
  docker run -d \
    --name "${DOLTGRES_CONTAINER}" \
    --memory "${DB_MEMORY}" \
    -e GOMEMLIMIT="$(go_memory_limit_from_buffer "${DB_BUFFER}")" \
    -e DOLTGRES_PASSWORD="${DOLTGRES_PASSWORD}" \
    -p "${DOLTGRES_PORT}:5432" \
    "${DOLTGRES_IMAGE}" >/dev/null
  DOLTGRES_STARTED_BY_SCRIPT=1
  wait_for_doltgres
}

select_backends() {
  local supported_csv="$1"
  local default_csv="$2"
  local requested="${REQUESTED_BACKENDS:-${default_csv}}"
  local selected=()
  IFS=',' read -ra requested_items <<< "${requested}"
  IFS=',' read -ra supported_items <<< "${supported_csv}"
  for requested_item in "${requested_items[@]}"; do
    requested_item="$(echo "${requested_item}" | xargs)"
    for supported_item in "${supported_items[@]}"; do
      if [[ "${requested_item}" == "${supported_item}" ]]; then
        selected+=("${requested_item}")
      fi
    done
  done
  local joined=""
  for item in "${selected[@]}"; do
    if [[ -n "${joined}" ]]; then
      joined+=","
    fi
    joined+="${item}"
  done
  echo "${joined}"
}

engine_label() {
  case "$1" in
    sqlite)
      echo "sqlite-branching"
      ;;
    postgres)
      echo "postgres-branching"
      ;;
    doltgres)
      echo "doltgres-branching"
      ;;
    *)
      echo "unknown engine: $1" >&2
      return 2
      ;;
  esac
}

engine_supported_backends() {
  case "$1" in
    sqlite)
      echo "${SQLITE_SUPPORTED_BACKENDS}"
      ;;
    postgres)
      echo "${POSTGRES_SUPPORTED_BACKENDS}"
      ;;
    doltgres)
      echo "${DOLTGRES_SUPPORTED_BACKENDS}"
      ;;
    *)
      echo "unknown engine: $1" >&2
      return 2
      ;;
  esac
}

engine_default_backends() {
  case "$1" in
    sqlite)
      echo "${SQLITE_DEFAULT_BACKENDS}"
      ;;
    postgres)
      echo "${POSTGRES_DEFAULT_BACKENDS}"
      ;;
    doltgres)
      echo "${DOLTGRES_DEFAULT_BACKENDS}"
      ;;
    *)
      echo "unknown engine: $1" >&2
      return 2
      ;;
  esac
}

engine_database_url() {
  local engine="$1"
  case "${engine}" in
    sqlite)
      echo "sqlite:///:memory:"
      ;;
    postgres)
      if [[ -n "${CHRONOS_BRANCH_POSTGRES_DSN:-}" || -n "${CHRONOS_BRANCH_DATABASE_URL:-}" ]]; then
        echo "${CHRONOS_BRANCH_POSTGRES_DSN:-${CHRONOS_BRANCH_DATABASE_URL:-}}"
      else
        start_postgres_container >&2
        echo "postgresql://postgres:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
      fi
      ;;
    doltgres)
      if [[ -n "${CHRONOS_BRANCH_DOLTGRES_DSN:-}" ]]; then
        echo "${CHRONOS_BRANCH_DOLTGRES_DSN}"
      else
        start_doltgres_container >&2
        echo "postgresql://postgres:${DOLTGRES_PASSWORD}@localhost:${DOLTGRES_PORT}/postgres"
      fi
      ;;
    *)
      echo "unknown engine: ${engine}" >&2
      return 2
      ;;
  esac
}

selected_backends_for_engine() {
  local engine="$1"
  select_backends "$(engine_supported_backends "${engine}")" \
    "$(engine_default_backends "${engine}")"
}

run_engine() {
  local engine="$1"
  local backend_csv label database_url
  backend_csv="$(selected_backends_for_engine "${engine}")"
  label="$(engine_label "${engine}")"
  database_url="$(engine_database_url "${engine}")"
  run_one "${label}" "${database_url}" "${backend_csv}"
}

run_one() {
  local label="$1"
  local database_url="$2"
  local backend_csv="$3"
  local output_dir="${RUN_OUTPUT_DIR}/${label}"

  if [[ -z "${backend_csv}" ]]; then
    echo "==> Skipping ${label}; no requested backends are supported by this database"
    return
  fi

  echo "==> Running ${label} branching benchmark"
  echo "    output: ${output_dir}"
  echo "    database: ${database_url}"
  echo "    backends: ${backend_csv}"

  (
    cd "${ROOT_DIR}"
    CHRONOS_BRANCH_DATABASE_URL="${database_url}" \
      PYTHONPATH="${PYTHONPATH_VALUE}" \
      "${PYTHON_BIN}" bench/branching/branching_backends.py \
        "${DEFAULT_ARGS[@]}" \
        --backends "${backend_csv}" \
        --output-dir "${output_dir}" \
        "${FILTERED_EXTRA_ARGS[@]}"
  )
}
#        "--include-join-aggregate" \

merge_results() {
  local output_dir="${RUN_OUTPUT_DIR}"
  local merged="${output_dir}/results.csv"
  local wrote_header=0
  local subdir result_file

  for subdir in sqlite-branching postgres-branching doltgres-branching; do
    result_file="${output_dir}/${subdir}/results.csv"
    [[ -f "${result_file}" ]] || continue
    if [[ "${wrote_header}" == "0" ]]; then
      head -n 1 "${result_file}" > "${merged}"
      wrote_header=1
    fi
    tail -n +2 "${result_file}" >> "${merged}"
  done

  if [[ "${wrote_header}" == "0" ]]; then
    echo "No benchmark result files found under ${output_dir}" >&2
    return 1
  fi

  (
    cd "${ROOT_DIR}"
    PYTHONPATH="${PYTHONPATH_VALUE}" \
      "${PYTHON_BIN}" bench/branching/branching_backends.py \
        --summarize-existing \
        --output-dir "${output_dir}"
  )
}

RUN_ENGINES=()

case "${MODE}" in
  sqlite)
    RUN_ENGINES=(sqlite)
    ;;
  postgres)
    RUN_ENGINES=(postgres)
    ;;
  doltgres)
    RUN_ENGINES=(doltgres)
    ;;
  both)
    RUN_ENGINES=(sqlite postgres)
    ;;
  postgres-doltgres|all)
    RUN_ENGINES=(postgres doltgres)
    ;;
esac

echo "==> Selected benchmark runs:"
for engine in "${RUN_ENGINES[@]}"; do
  printf '    %-8s %s\n' "${engine}:" "$(selected_backends_for_engine "${engine}")"
done

for engine in "${RUN_ENGINES[@]}"; do
  run_engine "${engine}"
done

merge_results
