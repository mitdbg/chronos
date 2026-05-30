#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Run schema-branching benchmarks for Chronos PostgreSQL backends and/or Doltgres.

Usage:
  bench/run_schema_branching_experiments.sh [postgres|doltgres|postgres-doltgres] [extra benchmark args...]

Environment:
  CHRONOS_BRANCH_POSTGRES_DSN     PostgreSQL URL used for the Chronos run.
  CHRONOS_BRANCH_DATABASE_URL     Fallback PostgreSQL URL if CHRONOS_BRANCH_POSTGRES_DSN is unset.
  CHRONOS_BRANCH_DOLTGRES_DSN     Doltgres URL used for the Doltgres run.
  CHRONOS_BENCH_DB_MEMORY         Docker memory limit. Default: 10g.
  CHRONOS_BENCH_DB_BUFFER         PostgreSQL shared_buffers and Doltgres GOMEMLIMIT budget. Default: 5g.
  CHRONOS_BENCH_POSTGRES_SHM_SIZE Docker /dev/shm size for PostgreSQL. Default: CHRONOS_BENCH_DB_MEMORY.
  CHRONOS_BENCH_POSTGRES_IMAGE    Docker image. Default: postgres:16-alpine.
  CHRONOS_BENCH_POSTGRES_NAME     Docker container name. Default: chronos-schema-postgres.
  CHRONOS_BENCH_POSTGRES_PORT     Host port. Default: 55438.
  CHRONOS_BENCH_POSTGRES_DB       Database name. Default: chronos_schema_bench.
  CHRONOS_BENCH_POSTGRES_PASSWORD Password. Default: postgres.
  CHRONOS_BENCH_POSTGRES_KEEP     Set to 1 to leave a script-started container running.
  CHRONOS_BENCH_DOLTGRES_IMAGE    Docker image. Default: dolthub/doltgresql:latest.
  CHRONOS_BENCH_DOLTGRES_NAME     Docker container name. Default: chronos-schema-doltgres.
  CHRONOS_BENCH_DOLTGRES_PORT     Host port. Default: 55439.
  CHRONOS_BENCH_DOLTGRES_PASSWORD Password. Default: password.
  CHRONOS_BENCH_DOLTGRES_KEEP     Set to 1 to leave a script-started container running.
  CHRONOS_BENCH_OUTPUT_DIR        Output root. Default: .benchmarks/schema-branching-<timestamp>.
  PYTHON                          Python executable. Default: python3.

Defaults:
  mode:          postgres-doltgres
  backends:      postgres -> interval,copy; doltgres -> doltgres
  dataset sizes: 100000,1000000
  DDL variants: no_default,default
  read ops:      100
  range reads:   20
  write ops:     100
  warmup ops:    1 before each steady-state post-DDL phase

Examples:
  bench/run_schema_branching_experiments.sh
  bench/run_schema_branching_experiments.sh postgres --quick
  bench/run_schema_branching_experiments.sh postgres-doltgres --dataset-sizes 100000,5000000
  bench/run_schema_branching_experiments.sh doltgres --ddl-variants default
USAGE
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON:-python3}"
MODE="${1:-postgres-doltgres}"
POSTGRES_CONTAINER="${CHRONOS_BENCH_POSTGRES_NAME:-chronos-schema-postgres}"
POSTGRES_IMAGE="${CHRONOS_BENCH_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_PORT="${CHRONOS_BENCH_POSTGRES_PORT:-55438}"
POSTGRES_DB="${CHRONOS_BENCH_POSTGRES_DB:-chronos_schema_bench}"
POSTGRES_PASSWORD="${CHRONOS_BENCH_POSTGRES_PASSWORD:-postgres}"
POSTGRES_STARTED_BY_SCRIPT=0
DB_MEMORY="${CHRONOS_BENCH_DB_MEMORY:-10g}"
DB_BUFFER="${CHRONOS_BENCH_DB_BUFFER:-5g}"
POSTGRES_SHM_SIZE="${CHRONOS_BENCH_POSTGRES_SHM_SIZE:-${DB_MEMORY}}"
DOLTGRES_CONTAINER="${CHRONOS_BENCH_DOLTGRES_NAME:-chronos-schema-doltgres}"
DOLTGRES_IMAGE="${CHRONOS_BENCH_DOLTGRES_IMAGE:-dolthub/doltgresql:latest}"
DOLTGRES_PORT="${CHRONOS_BENCH_DOLTGRES_PORT:-55439}"
DOLTGRES_PASSWORD="${CHRONOS_BENCH_DOLTGRES_PASSWORD:-password}"
DOLTGRES_STARTED_BY_SCRIPT=0

if [[ "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

case "${MODE}" in
  postgres|doltgres|postgres-doltgres)
    shift || true
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    usage >&2
    exit 2
    ;;
esac

DEFAULT_ARGS=(
  --dataset-sizes 100000,1000000,10000000
  --ddl-variants no_default,default
  --read-ops 1000
  --range-read-ops 200
  --write-ops 1000
  --warmup-ops 100
)
EXTRA_ARGS=("$@")
PYTHONPATH_VALUE="${ROOT_DIR}/packages/chronos-core/src${PYTHONPATH:+:${PYTHONPATH}}"
RUN_STAMP="$(date +%Y%m%d-%H%M%S)"
RUN_OUTPUT_DIR="${CHRONOS_BENCH_OUTPUT_DIR:-${ROOT_DIR}/.benchmarks/schema-branching-${RUN_STAMP}}"
case "${RUN_OUTPUT_DIR}" in
  /*) ;;
  *) RUN_OUTPUT_DIR="$(pwd)/${RUN_OUTPUT_DIR}" ;;
esac

if [[ " ${EXTRA_ARGS[*]} " == *" --quick "* ]]; then
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
    B|"") echo "${number}${unit}" ;;
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
    KB|K) echo "${number}KiB" ;;
    MB|M) echo "${number}MiB" ;;
    GB|G) echo "${number}GiB" ;;
    TB|T) echo "${number}TiB" ;;
    KIB|MIB|GIB|TIB|B) echo "${number}${unit}" ;;
    "") echo "${number}" ;;
    *) echo "${value}" ;;
  esac
}

postgres_container_running() {
  docker ps --format '{{.Names}}' | grep -Fxq "${POSTGRES_CONTAINER}"
}

postgres_container_exists() {
  docker ps -a --format '{{.Names}}' | grep -Fxq "${POSTGRES_CONTAINER}"
}

wait_for_postgres() {
  for _ in $(seq 1 60); do
    if ! postgres_container_running; then
      echo "PostgreSQL container exited before becoming ready" >&2
      docker logs --tail 120 "${POSTGRES_CONTAINER}" >&2 || true
      return 1
    fi
    if docker exec "${POSTGRES_CONTAINER}" \
      psql -U postgres -d "${POSTGRES_DB}" -Atqc "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "PostgreSQL container did not become ready in time" >&2
  return 1
}

start_postgres_container() {
  if postgres_container_running; then
    echo "==> Reusing running PostgreSQL container ${POSTGRES_CONTAINER}"
    wait_for_postgres
    return
  fi
  if postgres_container_exists; then
    docker rm "${POSTGRES_CONTAINER}" >/dev/null
  fi
  echo "==> Starting PostgreSQL container ${POSTGRES_CONTAINER} on port ${POSTGRES_PORT}"
  docker run -d \
    --name "${POSTGRES_CONTAINER}" \
    --memory "${DB_MEMORY}" \
    --shm-size "${POSTGRES_SHM_SIZE}" \
    -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
    -e POSTGRES_DB="${POSTGRES_DB}" \
    -p "${POSTGRES_PORT}:5432" \
    "${POSTGRES_IMAGE}" \
    postgres -c "shared_buffers=$(postgres_buffer_setting "${DB_BUFFER}")" >/dev/null
  POSTGRES_STARTED_BY_SCRIPT=1
  wait_for_postgres
}

doltgres_container_running() {
  docker ps --format '{{.Names}}' | grep -Fxq "${DOLTGRES_CONTAINER}"
}

doltgres_container_exists() {
  docker ps -a --format '{{.Names}}' | grep -Fxq "${DOLTGRES_CONTAINER}"
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

start_doltgres_container() {
  if doltgres_container_running; then
    echo "==> Reusing running Doltgres container ${DOLTGRES_CONTAINER}"
    wait_for_doltgres
    return
  fi
  if doltgres_container_exists; then
    docker rm "${DOLTGRES_CONTAINER}" >/dev/null
  fi
  echo "==> Starting Doltgres container ${DOLTGRES_CONTAINER} on port ${DOLTGRES_PORT}"
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

postgres_database_url() {
  if [[ -n "${CHRONOS_BRANCH_POSTGRES_DSN:-}" || -n "${CHRONOS_BRANCH_DATABASE_URL:-}" ]]; then
    echo "${CHRONOS_BRANCH_POSTGRES_DSN:-${CHRONOS_BRANCH_DATABASE_URL:-}}"
  else
    echo "postgresql://postgres:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
  fi
}

doltgres_database_url() {
  if [[ -n "${CHRONOS_BRANCH_DOLTGRES_DSN:-}" ]]; then
    echo "${CHRONOS_BRANCH_DOLTGRES_DSN}"
  else
    echo "postgresql://postgres:${DOLTGRES_PASSWORD}@localhost:${DOLTGRES_PORT}/postgres"
  fi
}

run_one() {
  local label="$1"
  local database_url="$2"
  local backend_csv="$3"
  local output_dir="${RUN_OUTPUT_DIR}/${label}"
  echo "==> Running ${label} schema branching benchmark"
  echo "    output: ${output_dir}"
  echo "    database: ${database_url}"
  echo "    backends: ${backend_csv}"
  (
    cd "${ROOT_DIR}"
    CHRONOS_BRANCH_DATABASE_URL="${database_url}" \
      PYTHONPATH="${PYTHONPATH_VALUE}" \
      "${PYTHON_BIN}" bench/schema_branching_backends.py \
        "${DEFAULT_ARGS[@]}" \
        --backends "${backend_csv}" \
        --output-dir "${output_dir}" \
        "${EXTRA_ARGS[@]}"
  )
}

merge_results() {
  local output_dir="${RUN_OUTPUT_DIR}"
  local merged="${output_dir}/results.csv"
  local wrote_header=0
  local subdir result_file
  for subdir in postgres-schema doltgres-schema; do
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
      "${PYTHON_BIN}" bench/schema_branching_backends.py \
        --summarize-existing \
        --output-dir "${output_dir}"
  )
}

RUN_ENGINES=()
case "${MODE}" in
  postgres) RUN_ENGINES=(postgres) ;;
  doltgres) RUN_ENGINES=(doltgres) ;;
  postgres-doltgres) RUN_ENGINES=(postgres doltgres) ;;
esac

echo "==> Selected schema benchmark runs:"
for engine in "${RUN_ENGINES[@]}"; do
  case "${engine}" in
    postgres) printf '    %-10s %s\n' "postgres:" "interval,copy" ;;
    doltgres) printf '    %-10s %s\n' "doltgres:" "doltgres" ;;
  esac
done

for engine in "${RUN_ENGINES[@]}"; do
  case "${engine}" in
    postgres)
      if [[ -z "${CHRONOS_BRANCH_POSTGRES_DSN:-}" && -z "${CHRONOS_BRANCH_DATABASE_URL:-}" ]]; then
        start_postgres_container
      fi
      run_one "postgres-schema" "$(postgres_database_url)" "interval,copy"
      ;;
    doltgres)
      if [[ -z "${CHRONOS_BRANCH_DOLTGRES_DSN:-}" ]]; then
        start_doltgres_container
      fi
      run_one "doltgres-schema" "$(doltgres_database_url)" "doltgres"
      ;;
  esac
done

merge_results
