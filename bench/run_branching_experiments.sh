#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Run Chronos branching backend benchmarks for SQLite, PostgreSQL, and/or Doltgres.

Usage:
  bench/run_branching_experiments.sh [sqlite|postgres|doltgres|postgres-doltgres|both|all] [extra benchmark args...]

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
  mode:               postgres
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
  bench/run_branching_experiments.sh

  bench/run_branching_experiments.sh sqlite

  bench/run_branching_experiments.sh postgres --backends interval,copy

  bench/run_branching_experiments.sh doltgres --backends doltgres

  bench/run_branching_experiments.sh postgres-doltgres --backends copy,interval,doltgres

  bench/run_branching_experiments.sh all

  bench/run_branching_experiments.sh both --backends interval,copy
USAGE
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON:-python3}"
MODE="${1:-postgres}"
POSTGRES_CONTAINER="${CHRONOS_BENCH_POSTGRES_NAME:-chronos-branch-postgres}"
POSTGRES_IMAGE="${CHRONOS_BENCH_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_PORT="${CHRONOS_BENCH_POSTGRES_PORT:-55433}"
POSTGRES_DB="${CHRONOS_BENCH_POSTGRES_DB:-chronos_branch_test}"
POSTGRES_PASSWORD="${CHRONOS_BENCH_POSTGRES_PASSWORD:-postgres}"
POSTGRES_STARTED_BY_SCRIPT=0
DB_MEMORY="${CHRONOS_BENCH_DB_MEMORY:-10g}"
DB_BUFFER="${CHRONOS_BENCH_DB_BUFFER:-5g}"
POSTGRES_SHM_SIZE="${CHRONOS_BENCH_POSTGRES_SHM_SIZE:-${DB_MEMORY}}"
DOLTGRES_CONTAINER="${CHRONOS_BENCH_DOLTGRES_NAME:-chronos-branch-doltgres}"
DOLTGRES_IMAGE="${CHRONOS_BENCH_DOLTGRES_IMAGE:-dolthub/doltgresql:latest}"
DOLTGRES_PORT="${CHRONOS_BENCH_DOLTGRES_PORT:-55437}"
DOLTGRES_PASSWORD="${CHRONOS_BENCH_DOLTGRES_PASSWORD:-password}"
DOLTGRES_STARTED_BY_SCRIPT=0

if [[ "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

case "${MODE}" in
  sqlite|postgres|doltgres|postgres-doltgres|both|all)
    shift || true
    ;;
  *)
    echo "unknown mode: ${MODE}" >&2
    usage >&2
    exit 2
    ;;
esac

DEFAULT_ARGS=(
  --dataset-sizes 10000000
  --depths 1,4,8,16,32,64
  --widths 1,4,8,16
  --benchmark-shapes depth,width
  --read-ops 1000
  --range-read-ops 100
  --write-ops 1000
  --warmup-ops 200
  --post-branch-warmup auto
  --branch-mutations 500
)

EXTRA_ARGS=("$@")
PYTHONPATH_VALUE="${ROOT_DIR}/packages/chronos-core/src${PYTHONPATH:+:${PYTHONPATH}}"
RUN_STAMP="$(date +%Y%m%d-%H%M%S)"
RUN_OUTPUT_DIR="${CHRONOS_BENCH_OUTPUT_DIR:-${ROOT_DIR}/.benchmarks/branching-${RUN_STAMP}}"
REQUESTED_BACKENDS=""
FILTERED_EXTRA_ARGS=()

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
  echo "    shm size: ${POSTGRES_SHM_SIZE}"
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
      "${PYTHON_BIN}" bench/branching_backends.py \
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
      "${PYTHON_BIN}" bench/branching_backends.py \
        --summarize-existing \
        --output-dir "${output_dir}"
  )
}

run_sqlite=false
run_postgres=false
run_doltgres=false

case "${MODE}" in
  sqlite)
    run_sqlite=true
    ;;
  postgres)
    run_postgres=true
    ;;
  doltgres)
    run_doltgres=true
    ;;
  both)
    # Legacy mode: local SQLite plus PostgreSQL.
    run_sqlite=true
    run_postgres=true
    ;;
  postgres-doltgres|all)
    run_postgres=true
    run_doltgres=true
    ;;
esac

echo "==> Selected benchmark runs:"
if [[ "${run_sqlite}" == "true" ]]; then
  echo "    sqlite:   $(select_backends "copy,interval,log" "copy,interval,log")"
fi
if [[ "${run_postgres}" == "true" ]]; then
  echo "    postgres: $(select_backends "copy,interval,log" "copy,interval,log")"
fi
if [[ "${run_doltgres}" == "true" ]]; then
  echo "    doltgres: $(select_backends "doltgres" "doltgres")"
fi

if [[ "${run_sqlite}" == "true" ]]; then
  run_one "sqlite-branching" "sqlite:///:memory:" \
    "$(select_backends "copy,interval,log" "copy,interval,log")"
fi

if [[ "${run_postgres}" == "true" ]]; then
  if [[ -n "${CHRONOS_BRANCH_POSTGRES_DSN:-}" || -n "${CHRONOS_BRANCH_DATABASE_URL:-}" ]]; then
    POSTGRES_DSN="${CHRONOS_BRANCH_POSTGRES_DSN:-${CHRONOS_BRANCH_DATABASE_URL:-}}"
  else
    start_postgres_container
    POSTGRES_DSN="postgresql://postgres:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
  fi
  run_one "postgres-branching" "${POSTGRES_DSN}" \
    "$(select_backends "copy,interval,log" "copy,interval,log")"
fi

if [[ "${run_doltgres}" == "true" ]]; then
  if [[ -n "${CHRONOS_BRANCH_DOLTGRES_DSN:-}" ]]; then
    DOLTGRES_DSN="${CHRONOS_BRANCH_DOLTGRES_DSN}"
  else
    start_doltgres_container
    DOLTGRES_DSN="postgresql://postgres:${DOLTGRES_PASSWORD}@localhost:${DOLTGRES_PORT}/postgres"
  fi
  run_one "doltgres-branching" "${DOLTGRES_DSN}" \
    "$(select_backends "doltgres" "doltgres")"
fi

merge_results
