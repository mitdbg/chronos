#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${repo_root}/deploy/chronos-lake/docker-compose.yml"
python_bin="${PYTHON_BIN:-${repo_root}/.venv/bin/python}"
api_port="${MINIO_API_PORT:-19000}"
console_port="${MINIO_CONSOLE_PORT:-19001}"
project_name="chronos-lake-test"

cleanup() {
  if [[ "${KEEP_MINIO:-0}" != "1" ]]; then
    MINIO_API_PORT="${api_port}" MINIO_CONSOLE_PORT="${console_port}" \
      docker compose -p "${project_name}" -f "${compose_file}" down --volumes
  fi
}
trap cleanup EXIT

MINIO_API_PORT="${api_port}" MINIO_CONSOLE_PORT="${console_port}" \
  docker compose -p "${project_name}" -f "${compose_file}" up -d --wait

CHRONOS_TEST_S3_ENDPOINT="http://127.0.0.1:${api_port}" \
  "${python_bin}" -m pytest -q "${repo_root}/tests/test_chronos_lake.py"
