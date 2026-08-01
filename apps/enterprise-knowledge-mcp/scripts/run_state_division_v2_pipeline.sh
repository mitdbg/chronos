#!/usr/bin/env bash
set -Eeuo pipefail

repo=/home/ubuntu/TAR-OS/chronos
app="$repo/apps/enterprise-knowledge-mcp"
artifact_root="$repo/.enterprise-knowledge/enterprise-rag-curated-v2"
document_snapshot="$repo/.enterprise-knowledge/enterprise-rag-full-staged-v1/snapshot"
code_snapshot="$artifact_root/code-snapshot"
traces="$app/workflows/curated-v2/traces/full-zero-v2"
run_id="${ENTERPRISE_RUN_ID:-state-division-v2}"
output_root="$artifact_root/benchmarks/$run_id"
persistent_runtime_root="$repo/.enterprise-knowledge/runtime"
runtime_image="${ENTERPRISE_RUNTIME_IMAGE:-$persistent_runtime_root/state-division-v2.btrfs.img}"
runtime_root="${ENTERPRISE_RUNTIME_ROOT:-/mnt/chronos-enterprise-state-division-v2}"
runtime_image_size="${ENTERPRISE_RUNTIME_IMAGE_SIZE:-80G}"
host_runtime_root="$persistent_runtime_root/state-division-v2-host"
state_root="$host_runtime_root/states"
service_root="$host_runtime_root/services"
work_root="$host_runtime_root/work"
replay_workdir="$host_runtime_root/replay-workdir"
log="$output_root/pipeline.log"
status="$output_root/status.tsv"

qdrant_container=chronos-enterprise-state-v2-qdrant
qdrant_port=6340
qdrant_grpc_port=6341
qdrant_url="http://127.0.0.1:$qdrant_port"
# Qdrant manages its own WAL and immutable segments. Keeping those files on
# the host filesystem avoids an unnecessary loop-mounted CoW layer; Btrfs is
# reserved for branchable document workspaces.
qdrant_storage="$persistent_runtime_root/state-division-v2-qdrant"

dolt_container=chronos-enterprise-state-v2-doltgres
dolt_port=55440
dolt_database=chronos_enterprise_state_v2
dolt_storage="$service_root/doltgres"
dolt_admin_dsn="postgresql://postgres:password@127.0.0.1:$dolt_port/postgres"
dolt_dsn="postgresql://postgres:password@127.0.0.1:$dolt_port/$dolt_database"
native_workspaces="$runtime_root/native-workspaces"

cli="$repo/.venv/bin/chronos-enterprise-knowledge"
python="$repo/.venv/bin/python"
summarizer="$app/workflows/real/summarize_results.py"
suite_summarizer="$app/scripts/summarize_workflow_suite.py"
runner="$app/scripts/run_full_workflow_benchmarks.py"
merger="$app/scripts/merge_workflow_backend_results.py"
embedding_model="sentence-transformers/all-MiniLM-L6-v2#onnx:onnx/model_qint8_avx512.onnx"
batch_size=${ENTERPRISE_INGEST_BATCH_SIZE:-512}
resume=${ENTERPRISE_RESUME:-1}
document_max_documents=${ENTERPRISE_DOCUMENT_MAX_DOCUMENTS:-}
code_max_documents=${ENTERPRISE_CODE_MAX_DOCUMENTS:-}
backend_selection=${ENTERPRISE_BACKENDS:-chronos,native-branching,app-managed}
workflow_start=${ENTERPRISE_WORKFLOW_START:-1}
workflow_end=${ENTERPRISE_WORKFLOW_END:-13}
sqlite_buffer_kib=${ENTERPRISE_SQLITE_BUFFER_KIB:-4194304}
doltgres_memory_limit=${ENTERPRISE_DOLTGRES_MEMORY_LIMIT:-4g}
doltgres_go_memory_limit=${ENTERPRISE_DOLTGRES_GO_MEMORY_LIMIT:-4GiB}
qdrant_memory_limit=${ENTERPRISE_QDRANT_MEMORY_LIMIT:-8g}
qdrant_max_optimization_threads=${ENTERPRISE_QDRANT_MAX_OPTIMIZATION_THREADS:-4}
qdrant_max_indexing_threads=${ENTERPRISE_QDRANT_MAX_INDEXING_THREADS:-4}
qdrant_upload_workers=${ENTERPRISE_QDRANT_UPLOAD_WORKERS:-4}
qdrant_service_workers=${ENTERPRISE_QDRANT_SERVICE_WORKERS:-8}
qdrant_shards=${ENTERPRISE_QDRANT_SHARDS:-4}
qdrant_point_batch_size=${ENTERPRISE_QDRANT_POINT_BATCH_SIZE:-256}
qdrant_bulk_hnsw_m=${ENTERPRISE_QDRANT_BULK_HNSW_M:-0}
qdrant_query_hnsw_m=${ENTERPRISE_QDRANT_QUERY_HNSW_M:-0}
# Keep sparse mutable segments bounded. A zero threshold disables optimizer
# conversion and makes BM25 insertion progressively slower as the corpus grows.
qdrant_indexing_threshold_kib=${ENTERPRISE_QDRANT_INDEXING_THRESHOLD_KIB:-10000}
bm25_threads=${ENTERPRISE_BM25_THREADS:-4}

# SQLite exposes a per-connection page-cache target rather than a server-wide
# buffer pool. Apply the same target to native Chronos connections and the
# application-managed SQLite baseline.
export CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB="$sqlite_buffer_kib"
export CHRONOS_APP_SQLITE_CACHE_SIZE_KIB="$sqlite_buffer_kib"
export CHRONOS_QDRANT_MAX_OPTIMIZATION_THREADS="$qdrant_max_optimization_threads"
export CHRONOS_QDRANT_MAX_INDEXING_THREADS="$qdrant_max_indexing_threads"
export CHRONOS_QDRANT_UPLOAD_WORKERS="$qdrant_upload_workers"
export CHRONOS_QDRANT_SHARD_NUMBER="$qdrant_shards"
export CHRONOS_QDRANT_POINT_BATCH_SIZE="$qdrant_point_batch_size"
export CHRONOS_QDRANT_INDEXING_THRESHOLD_KB="$qdrant_indexing_threshold_kib"
export CHRONOS_QDRANT_HNSW_M="$qdrant_bulk_hnsw_m"
export CHRONOS_QDRANT_GRPC_PORT="$qdrant_grpc_port"
export CHRONOS_QDRANT_PREFER_GRPC=1
export CHRONOS_BM25_THREADS="$bm25_threads"
export CHRONOS_INGEST_PROFILE=1

mkdir -p "$output_root" "$persistent_runtime_root"
touch "$log" "$status"

required_document_ids_file="$output_root/required-company-document-ids.txt"
required_code_ids_file="$output_root/required-code-document-ids.txt"
declare -a required_document_ids=()
declare -a required_code_ids=()

ensure_btrfs_runtime() {
  local filesystem
  filesystem=$(findmnt -rn -M "$runtime_root" -o FSTYPE 2>/dev/null || true)
  if [[ -n "$filesystem" ]]; then
    if [[ "$filesystem" != btrfs ]]; then
      printf '%s is mounted as %s, expected btrfs\n' \
        "$runtime_root" "$filesystem" >&2
      return 1
    fi
    if ! findmnt -rn -M "$runtime_root" -o OPTIONS |
      tr ',' '\n' |
      grep -qx user_subvol_rm_allowed; then
      sudo mount -o remount,user_subvol_rm_allowed "$runtime_root"
    fi
    return
  fi

  sudo mkdir -p "$runtime_root"
  if [[ ! -e "$runtime_image" ]]; then
    truncate -s "$runtime_image_size" "$runtime_image"
    sudo mkfs.btrfs -q -f "$runtime_image"
  elif ! sudo blkid -p "$runtime_image" | grep -q 'TYPE="btrfs"'; then
    printf '%s is not a Btrfs filesystem image\n' "$runtime_image" >&2
    return 1
  fi
  sudo mount \
    -o loop,noatime,compress=zstd,user_subvol_rm_allowed \
    "$runtime_image" "$runtime_root"
  sudo chown "$(id -u):$(id -g)" "$runtime_root"
}

ensure_btrfs_runtime
mkdir -p \
  "$runtime_root" \
  "$host_runtime_root" \
  "$state_root" \
  "$service_root" \
  "$work_root" \
  "$replay_workdir"
cd "$repo"

log_phase() {
  printf '%s phase=%s\n' "$(date -u +%FT%TZ)" "$1" | tee -a "$log"
}

timed_phase() {
  local backend=$1
  local phase=$2
  shift 2
  local started ended elapsed result
  started=$(date +%s%N)
  log_phase "$backend:$phase:start"
  set +e
  "$@" >>"$log" 2>&1
  result=$?
  set -e
  ended=$(date +%s%N)
  elapsed=$((ended - started))
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "$(date -u +%FT%TZ)" "$backend" "$phase" "$elapsed" "$result" \
    | tee -a "$status"
  if [[ "$result" -ne 0 ]]; then
    log_phase "$backend:$phase:failed"
    return "$result"
  fi
  log_phase "$backend:$phase:complete"
}

delete_tree_contents() {
  local path=$1
  mkdir -p "$path"
  find "$path" -mindepth 1 -depth -delete
}

delete_service_contents() {
  local path=$1
  mkdir -p "$path"
  sudo find "$path" -mindepth 1 -depth -delete
}

delete_btrfs_workspace_contents() {
  local relative path
  local -a subvolumes=()
  mkdir -p "$native_workspaces"
  mapfile -t subvolumes < <(
    sudo btrfs subvolume list -o "$native_workspaces" |
      sed -n 's/^.* path //p' |
      sort -r
  )
  for relative in "${subvolumes[@]}"; do
    path="$runtime_root/$relative"
    btrfs property set "$path" ro false >/dev/null 2>&1 || true
    btrfs subvolume delete "$path"
  done
  find "$native_workspaces" -mindepth 1 -depth -delete
}

wait_for_docker() {
  local deadline=$((SECONDS + 300))
  until docker info >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      printf 'Docker did not become ready\n' >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_qdrant() {
  # Large on-disk collections may need several minutes to replay their update
  # journal after an interrupted write.
  local deadline=$((SECONDS + 14400))
  until curl -fsS "$qdrant_url/collections" >/dev/null; do
    if (( SECONDS >= deadline )); then
      printf 'Qdrant did not become ready\n' >&2
      return 1
    fi
    sleep 2
  done
}

tune_existing_qdrant_collections() {
  local collection
  while IFS= read -r collection; do
    [[ -n "$collection" ]] || continue
    curl -fsS -X PATCH \
      "$qdrant_url/collections/$collection" \
      -H 'content-type: application/json' \
      -d "{
        \"optimizers_config\": {
          \"indexing_threshold\": $qdrant_indexing_threshold_kib,
          \"max_optimization_threads\": $qdrant_max_optimization_threads
        },
        \"hnsw_config\": {
          \"m\": $qdrant_bulk_hnsw_m,
          \"max_indexing_threads\": $qdrant_max_indexing_threads
        }
      }" >/dev/null
  done < <(
    curl -fsS "$qdrant_url/collections" |
      "$python" -c '
import json
import sys

for item in json.load(sys.stdin)["result"]["collections"]:
    print(item["name"])
'
  )
}

wait_for_qdrant_idle() {
  local deadline pending
  deadline=$((SECONDS + 14400))
  while true; do
    pending=$(
      curl --max-time 30 -fsS "$qdrant_url/collections" |
        "$python" -c '
import json
import sys
import urllib.request

url = sys.argv[1]
collections = json.load(sys.stdin)["result"]["collections"]
pending = []
for item in collections:
    name = item["name"]
    try:
        with urllib.request.urlopen(
            f"{url}/collections/{name}", timeout=30
        ) as response:
            state = json.load(response)["result"]
    except Exception as error:
        pending.append(f"{name}:unavailable:{type(error).__name__}")
        continue
    queue = int((state.get("update_queue") or {}).get("length") or 0)
    if state.get("status") != "green" or queue:
        pending.append("{}:{}:queue={}".format(
            name, state.get("status"), queue
        ))
print(",".join(pending))
' "$qdrant_url" 2>/dev/null || printf 'collection-list:unavailable'
    )
    [[ -n "$pending" ]] || break
    if (( SECONDS >= deadline )); then
      printf 'Qdrant optimization did not finish: %s\n' "$pending" >&2
      return 1
    fi
    printf '%s qdrant_optimization_pending %s\n' \
      "$(date -u +%FT%TZ)" "$pending" | tee -a "$log"
    sleep 10
  done
}

finalize_qdrant_storage() {
  local collection
  # This benchmark deliberately uses zero-filled placeholder embeddings.
  # Dense similarity therefore carries no signal, so keep HNSW disabled and
  # serve retrieval through the sparse BM25 index.
  export CHRONOS_QDRANT_HNSW_M="$qdrant_query_hnsw_m"
  while IFS= read -r collection; do
    [[ -n "$collection" ]] || continue
    curl -fsS -X PATCH \
      "$qdrant_url/collections/$collection" \
      -H 'content-type: application/json' \
      -d "{
        \"hnsw_config\": {
          \"m\": $qdrant_query_hnsw_m,
          \"max_indexing_threads\": $qdrant_max_indexing_threads
        },
        \"optimizers_config\": {
          \"indexing_threshold\": $qdrant_indexing_threshold_kib,
          \"max_optimization_threads\": $qdrant_max_optimization_threads
        }
      }" >/dev/null
  done < <(
    curl -fsS "$qdrant_url/collections" |
      "$python" -c '
import json
import sys

for item in json.load(sys.stdin)["result"]["collections"]:
    print(item["name"])
'
  )
  wait_for_qdrant_idle
}

start_qdrant() {
  local preserve=${1:-0}
  wait_for_docker
  docker stop "$qdrant_container" >/dev/null 2>&1 || true
  docker rm "$qdrant_container" >/dev/null 2>&1 || true
  mkdir -p "$qdrant_storage"
  if [[ "$preserve" != 1 ]]; then
    delete_service_contents "$qdrant_storage"
  fi
  chmod 0777 "$qdrant_storage"
  docker run -d \
    --name "$qdrant_container" \
    --memory "$qdrant_memory_limit" \
    --memory-swap "$qdrant_memory_limit" \
    -e QDRANT__SERVICE__MAX_WORKERS="$qdrant_service_workers" \
    -p "$qdrant_port:6333" \
    -p "$qdrant_grpc_port:6334" \
    -v "$qdrant_storage:/qdrant/storage" \
    qdrant/qdrant:v1.18.2 \
    >>"$log"
  wait_for_qdrant
  tune_existing_qdrant_collections
  wait_for_qdrant_idle
}

stop_qdrant() {
  docker stop "$qdrant_container" >/dev/null 2>&1 || true
  docker rm "$qdrant_container" >/dev/null 2>&1 || true
}

wait_for_doltgres() {
  "$python" - "$dolt_admin_dsn" "$dolt_database" <<'PY'
import sys
import time

import psycopg
from psycopg import sql

dsn, database = sys.argv[1:]
deadline = time.monotonic() + 180
while True:
    try:
        with psycopg.connect(dsn, autocommit=True) as connection:
            databases = {
                str(row[0])
                for row in connection.execute(
                    "SELECT datname FROM pg_database"
                ).fetchall()
            }
            if database not in databases:
                connection.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(database)
                    )
                )
        break
    except Exception:
        if time.monotonic() >= deadline:
            raise
        time.sleep(2)
PY
}

start_doltgres() {
  local preserve=${1:-0}
  wait_for_docker
  docker stop "$dolt_container" >/dev/null 2>&1 || true
  docker rm "$dolt_container" >/dev/null 2>&1 || true
  mkdir -p "$dolt_storage"
  if [[ "$preserve" != 1 ]]; then
    delete_service_contents "$dolt_storage"
  fi
  chmod 0777 "$dolt_storage"
  docker run -d \
    --name "$dolt_container" \
    --memory "$doltgres_memory_limit" \
    --memory-swap "$doltgres_memory_limit" \
    -e DOLTGRES_PASSWORD=password \
    -e GOMEMLIMIT="$doltgres_go_memory_limit" \
    -p "$dolt_port:5432" \
    -v "$dolt_storage:/var/lib/doltgres" \
    dolthub/doltgresql@sha256:4e4872c3c3400c7918c84e57b71c191cfd6b2b46b65dda35e90c5ee64d396838 \
    >>"$log"
  wait_for_doltgres
}

stop_doltgres() {
  docker stop "$dolt_container" >/dev/null 2>&1 || true
  docker rm "$dolt_container" >/dev/null 2>&1 || true
}

backend_args() {
  local backend=$1
  local state=$2
  printf '%s\n' \
    --backend "$backend" \
    --state-dir "$state" \
    --dimensions 384 \
    --placeholder-zero-embeddings \
    --qdrant-url "$qdrant_url" \
    --qdrant-storage-dir "$qdrant_storage"
  if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
    printf '%s\n' \
      --doltgres-dsn "$dolt_dsn" \
      --btrfs-root "$native_workspaces" \
      --doltgres-data-dir "$dolt_storage"
  fi
}

snapshot_selection_args() {
  local max_documents=$1
  local required_ids_file=$2
  [[ -n "$max_documents" ]] || return
  printf '%s\n' --max-documents "$max_documents"
  while IFS= read -r identifier; do
    [[ -n "$identifier" ]] || continue
    printf '%s\n' --required-document-id "$identifier"
  done <"$required_ids_file"
}

run_backend() {
  local label=$1
  local backend=$2
  local state="$state_root/$label/state"
  local output="$output_root/$label"
  local backend_work="$work_root/$label"
  local active_backend_file="$service_root/active-backend"
  local preserve=0
  local -a args document_selection_args code_selection_args
  mapfile -t args < <(backend_args "$backend" "$state")
  mapfile -t document_selection_args < <(
    snapshot_selection_args \
      "$document_max_documents" \
      "$required_document_ids_file"
  )
  mapfile -t code_selection_args < <(
    snapshot_selection_args \
      "$code_max_documents" \
      "$required_code_ids_file"
  )

  if [[ -f "$output/.complete" ]]; then
    log_phase "$label:already-complete"
    return
  fi
  if [[
    "$resume" == 1
    && -d "$state"
    && -f "$active_backend_file"
    && "$(<"$active_backend_file")" == "$label"
  ]]; then
    preserve=1
    log_phase "$label:resume-existing-state"
  else
    delete_tree_contents "$state_root/$label"
    delete_tree_contents "$output"
    delete_service_contents "$qdrant_storage"
    if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
      delete_service_contents "$dolt_storage"
      delete_btrfs_workspace_contents
    fi
  fi
  mkdir -p "$state" "$backend_work" "$output"
  printf '%s\n' "$label" >"$active_backend_file"
  export CHRONOS_QDRANT_HNSW_M="$qdrant_bulk_hnsw_m"
  start_qdrant "$preserve"
  if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
    start_doltgres "$preserve"
  fi

  timed_phase "$label" ingest-company \
    "$cli" "${args[@]}" ingest-snapshot "$document_snapshot" \
      --branch main \
      --batch-size "$batch_size" \
      --resume \
      --progress-every 1000 \
      "${document_selection_args[@]}"

  timed_phase "$label" ingest-code \
    "$cli" "${args[@]}" ingest-snapshot "$code_snapshot" \
      --branch main \
      --batch-size "$batch_size" \
      --resume \
      --progress-every 1000 \
      "${code_selection_args[@]}"

  timed_phase "$label" finalize-vector-store \
    finalize_qdrant_storage

  timed_phase "$label" build-hierarchy \
    "$cli" "${args[@]}" init-hierarchy \
      --snapshot "$document_snapshot"

  timed_phase "$label" storage-after-ingestion \
    "$cli" "${args[@]}" storage-stats
  "$cli" "${args[@]}" storage-stats \
    >"$output/ingestion-storage.json" 2>>"$log"

  local -a runner_args=(
    "$python" "$runner"
    --traces-dir "$traces"
    --snapshot-manifest "$document_snapshot/manifest.json"
    --snapshot-manifest "$code_snapshot/manifest.json"
    --base-state "$backend=$state"
    --in-place-backend "$backend"
    --qdrant-url "$qdrant_url"
    --qdrant-storage-dir "$qdrant_storage"
    --work-root "$backend_work"
    --output-root "$output"
    --repo-dir "$replay_workdir"
    --dimensions 384
    --embedding-model "$embedding_model"
    --repetitions 1
    --max-shell-interrupt-seconds 30
    --start "$workflow_start"
    --end "$workflow_end"
    --dependency-mode independent
    --isolated-workflows
    --summarizer "$summarizer"
  )
  if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
    runner_args+=(
      --doltgres-dsn "$dolt_dsn"
      --btrfs-root "$native_workspaces"
      --doltgres-data-dir "$dolt_storage"
    )
  fi
  timed_phase "$label" workflows "${runner_args[@]}"
  timed_phase "$label" summarize-suite \
    "$python" "$suite_summarizer" "$output" \
      --output-dir "$output/summary"

  printf 'complete\n' >"$output/.complete"
  timed_phase "$label" destroy-external-state \
    "$cli" "${args[@]}" destroy-state
  if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
    stop_doltgres
  fi
  stop_qdrant
  delete_tree_contents "$state_root/$label"
  delete_service_contents "$qdrant_storage"
  if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
    delete_service_contents "$dolt_storage"
    delete_btrfs_workspace_contents
  fi
  rm -f "$active_backend_file"
}

cleanup_services() {
  stop_doltgres
  stop_qdrant
}
trap cleanup_services EXIT

log_phase preflight
printf '%s ingest_config batch_size=%s bm25_threads=%s qdrant_upload_workers=%s qdrant_service_workers=%s qdrant_shards=%s qdrant_point_batch_size=%s qdrant_indexing_threshold_kib=%s qdrant_bulk_hnsw_m=%s qdrant_query_hnsw_m=%s qdrant_text_index=absent qdrant_optimization_threads=%s qdrant_indexing_threads=%s qdrant_transport=grpc qdrant_memory=%s\n' \
  "$(date -u +%FT%TZ)" \
  "$batch_size" \
  "$bm25_threads" \
  "$qdrant_upload_workers" \
  "$qdrant_service_workers" \
  "$qdrant_shards" \
  "$qdrant_point_batch_size" \
  "$qdrant_indexing_threshold_kib" \
  "$qdrant_bulk_hnsw_m" \
  "$qdrant_query_hnsw_m" \
  "$qdrant_max_optimization_threads" \
  "$qdrant_max_indexing_threads" \
  "$qdrant_memory_limit" \
  | tee -a "$log"
"$python" - \
  "$document_snapshot" \
  "$code_snapshot" \
  "$traces" \
  "$required_document_ids_file" \
  "$required_code_ids_file" <<'PY'
import json
import re
import sqlite3
import sys
from pathlib import Path

document_snapshot, code_snapshot, traces, document_output, code_output = (
    Path(value) for value in sys.argv[1:]
)
for manifest_path in (
    document_snapshot / "manifest.json",
    code_snapshot / "manifest.json",
):
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if not manifest.get("complete"):
        raise SystemExit(f"incomplete snapshot: {manifest_path}")
paths = sorted(traces.glob("[0-9][0-9]-*.jsonl"))
if len(paths) != 13:
    raise SystemExit(f"expected 13 real Codex traces, found {len(paths)}")

identifier_pattern = re.compile(
    r"\b(?:dsid|enterprise)_[0-9a-f]{16,64}\b"
)
path_pattern = re.compile(
    r"/(?:code|knowledge/company)/[A-Za-z0-9_./-]+"
)
identifiers: set[str] = set()
referenced_paths: set[str] = set()

def visit(value: object) -> None:
    if isinstance(value, dict):
        for item in value.values():
            visit(item)
    elif isinstance(value, list):
        for item in value:
            visit(item)
    elif isinstance(value, str):
        identifiers.update(identifier_pattern.findall(value))
        referenced_paths.update(path_pattern.findall(value))

for trace_path in paths:
    with trace_path.open(encoding="utf-8") as trace:
        for line in trace:
            visit(json.loads(line))

def required_ids(snapshot: Path, *, path_prefix: str) -> list[str]:
    relative_paths = {
        (
            value.removeprefix("/knowledge/company/")
            if path_prefix == "/knowledge/company/"
            else f"codebases/{value.removeprefix('/code/')}"
        )
        for value in referenced_paths
        if value.startswith(path_prefix)
    }
    database = sqlite3.connect(snapshot / "snapshot.sqlite")
    try:
        selected: set[str] = set()
        for identifier in identifiers:
            row = database.execute(
                "SELECT id FROM documents WHERE id = ?",
                (identifier,),
            ).fetchone()
            if row is not None:
                selected.add(str(row[0]))
        for relative_path in relative_paths:
            row = database.execute(
                "SELECT id FROM documents WHERE relative_path = ?",
                (relative_path,),
            ).fetchone()
            if row is not None:
                selected.add(str(row[0]))
        return sorted(selected)
    finally:
        database.close()

company_ids = required_ids(
    document_snapshot,
    path_prefix="/knowledge/company/",
)
code_ids = required_ids(code_snapshot, path_prefix="/code/")
document_output.write_text(
    "".join(f"{value}\n" for value in company_ids),
    encoding="utf-8",
)
code_output.write_text(
    "".join(f"{value}\n" for value in code_ids),
    encoding="utf-8",
)
print(
    "workflow_selection "
    f"trace_ids={len(identifiers)} "
    f"trace_paths={len(referenced_paths)} "
    f"company_required={len(company_ids)} "
    f"code_required={len(code_ids)}"
)
PY

printf '%s bounded_selection company_max=%s company_required=%s code_max=%s code_required=%s\n' \
  "$(date -u +%FT%TZ)" \
  "${document_max_documents:-all}" \
  "$(wc -l <"$required_document_ids_file")" \
  "${code_max_documents:-all}" \
  "$(wc -l <"$required_code_ids_file")" \
  | tee -a "$log"

# Run one complete ingestion and workflow sequence before starting the next
# backend. The order is part of the experiment protocol. A subset is useful
# for clean validation runs; the default remains the complete comparison.
backend_selected() {
  local requested=$1
  [[ ",$backend_selection," == *",$requested,"* ]]
}

backend_selected chronos &&
  run_backend "01-chronos" "chronos"
backend_selected native-branching &&
  run_backend "02-native-branching" "doltgres-qdrant-btrfs"
backend_selected app-managed &&
  run_backend "03-app-managed" "app-managed"

log_phase merge-backend-results
combined_two="$output_root/.combined-chronos-native"
final="$output_root/combined"
delete_tree_contents "$combined_two"
delete_tree_contents "$final"
if backend_selected chronos &&
  backend_selected native-branching &&
  backend_selected app-managed; then
  "$python" "$merger" \
    --primary-root "$output_root/01-chronos" \
    --additional-root "$output_root/02-native-branching" \
    --traces-dir "$traces" \
    --summarizer "$summarizer" \
    --output-root "$combined_two" \
    >>"$log" 2>&1
  "$python" "$merger" \
    --primary-root "$combined_two" \
    --additional-root "$output_root/03-app-managed" \
    --traces-dir "$traces" \
    --summarizer "$summarizer" \
    --output-root "$final" \
    >>"$log" 2>&1
  "$python" "$suite_summarizer" "$final" \
    --output-dir "$final/summary" \
    >>"$log" 2>&1
  delete_tree_contents "$combined_two"
else
  log_phase merge-backend-results:skipped-for-subset
fi

log_phase complete
printf 'complete\n' >"$output_root/.complete"
trap - EXIT
cleanup_services
