#!/usr/bin/env bash
set -euo pipefail

repo=/home/ubuntu/TAR-OS/chronos
app="$repo/apps/enterprise-knowledge-mcp"
artifact_root="$repo/.enterprise-knowledge/enterprise-rag-curated-v2"
document_snapshot="$repo/.enterprise-knowledge/enterprise-rag-full-staged-v1/snapshot"
code_snapshot="$artifact_root/code-snapshot"
native_root=/tmp/chronos-enterprise-full-bench/native-curated-v2
state="$native_root/state"
services="$native_root/services"
workspaces="$native_root/workspaces"
work_root="$native_root/work-all"
output="$artifact_root/benchmarks/20260728-native-priority-all-r1"
traces="$app/workflows/curated-v2/traces/full-zero-v2"
replay_workdir=/tmp/chronos-enterprise-curated-v2-replay-workdir
log="$artifact_root/native-priority-pipeline.log"

dolt_container=chronos-enterprise-native-doltgres
qdrant_container=chronos-enterprise-native-qdrant
dolt_port=55439
qdrant_port=6339
qdrant_memory_limit=${ENTERPRISE_QDRANT_MEMORY_LIMIT:-8g}
qdrant_max_optimization_threads=${ENTERPRISE_QDRANT_MAX_OPTIMIZATION_THREADS:-2}
qdrant_max_indexing_threads=${ENTERPRISE_QDRANT_MAX_INDEXING_THREADS:-4}
qdrant_upload_workers=${ENTERPRISE_QDRANT_UPLOAD_WORKERS:-4}
qdrant_service_workers=${ENTERPRISE_QDRANT_SERVICE_WORKERS:-8}
bm25_threads=${ENTERPRISE_BM25_THREADS:-4}
database=chronos_enterprise_curated_v2
dolt_admin_dsn="postgresql://postgres:password@127.0.0.1:$dolt_port/postgres"
dolt_dsn="postgresql://postgres:password@127.0.0.1:$dolt_port/$database"
qdrant_url="http://127.0.0.1:$qdrant_port"
cli="$repo/.venv/bin/chronos-enterprise-knowledge"
python="$repo/.venv/bin/python"

export CHRONOS_QDRANT_MAX_OPTIMIZATION_THREADS="$qdrant_max_optimization_threads"
export CHRONOS_QDRANT_MAX_INDEXING_THREADS="$qdrant_max_indexing_threads"
export CHRONOS_QDRANT_UPLOAD_WORKERS="$qdrant_upload_workers"
export CHRONOS_BM25_THREADS="$bm25_threads"

mkdir -p \
  "$artifact_root" \
  "$state" \
  "$services/doltgres" \
  "$services/qdrant" \
  "$workspaces" \
  "$work_root" \
  "$output" \
  "$replay_workdir"
cd "$repo"

log_phase() {
  printf '%s phase=%s\n' "$(date -u +%FT%TZ)" "$1" | tee -a "$log"
}

ensure_container() {
  local name=$1
  shift
  if ! docker inspect "$name" >/dev/null 2>&1; then
    docker run -d --name "$name" "$@" >>"$log"
  elif [[ "$(docker inspect -f '{{.State.Running}}' "$name")" != true ]]; then
    docker start "$name" >>"$log"
  fi
}

log_phase start-native-services
ensure_container \
  "$dolt_container" \
  -e DOLTGRES_PASSWORD=password \
  -p "$dolt_port:5432" \
  -v "$services/doltgres:/var/lib/doltgres" \
  dolthub/doltgresql@sha256:4e4872c3c3400c7918c84e57b71c191cfd6b2b46b65dda35e90c5ee64d396838
ensure_container \
  "$qdrant_container" \
  --memory "$qdrant_memory_limit" \
  --memory-swap "$qdrant_memory_limit" \
  -e QDRANT__SERVICE__MAX_WORKERS="$qdrant_service_workers" \
  -p "$qdrant_port:6333" \
  -v "$services/qdrant:/qdrant/storage" \
  qdrant/qdrant:v1.18.2
docker update \
  --memory "$qdrant_memory_limit" \
  --memory-swap "$qdrant_memory_limit" \
  "$qdrant_container" >/dev/null

"$python" - "$dolt_admin_dsn" "$database" "$qdrant_url" <<'PY'
import sys
import time

import psycopg
from psycopg import sql
from qdrant_client import QdrantClient

admin_dsn, database, qdrant_url = sys.argv[1:]
deadline = time.monotonic() + 120
while True:
    try:
        with psycopg.connect(admin_dsn, autocommit=True) as connection:
            existing = {
                str(row[0])
                for row in connection.execute(
                    "SELECT datname FROM pg_database"
                ).fetchall()
            }
            if database not in existing:
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

client = QdrantClient(url=qdrant_url)
while True:
    try:
        client.get_collections()
        break
    except Exception:
        if time.monotonic() >= deadline:
            raise
        time.sleep(2)
client.close()
PY

backend_args=(
  --backend doltgres-qdrant-btrfs
  --state-dir "$state"
  --dimensions 384
  --placeholder-zero-embeddings
  --qdrant-url "$qdrant_url"
  --doltgres-dsn "$dolt_dsn"
  --btrfs-root "$workspaces"
  --doltgres-data-dir "$services/doltgres"
  --qdrant-storage-dir "$services/qdrant"
)

log_phase ingest-company-document-snapshot
if [[ ! -e "$state/.documents-ingested" ]]; then
  "$cli" \
    "${backend_args[@]}" \
    ingest-snapshot "$document_snapshot" \
    --branch main \
    --batch-size 512 \
    --resume \
    --progress-every 5000 \
    >>"$log" 2>&1
  touch "$state/.documents-ingested"
fi

log_phase ingest-source-code-snapshot
if [[ ! -e "$state/.code-ingested" ]]; then
  "$cli" \
    "${backend_args[@]}" \
    ingest-snapshot "$code_snapshot" \
    --branch main \
    --batch-size 512 \
    --resume \
    --progress-every 1000 \
    >>"$log" 2>&1
  touch "$state/.code-ingested"
fi

log_phase build-native-hierarchy
if [[ ! -e "$state/.hierarchy-ready" ]]; then
  "$cli" \
    "${backend_args[@]}" \
    init-hierarchy \
    --snapshot "$document_snapshot" \
    >>"$log" 2>&1
  touch "$state/.hierarchy-ready"
fi

log_phase replay-all-workflows
"$python" \
  "$app/scripts/run_full_workflow_benchmarks.py" \
  --traces-dir "$traces" \
  --snapshot-manifest "$document_snapshot/manifest.json" \
  --snapshot-manifest "$code_snapshot/manifest.json" \
  --base-state "doltgres-qdrant-btrfs=$state" \
  --in-place-backend doltgres-qdrant-btrfs \
  --qdrant-url "$qdrant_url" \
  --doltgres-dsn "$dolt_dsn" \
  --btrfs-root "$workspaces" \
  --doltgres-data-dir "$services/doltgres" \
  --qdrant-storage-dir "$services/qdrant" \
  --work-root "$work_root" \
  --output-root "$output" \
  --repo-dir "$replay_workdir" \
  --dimensions 384 \
  --embedding-model \
  "sentence-transformers/all-MiniLM-L6-v2#onnx:onnx/model_qint8_avx512.onnx" \
  --repetitions 1 \
  --max-shell-interrupt-seconds 30 \
  --start 1 \
  --end 13 \
  --dependency-mode independent \
  --shared-root-sequence \
  --summarizer "$app/workflows/real/summarize_results.py" \
  >>"$log" 2>&1

"$python" \
  "$app/scripts/summarize_workflow_suite.py" \
  "$output" \
  --output-dir "$output/summary" \
  >>"$log" 2>&1

log_phase complete
