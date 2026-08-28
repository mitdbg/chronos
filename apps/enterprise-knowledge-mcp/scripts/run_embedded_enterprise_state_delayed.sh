#!/usr/bin/env bash
set -Eeuo pipefail

# Deliberately wait before touching benchmark-owned state.  This wrapper is
# used for the post-backfill full-corpus replay and keeps its output separate
# from every previously published benchmark directory.
repo=/home/ubuntu/TAR-OS/chronos
app="$repo/apps/enterprise-knowledge-mcp"
artifact_root="$repo/.enterprise-knowledge/enterprise-rag-curated-v2"
output_root="$artifact_root/benchmarks/all-16-workflows-100pct-llm-latency-v4-embedded-20260811"
log="$output_root/delayed-run.log"
cli="$repo/.venv/bin/chronos-enterprise-knowledge"
python="$repo/.venv/bin/python"

mkdir -p "$output_root"
exec > >(tee -a "$log") 2>&1
printf '%s scheduled; sleeping exactly 3600 seconds before ingestion\n' \
  "$(date -u +%FT%TZ)"
if [[ "${ENTERPRISE_SKIP_DELAY:-0}" == 1 ]]; then
  printf '%s recovery mode; one-hour delay already satisfied\n' \
    "$(date -u +%FT%TZ)"
else
  sleep 3600
fi
printf '%s delay complete; checking snapshot embeddings\n' \
  "$(date -u +%FT%TZ)"

interval_experiment_running() {
  ps -eo pid=,args= | awk '
    /awk/ { next }
    $0 ~ /Chronos-VLDB27\/exps\/interval_data_type\/run\.py/ &&
    $0 ~ /--storage-only/ { found = 1 }
    $0 ~ /storage-density-isolated-v3-full/ &&
    $0 ~ /oltp_overhead_cpp/ { found = 1 }
    END { exit(found ? 0 : 1) }
  '
}

while interval_experiment_running; do
  printf '%s waiting for interval_data_type experiment to finish\n' \
    "$(date -u +%FT%TZ)"
  sleep 60
done
printf '%s interval_data_type experiment is no longer running\n' \
  "$(date -u +%FT%TZ)"

backfill_if_needed() {
  local snapshot=$1
  local manifest="$snapshot/manifest.json"
  local -a spec
  mapfile -t spec < <(
    "$python" - "$manifest" <<'PY'
import json
import sys

manifest = json.loads(open(sys.argv[1], encoding="utf-8").read())
spec = manifest["spec"]
model = str(spec["embedding_model"])
base, separator, variant = model.partition("#")
backend = "torch"
model_file = ""
if separator:
    backend, _, model_file = variant.partition(":")
print(base)
print(int(spec["dimensions"]))
print(backend)
print(model_file)
print(int(manifest.get("embeddings_complete", False)))
PY
  )
  if [[ "${spec[4]}" == 1 ]]; then
    printf '%s embeddings already complete snapshot=%s\n' \
      "$(date -u +%FT%TZ)" "$snapshot"
    return
  fi
  printf '%s backfill start snapshot=%s model=%s backend=%s\n' \
    "$(date -u +%FT%TZ)" "$snapshot" "${spec[0]}" "${spec[2]}"
  local -a args=(
    --embedding-model "${spec[0]}"
    --dimensions "${spec[1]}"
    --embedding-backend "${spec[2]}"
    --embedding-workers 2
    --embedding-batch-size 128
  )
  if [[ -n "${spec[3]}" ]]; then
    args+=(--embedding-model-file "${spec[3]}")
  fi
  args+=(
    backfill-snapshot-embeddings "$snapshot"
    --batch-size 2048
    --progress-every 20000
  )
  "$cli" "${args[@]}"
  "$python" - "$manifest" <<'PY'
import json
import sys

manifest = json.loads(open(sys.argv[1], encoding="utf-8").read())
if not manifest.get("complete") or not manifest.get("embeddings_complete"):
    raise SystemExit(f"backfill did not complete: {sys.argv[1]}")
print(json.dumps({
    "snapshot": sys.argv[1],
    "documents": manifest.get("documents"),
    "chunks": manifest.get("chunks"),
    "embeddings_complete": manifest.get("embeddings_complete"),
}))
PY
}

cd "$repo"
backfill_if_needed \
  "$repo/.enterprise-knowledge/enterprise-rag-full-staged-v1/snapshot"
backfill_if_needed \
  "$artifact_root/code-snapshot"

printf '%s starting full embedded enterprise-state pipeline\n' \
  "$(date -u +%FT%TZ)"
export ENTERPRISE_RUN_ID=all-16-workflows-100pct-llm-latency-v4-embedded-20260811
export ENTERPRISE_TRACES_DIR="$app/workflows/curated-v2/traces/llm-latency-v1"
export ENTERPRISE_BACKENDS=chronos,native-branching
export ENTERPRISE_WORKFLOW_START=1
export ENTERPRISE_WORKFLOW_END=16
export ENTERPRISE_EXPECTED_TRACE_COUNT=16
export ENTERPRISE_INCLUDE_CODE_SNAPSHOT=1
export ENTERPRISE_USE_BACKFILLED_EMBEDDINGS=1
export ENTERPRISE_QDRANT_CONTAINER=chronos-enterprise-embedded-20260811-qdrant
export ENTERPRISE_QDRANT_PORT=6350
export ENTERPRISE_QDRANT_GRPC_PORT=6351
export ENTERPRISE_QDRANT_STORAGE="$repo/.enterprise-knowledge/runtime/embedded-20260811-qdrant"
export ENTERPRISE_DOLTGRES_CONTAINER=chronos-enterprise-embedded-20260811-doltgres
export ENTERPRISE_DOLTGRES_PORT=55450
export ENTERPRISE_DOLTGRES_DATABASE=chronos_enterprise_embedded_20260811
export ENTERPRISE_HOST_RUNTIME_ROOT=/tmp/chronos-enterprise-embedded-20260811-host
export ENTERPRISE_RUNTIME_ROOT=/mnt/chronos-enterprise-embedded-20260811
export ENTERPRISE_RUNTIME_IMAGE=/tmp/chronos-enterprise-embedded-20260811.btrfs.img
export ENTERPRISE_RUNTIME_IMAGE_SIZE=80G
export ENTERPRISE_NATIVE_WORKSPACES=/mnt/chronos-enterprise-embedded-20260811/native-workspaces
export ENTERPRISE_QDRANT_MEMORY_LIMIT=8g
export ENTERPRISE_QDRANT_SERVICE_WORKERS=8
export ENTERPRISE_QDRANT_MAX_OPTIMIZATION_THREADS=4
export ENTERPRISE_QDRANT_MAX_INDEXING_THREADS=4
export ENTERPRISE_QDRANT_UPLOAD_WORKERS=4
export ENTERPRISE_QDRANT_SHARDS=4
export ENTERPRISE_QDRANT_POINT_BATCH_SIZE=256
export ENTERPRISE_QDRANT_BULK_HNSW_M=0
export ENTERPRISE_QDRANT_QUERY_HNSW_M=16
export ENTERPRISE_QDRANT_INDEXING_THRESHOLD_KIB=10000
export ENTERPRISE_BM25_THREADS=4

exec "$app/scripts/run_state_division_v2_pipeline.sh"
