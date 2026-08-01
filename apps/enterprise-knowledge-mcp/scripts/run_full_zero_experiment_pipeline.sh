#!/usr/bin/env bash
set -euo pipefail

repo=/home/ubuntu/TAR-OS/chronos
app="$repo/apps/enterprise-knowledge-mcp"
snapshot="$repo/.enterprise-knowledge/enterprise-rag-full-staged-v1/snapshot"
manifest="$snapshot/manifest.json"
chronos_state="$repo/.enterprise-knowledge/enterprise-rag-full-zero-v1/state"
btrfs=/tmp/chronos-enterprise-full-bench
chronos_base="$btrfs/base-chronos-full-zero"
app_base="$btrfs/base-app-managed-full-zero"
work_root="$btrfs/work"
capture_workdir=/tmp/chronos-enterprise-full-capture-workdir
replay_workdir=/tmp/chronos-enterprise-full-replay-workdir
runs="$app/workflows/real/runs/20260727-full-zero-v1"
traces="$app/workflows/real/traces/full-zero-v1"
benchmark="$repo/.enterprise-knowledge/enterprise-rag-full-zero-v1/benchmarks/20260727-full-corpus-per-workflow-r3"
cli="$repo/.venv/bin/chronos-enterprise-knowledge"
pipeline_log="$repo/.enterprise-knowledge/enterprise-rag-full-zero-v1/full-pipeline.log"

mkdir -p "$capture_workdir" "$replay_workdir" "$work_root" "$(dirname "$pipeline_log")"
cd "$repo"

log_phase() {
  printf '%s phase=%s\n' "$(date -u +%FT%TZ)" "$1" | tee -a "$pipeline_log"
}

root_counts() {
  "$repo/.venv/bin/python" - "$chronos_state/knowledge.sqlite" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True, timeout=30)
documents = connection.execute(
    "SELECT COUNT(*) FROM _chronos_b_interval_knowledge_documents"
).fetchone()[0]
chunks = connection.execute(
    "SELECT COUNT(*) FROM _chronos_b_interval_knowledge_chunks"
).fetchone()[0]
connection.close()
print(f"{documents} {chunks}")
PY
}

read_root_counts() {
  local counts
  until counts="$(root_counts)"; do
    printf '%s root_count_retry=true\n' \
      "$(date -u +%FT%TZ)" | tee -a "$pipeline_log"
    sleep 10
  done
  read -r documents chunks <<<"$counts"
}

root_is_complete() {
  local documents chunks
  read_root_counts
  [[ "$documents" -eq 511975 && "$chunks" -eq 6824073 ]]
}

log_phase wait-for-current-ingestion
while pgrep -f \
  "^/home/ubuntu/TAR-OS/chronos/.venv/bin/python.*enterprise-rag-full-zero-v1/state.*ingest-snapshot" \
  >/dev/null; do
  documents=0
  chunks=0
  read_root_counts
  printf '%s root_documents=%s root_chunks=%s\n' \
    "$(date -u +%FT%TZ)" "$documents" "$chunks" | tee -a "$pipeline_log"
  sleep 60
done

if ! root_is_complete; then
  log_phase resume-full-root-ingestion
  "$cli" \
    --dimensions 384 \
    --state-dir "$chronos_state" \
    --placeholder-zero-embeddings \
    ingest-snapshot "$snapshot" \
    --branch main \
    --batch-size 256 \
    --resume \
    --progress-every 5000 \
    >>"$pipeline_log" 2>&1
fi
if ! root_is_complete; then
  printf 'full root validation failed\n' >&2
  exit 1
fi

log_phase initialize-full-root-hierarchy
"$cli" \
  --dimensions 384 \
  --state-dir "$chronos_state" \
  --placeholder-zero-embeddings \
  init-hierarchy \
  >>"$pipeline_log" 2>&1

if [[ ! -e "$chronos_base/.full-root-ready" ]]; then
  log_phase preserve-pristine-chronos-base
  temporary="$btrfs/.base-chronos-full-zero.$$.tmp"
  mkdir "$temporary"
  cp -a "$chronos_state/." "$temporary"
  {
    printf 'documents=511975\n'
    printf 'chunks=6824073\n'
    printf 'selection_digest=db2ff56e2ff975612770eb06b87881a5c230117f4b92e37b710cef91aac3645f\n'
  } >"$temporary/.full-root-ready"
  mv "$temporary" "$chronos_base"
fi

if ! pgrep -f \
  "backfill-snapshot-embeddings $snapshot" \
  >/dev/null; then
  log_phase start-minilm-backfill
  nohup nice -n 10 ionice -c 3 \
    "$cli" \
    --embedding-backend onnx \
    --embedding-model-file onnx/model_qint8_avx512.onnx \
    --embedding-workers 1 \
    --embedding-batch-size 128 \
    backfill-snapshot-embeddings "$snapshot" \
    --batch-size 2048 \
    --progress-every 20000 \
    --follow-preparation \
    --poll-seconds 5 \
    >>"$repo/.enterprise-knowledge/enterprise-rag-full-staged-v1/backfill-resume.log" \
    2>&1 &
fi

log_phase configure-codex-full-root-mcp
codex mcp remove chronos_enterprise_knowledge >/dev/null 2>&1 || true
codex mcp add chronos_enterprise_knowledge -- \
  "$cli" \
  --dimensions 384 \
  --state-dir "$chronos_state" \
  --placeholder-zero-embeddings \
  serve \
  >>"$pipeline_log" 2>&1

if [[ ! -e "$runs/capture-manifest.json" ]]; then
  log_phase capture-ten-real-codex-workflows
  "$repo/.venv/bin/python" \
    "$app/scripts/capture_codex_workflows.py" \
    --prompts-dir "$app/workflows/real/prompts" \
    --runs-dir "$runs" \
    --traces-dir "$traces" \
    --workdir "$capture_workdir" \
    --state-dir "$chronos_state" \
    --dimensions 384 \
    --snapshot-manifest "$manifest" \
    --start 1 \
    --end 10 \
    >>"$pipeline_log" 2>&1
else
  log_phase reuse-complete-codex-capture
fi

if [[ ! -e "$app_base/.full-root-ready" ]]; then
  log_phase ingest-full-app-managed-base
  mkdir -p "$app_base"
  attempts=0
  until "$cli" \
      --backend app-managed \
      --dimensions 384 \
      --state-dir "$app_base" \
      --placeholder-zero-embeddings \
      ingest-snapshot "$snapshot" \
      --branch main \
      --batch-size 256 \
      --resume \
      --progress-every 5000 \
      >>"$pipeline_log" 2>&1; do
    attempts=$((attempts + 1))
    if [[ "$attempts" -ge 10 ]]; then
      printf 'app-managed full ingestion failed after %s attempts\n' \
        "$attempts" >&2
      exit 1
    fi
    printf '%s app_ingest_retry=%s\n' \
      "$(date -u +%FT%TZ)" "$attempts" | tee -a "$pipeline_log"
    sleep 60
  done
  "$cli" \
    --backend app-managed \
    --dimensions 384 \
    --state-dir "$app_base" \
    --placeholder-zero-embeddings \
    init-hierarchy \
    >>"$pipeline_log" 2>&1
  {
    printf 'documents=511975\n'
    printf 'chunks=6824073\n'
    printf 'selection_digest=db2ff56e2ff975612770eb06b87881a5c230117f4b92e37b710cef91aac3645f\n'
  } >"$app_base/.full-root-ready"
fi

log_phase benchmark-each-workflow-independently
"$repo/.venv/bin/python" \
  "$app/scripts/run_full_workflow_benchmarks.py" \
  --traces-dir "$traces" \
  --snapshot-manifest "$manifest" \
  --base-state "chronos=$chronos_base" \
  --base-state "app-managed=$app_base" \
  --work-root "$work_root" \
  --output-root "$benchmark" \
  --repo-dir "$replay_workdir" \
  --dimensions 384 \
  --embedding-model \
  "sentence-transformers/all-MiniLM-L6-v2#onnx:onnx/model_qint8_avx512.onnx" \
  --repetitions 3 \
  --start 1 \
  --end 10 \
  --shared-root-sequence \
  --summarizer "$app/workflows/real/summarize_results.py" \
  >>"$pipeline_log" 2>&1

log_phase complete
