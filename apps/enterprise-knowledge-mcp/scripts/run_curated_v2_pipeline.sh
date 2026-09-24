#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
app="$repo/apps/enterprise-knowledge-mcp"
corpus="${ENTERPRISE_CORPUS:-$repo/.enterprise-knowledge/datasets/enterprise-rag-infra-v1}"
artifact_root="$repo/.enterprise-knowledge/enterprise-rag-curated-v2"
document_snapshot="${ENTERPRISE_DOCUMENT_SNAPSHOT:-$artifact_root/document-snapshot}"
code_snapshot="$artifact_root/code-snapshot"
document_manifest="$document_snapshot/manifest.json"
code_manifest="$code_snapshot/manifest.json"

btrfs=/tmp/chronos-enterprise-full-bench
old_chronos_base="$btrfs/base-chronos-full-zero"
old_app_base="$btrfs/base-app-managed-full-zero"
chronos_base="$btrfs/base-chronos-curated-v2"
app_base="$btrfs/base-app-managed-curated-v2"
capture_state="$btrfs/capture-chronos-curated-v2"
work_root="$btrfs/work-curated-v2"
capture_workdir=/tmp/chronos-enterprise-curated-v2-capture-workdir
replay_workdir=/tmp/chronos-enterprise-curated-v2-replay-workdir

suite="$app/workflows/curated-v2"
runs="$suite/runs/20260727-full-zero-v2"
traces="$suite/traces/full-zero-v2"
benchmark="${ENTERPRISE_BENCHMARK_OUTPUT:-$artifact_root/benchmarks/20260727-main-suite-r3}"
codex_home="$artifact_root/codex-home"
cli="$repo/.venv/bin/chronos-enterprise-knowledge"
python="$repo/.venv/bin/python"
codex_bin=/home/ubuntu/.local/bin/codex
pipeline_log="$artifact_root/pipeline.log"
benchmark_repetitions=${BENCHMARK_REPETITIONS:-3}

mkdir -p \
  "$artifact_root" \
  "$capture_workdir" \
  "$replay_workdir" \
  "$runs" \
  "$traces" \
  "$work_root" \
  "$codex_home"
cd "$repo"

log_phase() {
  printf '%s phase=%s\n' "$(date -u +%FT%TZ)" "$1" | tee -a "$pipeline_log"
}

minilm_paused=false

resume_minilm() {
  if [[ "$minilm_paused" == true ]]; then
    systemctl --user kill \
      --kill-whom=main \
      --signal=SIGCONT \
      chronos-enterprise-minilm-backfill.service \
      >/dev/null 2>&1 || true
    minilm_paused=false
  fi
}

pause_minilm() {
  if systemctl --user is-active \
      chronos-enterprise-minilm-backfill.service \
      >/dev/null 2>&1; then
    systemctl --user kill \
      --kill-whom=main \
      --signal=SIGSTOP \
      chronos-enterprise-minilm-backfill.service
    minilm_paused=true
    trap resume_minilm EXIT
  fi
}

manifest_complete() {
  "$python" - "$1" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.is_file():
    raise SystemExit(1)
manifest = json.loads(path.read_text(encoding="utf-8"))
raise SystemExit(0 if manifest.get("complete") else 1)
PY
}

create_reflink_base() {
  local source=$1
  local destination=$2
  local copied_marker="$destination/.reflink-base-copied"
  if [[ ! -d "$destination" ]]; then
    sudo btrfs subvolume create "$destination" >>"$pipeline_log"
  fi
  sudo chown "$USER:$(id -gn)" "$destination"
  if [[ ! -e "$copied_marker" ]]; then
    cp -a --reflink=always "$source/." "$destination/"
    touch "$copied_marker"
  fi
}

reset_and_layer_code() {
  local backend=$1
  local state=$2
  local marker="$state/.curated-v2-ready"
  if [[ -e "$marker" ]]; then
    return
  fi
  "$python" \
    "$app/scripts/reset_curated_hierarchy.py" \
    --backend "$backend" \
    --state-dir "$state" \
    --dimensions 384 \
    >>"$pipeline_log" 2>&1
  CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB=262144 \
  "$cli" \
    --backend "$backend" \
    --dimensions 384 \
    --state-dir "$state" \
    --placeholder-zero-embeddings \
    ingest-snapshot "$code_snapshot" \
    --branch main \
    --batch-size 128 \
    --resume \
    --progress-every 1000 \
    >>"$pipeline_log" 2>&1
  "$cli" \
    --backend "$backend" \
    --dimensions 384 \
    --state-dir "$state" \
    --placeholder-zero-embeddings \
    init-hierarchy \
    --snapshot "$document_snapshot" \
    >>"$pipeline_log" 2>&1
  "$python" - "$document_manifest" "$code_manifest" "$marker" <<'PY'
import json
import sys
from pathlib import Path

documents = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
code = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
Path(sys.argv[3]).write_text(
    json.dumps(
        {
            "documents": documents["documents"] + code["documents"],
            "chunks": documents["chunks"] + code["chunks"],
            "document_selection_digest": documents["selection_digest"],
            "code_selection_digest": code["selection_digest"],
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
PY
}

log_phase prepare-document-snapshot
if ! manifest_complete "$document_manifest"; then
  nice -n 10 ionice -c 3 \
    "$cli" \
    --dimensions 384 \
    --placeholder-zero-embeddings \
    prepare-snapshot "$corpus" "$document_snapshot" \
    --sample-fraction 1 \
    --sample-seed chronos-enterprise-infra-v1 \
    --batch-size 256 \
    --chunk-workers 2 \
    --progress-every 1000 \
    >>"$pipeline_log" 2>&1
fi

log_phase prepare-code-snapshot
if ! manifest_complete "$code_manifest"; then
  nice -n 10 ionice -c 3 \
    "$cli" \
    --dimensions 384 \
    --placeholder-zero-embeddings \
    prepare-snapshot "$corpus" "$code_snapshot" \
    --sample-fraction 1 \
    --sample-seed chronos-enterprise-code-v2 \
    --connector codebase \
    --batch-size 128 \
    --chunk-workers 2 \
    --progress-every 1000 \
    >>"$pipeline_log" 2>&1
fi

log_phase build-chronos-main-suite-base
create_reflink_base "$old_chronos_base" "$chronos_base"
reset_and_layer_code chronos "$chronos_base"

log_phase create-independent-capture-state
create_reflink_base "$chronos_base" "$capture_state"

log_phase configure-isolated-codex-home
if [[ ! -e "$codex_home/auth.json" ]]; then
  cp "$HOME/.codex/auth.json" "$codex_home/auth.json"
  chmod 600 "$codex_home/auth.json"
fi
export CODEX_HOME="$codex_home"
"$codex_bin" mcp remove chronos_enterprise_knowledge >/dev/null 2>&1 || true
"$codex_bin" mcp add chronos_enterprise_knowledge -- \
  "$cli" \
  --dimensions 384 \
  --state-dir "$capture_state" \
  --placeholder-zero-embeddings \
  serve \
  >>"$pipeline_log" 2>&1

if [[ ! -e "$runs/capture-manifest.json" ]]; then
  log_phase capture-thirteen-real-codex-workflows
  "$python" \
    "$app/scripts/capture_codex_workflows.py" \
    --prompts-dir "$suite/prompts" \
    --runs-dir "$runs" \
    --traces-dir "$traces" \
    --workdir "$capture_workdir" \
    --session-root "$codex_home/sessions" \
    --codex "$codex_bin" \
    --state-dir "$capture_state" \
    --dimensions 384 \
    --snapshot-manifest "$document_manifest" \
    --snapshot-manifest "$code_manifest" \
    --start 1 \
    --end 13 \
    --resume \
    >>"$pipeline_log" 2>&1
else
  log_phase reuse-complete-main-suite-capture
fi

log_phase build-app-managed-full-root
if [[ ! -e "$old_app_base/.full-root-ready" ]]; then
  mkdir -p "$old_app_base"
  "$cli" \
    --backend app-managed \
    --dimensions 384 \
    --state-dir "$old_app_base" \
    --placeholder-zero-embeddings \
    ingest-snapshot "$document_snapshot" \
    --branch main \
    --batch-size 256 \
    --resume \
    --progress-every 5000 \
    >>"$pipeline_log" 2>&1
  "$cli" \
    --backend app-managed \
    --dimensions 384 \
    --state-dir "$old_app_base" \
    --placeholder-zero-embeddings \
    init-hierarchy \
    --snapshot "$document_snapshot" \
    >>"$pipeline_log" 2>&1
  {
    printf 'documents=511975\n'
    printf 'chunks=6824073\n'
    printf 'selection_digest=db2ff56e2ff975612770eb06b87881a5c230117f4b92e37b710cef91aac3645f\n'
  } >"$old_app_base/.full-root-ready"
fi

log_phase build-app-managed-main-suite-base
create_reflink_base "$old_app_base" "$app_base"
reset_and_layer_code app-managed "$app_base"

log_phase benchmark-recorded-main-suite
log_phase pause-minilm-for-timed-workflows
pause_minilm
"$python" \
  "$app/scripts/run_full_workflow_benchmarks.py" \
  --traces-dir "$traces" \
  --snapshot-manifest "$document_manifest" \
  --snapshot-manifest "$code_manifest" \
  --base-state "chronos=$chronos_base" \
  --base-state "app-managed=$app_base" \
  --work-root "$work_root" \
  --output-root "$benchmark" \
  --repo-dir "$replay_workdir" \
  --dimensions 384 \
  --embedding-model \
  "sentence-transformers/all-MiniLM-L6-v2#onnx:onnx/model_qint8_avx512.onnx" \
  --repetitions "$benchmark_repetitions" \
  --max-shell-interrupt-seconds 30 \
  --start 1 \
  --end 13 \
  --dependency-mode independent \
  --shared-root-sequence \
  --summarizer "$app/workflows/real/summarize_results.py" \
  >>"$pipeline_log" 2>&1

log_phase resume-minilm-after-timed-workflows
resume_minilm
trap - EXIT
log_phase complete
