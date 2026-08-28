#!/usr/bin/env bash
set -Eeuo pipefail

repo=/home/ubuntu/TAR-OS/chronos
app="$repo/apps/enterprise-knowledge-mcp"
suite="$app/workflows/curated-v2"
corpus="${ENTERPRISE_CORPUS:-/home/ubuntu/TAR-OS/EnterpriseRAG-Bench/generated_data_infra_v1}"
capture_tag="${ENTERPRISE_CAPTURE_TAG:-llm-v1}"
run_id="${ENTERPRISE_CAPTURE_RUN_ID:-20260804-llm-latency-v1}"
trace_id="${ENTERPRISE_CAPTURE_TRACE_ID:-llm-latency-v1}"
prompt_dir="${ENTERPRISE_PROMPTS_DIR:-$suite/prompts}"
artifact="$repo/.enterprise-knowledge/enterprise-rag-curated-v2/codex-trace-capture-$capture_tag"
state="${ENTERPRISE_CAPTURE_STATE_DIR:-$artifact/state}"
qdrant_storage="${ENTERPRISE_CAPTURE_QDRANT_DIR:-$artifact/qdrant}"
qdrant_container="chronos-codex-trace-$capture_tag-qdrant"
qdrant_url=http://127.0.0.1:6386
codex_home="$artifact/codex-home"
workdir="$artifact/workdir"
runs="$suite/runs/$run_id"
traces="$suite/traces/$trace_id"
company_snapshot="${ENTERPRISE_DOCUMENT_SNAPSHOT:-$repo/.enterprise-knowledge/enterprise-rag-curated-v2/document-snapshot}"
code_snapshot="$repo/.enterprise-knowledge/enterprise-rag-curated-v2/code-snapshot"
required_company="$artifact/required-company-document-ids.txt"
required_code="$artifact/required-code-document-ids.txt"
log="$artifact/capture.log"
python="$repo/.venv/bin/python"
cli="$repo/.venv/bin/chronos-enterprise-knowledge"
codex=/home/ubuntu/.local/bin/codex

mkdir -p "$artifact" "$workdir" "$codex_home" "$runs" "$traces"
cd "$repo"

if [[ ! -f "$company_snapshot/manifest.json" ]]; then
  "$cli" \
    --dimensions 384 \
    --placeholder-zero-embeddings \
    prepare-snapshot "$corpus" "$company_snapshot" \
    --sample-fraction 1 \
    --sample-seed chronos-enterprise-infra-v1 \
    --batch-size 256 \
    --chunk-workers 2 \
    --progress-every 1000
fi

if [[ -e "$runs/capture-manifest.json" ]]; then
  echo "capture already complete: $runs/capture-manifest.json"
  exit 0
fi

# ``--resume`` below validates and reuses completed traces, while moving any
# incomplete per-workflow artifacts aside.  The output directory may therefore
# already contain traces when a long capture is restarted after one Codex turn
# failed.

"$python" - \
  "$company_snapshot" \
  "$code_snapshot" \
  "$suite/traces/full-zero-v2" \
  "$suite/traces/atomic-updates-v1" \
  "$required_company" \
  "$required_code" <<'PY'
import json
import re
import sqlite3
import sys
from pathlib import Path

company, code, main_traces, atomic_traces, company_out, code_out = (
    Path(value) for value in sys.argv[1:]
)
identifier_pattern = re.compile(r"\b(?:dsid|enterprise)_[0-9a-f]{16,64}\b")
path_pattern = re.compile(r"/(?:code|knowledge/company)/[A-Za-z0-9_./-]+")
identifiers: set[str] = set()
paths: set[str] = set()

def visit(value: object) -> None:
    if isinstance(value, dict):
        for child in value.values():
            visit(child)
    elif isinstance(value, list):
        for child in value:
            visit(child)
    elif isinstance(value, str):
        identifiers.update(identifier_pattern.findall(value))
        paths.update(path_pattern.findall(value))

for trace_dir in (main_traces, atomic_traces):
    for trace_path in sorted(trace_dir.glob("[0-9][0-9]-*.jsonl")):
        for line in trace_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                visit(json.loads(line))

def resolve(snapshot: Path, prefix: str) -> list[str]:
    relative_paths = {
        value.removeprefix("/knowledge/company/")
        if prefix == "/knowledge/company/"
        else f"codebases/{value.removeprefix('/code/')}"
        for value in paths
        if value.startswith(prefix)
    }
    selected: set[str] = set()
    with sqlite3.connect(snapshot / "snapshot.sqlite") as database:
        for identifier in identifiers:
            row = database.execute(
                "SELECT id FROM documents WHERE id = ?", (identifier,)
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

company_ids = resolve(company, "/knowledge/company/")
code_ids = resolve(code, "/code/")
company_out.write_text("".join(f"{value}\n" for value in company_ids))
code_out.write_text("".join(f"{value}\n" for value in code_ids))
print(
    f"capture requirements company={len(company_ids)} code={len(code_ids)}",
    flush=True,
)
PY

mapfile -t company_ids <"$required_company"
company_args=(--max-documents 25600)
for identifier in "${company_ids[@]}"; do
  company_args+=(--required-document-id "$identifier")
done

export CHRONOS_QDRANT_HNSW_M=0
export CHRONOS_QDRANT_INDEXING_THRESHOLD_KB=10000
export CHRONOS_QDRANT_MAX_OPTIMIZATION_THREADS=4
export CHRONOS_QDRANT_MAX_INDEXING_THREADS=4
export CHRONOS_QDRANT_UPLOAD_WORKERS=4
export CHRONOS_QDRANT_SHARD_NUMBER=4
export CHRONOS_QDRANT_POINT_BATCH_SIZE=256
export CHRONOS_QDRANT_GRPC_PORT=6387
export CHRONOS_QDRANT_PREFER_GRPC=1
export CHRONOS_BM25_THREADS=4
export CHRONOS_NATIVE_SQLITE_CACHE_SIZE_KIB=262144

docker rm -f "$qdrant_container" >/dev/null 2>&1 || true
if [[ ! -e "$state/.capture-root-ready" ]]; then
  if [[ -d "$qdrant_storage" ]]; then
    sudo find "$qdrant_storage" -depth -delete
  fi
fi
mkdir -p "$qdrant_storage"
chmod 0777 "$qdrant_storage"
docker run -d \
  --name "$qdrant_container" \
  --memory 4g \
  --memory-swap 4g \
  -e QDRANT__SERVICE__MAX_WORKERS=8 \
  -p 6386:6333 \
  -p 6387:6334 \
  -v "$qdrant_storage:/qdrant/storage" \
  qdrant/qdrant@sha256:75eab8c4ba42096724fdcfde8b4de0b5713d529dde32f285a1f86fdcb2c9e50c \
  >/dev/null
for _ in $(seq 1 120); do
  if curl -fsS "$qdrant_url/readyz" >/dev/null; then
    break
  fi
  sleep 1
done
curl -fsS "$qdrant_url/readyz" >/dev/null

common_args=(
  --dimensions 384
  --state-dir "$state"
  --placeholder-zero-embeddings
  --qdrant-url "$qdrant_url"
  --qdrant-storage-dir "$qdrant_storage"
)

if [[ ! -e "$state/.capture-root-ready" ]]; then
  if [[ -d "$state" ]]; then
    rm -rf "$state"
  fi
  mkdir -p "$state"
  "$cli" "${common_args[@]}" \
    ingest-snapshot "$company_snapshot" \
    --branch main \
    --batch-size 256 \
    --progress-every 1000 \
    "${company_args[@]}" \
    2>&1 | tee -a "$log"
  "$cli" "${common_args[@]}" \
    ingest-snapshot "$code_snapshot" \
    --branch main \
    --batch-size 256 \
    --progress-every 1000 \
    2>&1 | tee -a "$log"
  "$cli" "${common_args[@]}" \
    init-hierarchy \
    --snapshot "$company_snapshot" \
    2>&1 | tee -a "$log"
  "$cli" "${common_args[@]}" \
    storage-stats >"$artifact/starting-state-storage.json"
  touch "$state/.capture-root-ready"
fi

if [[ ! -e "$codex_home/auth.json" ]]; then
  cp "$HOME/.codex/auth.json" "$codex_home/auth.json"
  chmod 600 "$codex_home/auth.json"
fi
export CODEX_HOME="$codex_home"
"$codex" mcp remove chronos_enterprise_knowledge >/dev/null 2>&1 || true
"$codex" mcp add chronos_enterprise_knowledge -- \
  "$cli" \
  --dimensions 384 \
  --state-dir "$state" \
  --placeholder-zero-embeddings \
  --qdrant-url "$qdrant_url" \
  --qdrant-storage-dir "$qdrant_storage" \
  serve >>"$log" 2>&1

"$python" "$app/scripts/capture_codex_workflows.py" \
  --prompts-dir "$prompt_dir" \
  --runs-dir "$runs" \
  --traces-dir "$traces" \
  --workdir "$workdir" \
  --session-root "$codex_home/sessions" \
  --codex "$codex" \
  --codex-config features.unified_exec=false \
  --codex-config features.multi_agent=false \
  --state-dir "$state" \
  --qdrant-url "$qdrant_url" \
  --qdrant-storage-dir "$qdrant_storage" \
  --dimensions 384 \
  --snapshot-manifest "$company_snapshot/manifest.json" \
  --snapshot-manifest "$code_snapshot/manifest.json" \
  --start 1 \
  --end 16 \
  --resume 2>&1 | tee -a "$log"
