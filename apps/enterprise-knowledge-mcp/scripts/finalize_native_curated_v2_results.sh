#!/usr/bin/env bash
set -euo pipefail

repo=/home/ubuntu/TAR-OS/chronos
app="$repo/apps/enterprise-knowledge-mcp"
artifact="$repo/.enterprise-knowledge/enterprise-rag-curated-v2"
native_output="$artifact/benchmarks/20260728-native-priority-all-r1"
paired_output="$artifact/benchmarks/20260728-all-chronos-app-r1"
final_output="$artifact/benchmarks/20260728-all-three-backends-r1"
traces="$app/workflows/curated-v2/traces/full-zero-v2"
log="$artifact/native-priority-finalize.log"
python="$repo/.venv/bin/python"
service=chronos-enterprise-native-priority.service

mkdir -p "$paired_output" "$final_output"
cd "$repo"

log_phase() {
  printf '%s phase=%s\n' "$(date -u +%FT%TZ)" "$1" | tee -a "$log"
}

log_phase wait-for-native-pipeline
while true; do
  state=$(systemctl --user show "$service" -p ActiveState --value)
  case "$state" in
    active|activating|deactivating|reloading)
      sleep 60
      ;;
    *)
      break
      ;;
  esac
done

result=$(systemctl --user show "$service" -p Result --value)
if [[ "$result" != success ]]; then
  printf 'native pipeline did not finish successfully: %s\n' "$result" >&2
  exit 1
fi

native_complete="$native_output/sequence-checkpoints/repeat-000-doltgres-qdrant-btrfs/.complete"
if [[ ! -f "$native_complete" ]]; then
  printf 'native sequence completion marker is missing\n' >&2
  exit 1
fi

native_reports=$(
  find "$native_output" -mindepth 2 -maxdepth 2 -name results.json | wc -l
)
if [[ "$native_reports" -ne 13 ]]; then
  printf 'expected 13 native reports, found %s\n' "$native_reports" >&2
  exit 1
fi

audit_root() {
  local root=$1
  local expected_backends=$2
  "$python" - "$root" "$expected_backends" <<'PY' >>"$log"
import json
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1])
expected_backends = sys.argv[2].split(",")
expected_workflows = {f"{number:02d}" for number in range(1, 14)}
reports = sorted(root.glob("[0-9][0-9]-*/results.json"))
found = {path.parent.name[:2] for path in reports}
if found != expected_workflows:
    raise RuntimeError(
        f"workflow set mismatch in {root}: "
        f"expected={sorted(expected_workflows)}, found={sorted(found)}"
    )
for path in reports:
    report = json.loads(path.read_text(encoding="utf-8"))
    if report["backends"] != expected_backends:
        raise RuntimeError(
            f"wrong backends in {path}: {report['backends']}"
        )
    if not report["matched"]:
        raise RuntimeError(f"logical state mismatch in {path}")
    if not all(run["replay"]["succeeded"] for run in report["runs"]):
        raise RuntimeError(f"failed replay in {path}")
    if not all(
        comparison["baseline_state_matched"]
        and comparison["final_state_matched"]
        and comparison["replay_succeeded"]
        for comparison in report["comparisons"]
    ):
        raise RuntimeError(f"failed comparison in {path}")
    figure = path.parent / "summary" / "backend_comparison.pdf"
    subprocess.run(
        ["pdfinfo", str(figure)],
        check=True,
        stdout=subprocess.DEVNULL,
    )
combined = root / "summary" / "backend_comparison.pdf"
subprocess.run(
    ["pdfinfo", str(combined)],
    check=True,
    stdout=subprocess.DEVNULL,
)
(root / ".verified").write_text("verified\n", encoding="utf-8")
print(f"verified {len(reports)} workflows in {root}")
PY
}

log_phase audit-native-results
audit_root "$native_output" "doltgres-qdrant-btrfs"

log_phase run-chronos-and-app-managed-after-native
if [[ ! -f "$paired_output/.verified" ]]; then
  BENCHMARK_REPETITIONS=1 \
  ENTERPRISE_BENCHMARK_OUTPUT="$paired_output" \
    "$app/scripts/run_curated_v2_pipeline.sh" >>"$log" 2>&1
  audit_root "$paired_output" "chronos,app-managed"
fi

log_phase merge-all-backends
"$python" \
  "$app/scripts/merge_workflow_backend_results.py" \
  --primary-root "$paired_output" \
  --additional-root "$native_output" \
  --output-root "$final_output" \
  --traces-dir "$traces" \
  --summarizer "$app/workflows/real/summarize_results.py" \
  >>"$log" 2>&1
"$python" \
  "$app/scripts/summarize_workflow_suite.py" \
  "$final_output" \
  --output-dir "$final_output/summary" \
  >>"$log" 2>&1

log_phase audit-final-results
audit_root \
  "$final_output" \
  "chronos,doltgres-qdrant-btrfs,app-managed"

log_phase resume-minilm-backfill
systemctl --user start chronos-enterprise-minilm-backfill.service
systemctl --user is-active --quiet \
  chronos-enterprise-minilm-backfill.service

log_phase complete
