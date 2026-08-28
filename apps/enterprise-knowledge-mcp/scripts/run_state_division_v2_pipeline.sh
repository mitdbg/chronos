#!/usr/bin/env bash
set -Eeuo pipefail

repo=/home/ubuntu/TAR-OS/chronos
app="$repo/apps/enterprise-knowledge-mcp"
artifact_root="$repo/.enterprise-knowledge/enterprise-rag-curated-v2"
corpus="${ENTERPRISE_CORPUS:-/home/ubuntu/TAR-OS/EnterpriseRAG-Bench/generated_data_infra_v1}"
document_snapshot="${ENTERPRISE_DOCUMENT_SNAPSHOT:-$artifact_root/document-snapshot}"
code_snapshot="${ENTERPRISE_CODE_SNAPSHOT:-$artifact_root/code-snapshot}"
traces="${ENTERPRISE_TRACES_DIR:-$app/workflows/curated-v2/traces/full-zero-v2}"
run_id="${ENTERPRISE_RUN_ID:-state-division-v2}"
output_root="$artifact_root/benchmarks/$run_id"
persistent_runtime_root="$repo/.enterprise-knowledge/runtime"
runtime_image_suffix="${run_id//[^A-Za-z0-9._-]/-}"
runtime_image="${ENTERPRISE_RUNTIME_IMAGE:-$persistent_runtime_root/state-division-v2-${runtime_image_suffix}.btrfs.img}"
runtime_image_created=0
runtime_root="${ENTERPRISE_RUNTIME_ROOT:-/mnt/chronos-enterprise-state-division-v2}"
runtime_image_size="${ENTERPRISE_RUNTIME_IMAGE_SIZE:-80G}"
# Keep service state outside the agent's home tree.  App-managed storage has
# one filesystem object per ingested document; placing that backing store under
# the repository makes captured commands such as `find /home/ubuntu` traverse
# hundreds of thousands of unrelated service files.  A real deployment keeps
# the service volume separate from the agent workspace, so use /tmp by default
# while retaining an explicit override for durable/resumable runs.
host_runtime_root="${ENTERPRISE_HOST_RUNTIME_ROOT:-/tmp/chronos-enterprise-state-division-v2-host}"
state_root="$host_runtime_root/states"
service_root="$host_runtime_root/services"
work_root="$host_runtime_root/work"
replay_workdir="$host_runtime_root/replay-workdir"
# Checkout scratch is kept outside the agent home tree even when a caller
# overrides the durable metadata root for resume.  The branch metadata and
# object store may be durable; materialized workspaces are disposable.
app_checkout_root="${ENTERPRISE_APP_CHECKOUT_ROOT:-/tmp/chronos-enterprise-state-division-v2-app-managed-checkouts/$run_id}"
log="$output_root/pipeline.log"
status="$output_root/status.tsv"

qdrant_container="${ENTERPRISE_QDRANT_CONTAINER:-chronos-enterprise-state-v2-qdrant}"
qdrant_port=${ENTERPRISE_QDRANT_PORT:-6340}
qdrant_grpc_port=${ENTERPRISE_QDRANT_GRPC_PORT:-6341}
qdrant_url="http://127.0.0.1:$qdrant_port"
qdrant_image="${ENTERPRISE_QDRANT_IMAGE:-qdrant/qdrant@sha256:75eab8c4ba42096724fdcfde8b4de0b5713d529dde32f285a1f86fdcb2c9e50c}"
# Qdrant manages its own WAL and immutable segments. Keeping those files on
# the host filesystem avoids an unnecessary loop-mounted CoW layer; Btrfs is
# reserved for branchable document workspaces.
qdrant_storage="${ENTERPRISE_QDRANT_STORAGE:-$persistent_runtime_root/state-division-v2-qdrant}"

dolt_container="${ENTERPRISE_DOLTGRES_CONTAINER:-chronos-enterprise-state-v2-doltgres}"
dolt_port=${ENTERPRISE_DOLTGRES_PORT:-55440}
dolt_database="${ENTERPRISE_DOLTGRES_DATABASE:-chronos_enterprise_state_v2}"
dolt_storage="$service_root/doltgres"
dolt_admin_dsn="postgresql://postgres:password@127.0.0.1:$dolt_port/postgres"
dolt_dsn="postgresql://postgres:password@127.0.0.1:$dolt_port/$dolt_database"
chronos_postgres_container="${ENTERPRISE_CHRONOS_POSTGRES_CONTAINER:-chronos-enterprise-state-v2-postgres}"
chronos_postgres_port=${ENTERPRISE_CHRONOS_POSTGRES_PORT:-55441}
chronos_postgres_database="${ENTERPRISE_CHRONOS_POSTGRES_DATABASE:-chronos_enterprise_state_v2}"
chronos_postgres_image="${ENTERPRISE_CHRONOS_POSTGRES_IMAGE:-postgres@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685}"
chronos_postgres_storage="$service_root/chronos-postgres"
chronos_postgres_dsn="postgresql://postgres:password@127.0.0.1:$chronos_postgres_port/$chronos_postgres_database"
native_workspaces="${ENTERPRISE_NATIVE_WORKSPACES:-$runtime_root/native-workspaces}"

cli="$repo/.venv/bin/chronos-enterprise-knowledge"
python="$repo/.venv/bin/python"
summarizer="$app/workflows/real/summarize_results.py"
suite_summarizer="$app/scripts/summarize_workflow_suite.py"
runner="$app/scripts/run_full_workflow_benchmarks.py"
merger="$app/scripts/merge_workflow_backend_results.py"
qdrant_verifier="$app/scripts/verify_qdrant_benchmark.py"
embedding_model="sentence-transformers/all-MiniLM-L6-v2#onnx:onnx/model_qint8_avx512.onnx"
batch_size=${ENTERPRISE_INGEST_BATCH_SIZE:-512}
resume=${ENTERPRISE_RESUME:-1}
document_max_documents=${ENTERPRISE_DOCUMENT_MAX_DOCUMENTS:-}
code_max_documents=${ENTERPRISE_CODE_MAX_DOCUMENTS:-}
include_code_snapshot=${ENTERPRISE_INCLUDE_CODE_SNAPSHOT:-1}
use_backfilled_embeddings=${ENTERPRISE_USE_BACKFILLED_EMBEDDINGS:-0}
backend_selection=${ENTERPRISE_BACKENDS:-chronos,native-branching,app-managed}
workflow_start=${ENTERPRISE_WORKFLOW_START:-1}
workflow_end=${ENTERPRISE_WORKFLOW_END:-13}
expected_trace_count=${ENTERPRISE_EXPECTED_TRACE_COUNT:-13}
sqlite_buffer_kib=${ENTERPRISE_SQLITE_BUFFER_KIB:-4194304}
doltgres_memory_limit=${ENTERPRISE_DOLTGRES_MEMORY_LIMIT:-4g}
doltgres_go_memory_limit=${ENTERPRISE_DOLTGRES_GO_MEMORY_LIMIT:-4GiB}
chronos_postgres_memory_limit=${ENTERPRISE_CHRONOS_POSTGRES_MEMORY_LIMIT:-8g}
chronos_postgres_memory_swap_limit=${ENTERPRISE_CHRONOS_POSTGRES_MEMORY_SWAP_LIMIT:-$chronos_postgres_memory_limit}
qdrant_memory_limit=${ENTERPRISE_QDRANT_MEMORY_LIMIT:-8g}
qdrant_memory_swap_limit=${ENTERPRISE_QDRANT_MEMORY_SWAP_LIMIT:-$qdrant_memory_limit}
qdrant_max_optimization_threads=${ENTERPRISE_QDRANT_MAX_OPTIMIZATION_THREADS:-4}
qdrant_max_indexing_threads=${ENTERPRISE_QDRANT_MAX_INDEXING_THREADS:-4}
qdrant_upload_workers=${ENTERPRISE_QDRANT_UPLOAD_WORKERS:-4}
qdrant_service_workers=${ENTERPRISE_QDRANT_SERVICE_WORKERS:-8}
qdrant_shards=${ENTERPRISE_QDRANT_SHARDS:-4}
qdrant_point_batch_size=${ENTERPRISE_QDRANT_POINT_BATCH_SIZE:-256}
qdrant_bulk_hnsw_m=${ENTERPRISE_QDRANT_BULK_HNSW_M:-0}
qdrant_query_hnsw_m=${ENTERPRISE_QDRANT_QUERY_HNSW_M:-0}
qdrant_query_timeout_seconds=${ENTERPRISE_QDRANT_QUERY_TIMEOUT_SECONDS:-600}
qdrant_on_disk=${ENTERPRISE_QDRANT_ON_DISK:-1}
qdrant_query_indexed_only=${ENTERPRISE_QDRANT_QUERY_INDEXED_ONLY:-1}
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
export CHRONOS_QDRANT_QUERY_TIMEOUT_SECONDS="$qdrant_query_timeout_seconds"
export CHRONOS_QDRANT_ON_DISK="$qdrant_on_disk"
export CHRONOS_QDRANT_QUERY_INDEXED_ONLY="$qdrant_query_indexed_only"
export CHRONOS_BM25_THREADS="$bm25_threads"
export CHRONOS_INGEST_PROFILE=1

mkdir -p "$output_root" "$persistent_runtime_root"

manifest_complete() {
  "$python" - "$1" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.is_file():
    raise SystemExit(1)
try:
    manifest = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
raise SystemExit(0 if manifest.get("complete") else 1)
PY
}

# Snapshot preparation is resumable.  Check completeness rather than only
# checking for a manifest: a killed preparation leaves a valid but incomplete
# manifest, and a fresh replay must resume it instead of failing preflight.
if ! manifest_complete "$document_snapshot/manifest.json"; then
  "$cli" \
    --dimensions 384 \
    --placeholder-zero-embeddings \
    prepare-snapshot "$corpus" "$document_snapshot" \
    --sample-fraction 1 \
    --sample-seed chronos-enterprise-infra-v1 \
    --batch-size 256 \
    --chunk-workers 2 \
    --progress-every 1000
fi
if [[ "$include_code_snapshot" == 1 ]] && ! manifest_complete "$code_snapshot/manifest.json"; then
  "$cli" \
    --dimensions 384 \
    --placeholder-zero-embeddings \
    prepare-snapshot "$corpus" "$code_snapshot" \
    --sample-fraction 1 \
    --sample-seed chronos-enterprise-infra-v1 \
    --connector codebase \
    --batch-size 128 \
    --chunk-workers 2 \
    --progress-every 1000
fi
touch "$log" "$status"
exec 9>"$output_root/.pipeline.lock"
if ! flock -n 9; then
  printf 'benchmark run is already active: %s\n' "$output_root" >&2
  exit 1
fi

"$python" - \
  "$output_root/qdrant-deployment.json" \
  "$qdrant_image" \
  "$qdrant_memory_limit" \
  "$qdrant_service_workers" \
  "$qdrant_max_optimization_threads" \
  "$qdrant_max_indexing_threads" \
  "$qdrant_shards" \
  "$qdrant_indexing_threshold_kib" \
  "$qdrant_query_hnsw_m" \
  "$qdrant_query_timeout_seconds" \
  "$qdrant_on_disk" \
  "$qdrant_query_indexed_only" \
  "$backend_selection" \
  "$use_backfilled_embeddings" <<'PY'
import json
import sys
from pathlib import Path

(
    destination,
    image,
    memory,
    service_workers,
    optimization_threads,
    indexing_threads,
    shards,
    indexing_threshold_kib,
    hnsw_m,
    query_timeout_seconds,
    on_disk,
    query_indexed_only,
    backends,
    use_backfilled_embeddings,
) = sys.argv[1:]
Path(destination).write_text(
    json.dumps(
        {
            "schema_version": 1,
            "deployment": "docker",
            "image": image,
            "state_isolation": "fresh_service_storage_per_backend",
            "backends": backends.split(","),
            "embedding_mode": (
                "backfilled" if bool(int(use_backfilled_embeddings)) else "placeholder"
            ),
            "shared_configuration": {
                "memory_limit": memory,
                "service_workers": int(service_workers),
                "max_optimization_threads": int(optimization_threads),
                "max_indexing_threads": int(indexing_threads),
                "shards": int(shards),
                "indexing_threshold_kib": int(indexing_threshold_kib),
                "hnsw_m": int(hnsw_m),
                "query_timeout_seconds": int(query_timeout_seconds),
                "on_disk": bool(int(on_disk)),
                "query_indexed_only": bool(int(query_indexed_only)),
                "transport": "grpc",
            },
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
PY

"$python" - \
  "$output_root/chronos-postgres-deployment.json" \
  "$chronos_postgres_image" \
  "$chronos_postgres_memory_limit" \
  "$chronos_postgres_memory_swap_limit" \
  "$chronos_postgres_port" \
  "$chronos_postgres_database" \
  "$chronos_postgres_storage" \
  "$backend_selection" <<'PY'
import json
import sys
from pathlib import Path

destination, image, memory, memory_swap, port, database, storage, backends = sys.argv[1:]
Path(destination).write_text(
    json.dumps(
        {
            "schema_version": 1,
            "deployment": "docker",
            "image": image,
            "database": database,
            "port": int(port),
            "host_storage": str(Path(storage).resolve()),
            "memory_limit": memory,
            "memory_swap_limit": memory_swap,
            "used_by": ["chronos"] if "chronos" in backends.split(",") else [],
            "state_isolation": "fresh_service_storage_per_chronos_run",
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
PY

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
    local mounted_source backing_file expected_image
    mounted_source=$(findmnt -rn -M "$runtime_root" -o SOURCE 2>/dev/null || true)
    backing_file=$(sudo losetup --noheadings --output BACK-FILE "$mounted_source" 2>/dev/null |
      sed 's/^ *//;s/ *$//' || true)
    if [[ -n "$backing_file" ]]; then
      expected_image=$(readlink -f "$runtime_image")
      if [[ "$(readlink -f "$backing_file")" != "$expected_image" ]]; then
        printf '%s is mounted from %s, expected %s\n' \
          "$runtime_root" "$backing_file" "$expected_image" >&2
        return 1
      fi
    fi
    return
  fi

  sudo mkdir -p "$runtime_root"
  if [[ ! -e "$runtime_image" ]]; then
    truncate -s "$runtime_image_size" "$runtime_image"
    sudo mkfs.btrfs -q -f "$runtime_image"
    runtime_image_created=1
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

record_btrfs_runtime() {
  local mount_info
  mount_info=$(findmnt -rn -M "$runtime_root" -o TARGET,SOURCE,FSTYPE,OPTIONS)
  printf '%s btrfs_runtime image=%s image_created=%s mount=%s native_root=%s\n' \
    "$(date -u +%FT%TZ)" \
    "$runtime_image" \
    "$runtime_image_created" \
    "$mount_info" \
    "$native_workspaces" | tee -a "$log"
  "$python" - \
    "$output_root/btrfs-runtime.json" \
    "$runtime_image" \
    "$runtime_root" \
    "$native_workspaces" \
    "$runtime_image_created" \
    "$resume" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

destination, image, root, native_root, created, resume = sys.argv[1:]
def command(*args: str) -> str:
    result = subprocess.run(args, check=False, text=True, capture_output=True)
    return result.stdout.strip()

Path(destination).write_text(
    json.dumps(
        {
            "schema_version": 1,
            "image": str(Path(image).resolve()),
            "mount_root": root,
            "native_workspace_root": native_root,
            "image_created_for_run": bool(int(created)),
            "resume": bool(int(resume)),
            "mount": command("findmnt", "-rn", "-M", root, "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"),
            "filesystem_usage": command("sudo", "btrfs", "filesystem", "usage", "--raw", root),
            "subvolumes": command("sudo", "btrfs", "subvolume", "list", root),
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
PY
}

record_btrfs_runtime

validate_btrfs_scope() {
  local mount_target unexpected
  mount_target=$(findmnt -rn -M "$runtime_root" -o TARGET)
  [[ "$mount_target" == "$runtime_root" ]] || return 0
  unexpected=$(find "$runtime_root" -mindepth 1 -maxdepth 1 \
    ! -name "$(basename "$native_workspaces")" -printf '%p\n' | sort)
  if [[ -n "$unexpected" ]]; then
    printf '%s btrfs_scope_unexpected=%s\n' \
      "$(date -u +%FT%TZ)" "${unexpected//$'\n'/,}" | tee -a "$log"
    if [[ "$resume" != 1 ]]; then
      printf 'refusing a non-resume run on a Btrfs image with unrelated root entries\n' >&2
      return 1
    fi
  fi
}

validate_btrfs_scope
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
  local relative path workspace_mount
  local -a subvolumes=()
  mkdir -p "$native_workspaces"
  # ``btrfs subvolume list`` reports paths relative to the filesystem mount,
  # which may differ from runtime_root when the workspace volume is shared by
  # multiple benchmark services.
  workspace_mount=$(findmnt -n -T "$native_workspaces" -o TARGET)
  mapfile -t subvolumes < <(
    sudo btrfs subvolume list -o "$native_workspaces" |
      sed -n 's/^.* path //p' |
      sort -r
  )
  for relative in "${subvolumes[@]}"; do
    path="$workspace_mount/$relative"
    case "$path/" in
      "$native_workspaces/"*) ;;
      *)
        printf 'refusing to delete Btrfs subvolume outside %s: %s\n' \
          "$native_workspaces" "$path" >&2
        continue
        ;;
    esac
    # Native snapshots may be owned by the service account that performed
    # ingestion.  Use the same privileged path as subvolume enumeration so
    # an interrupted run can be cleaned without depending on file ownership.
    sudo btrfs property set "$path" ro false >/dev/null 2>&1 || true
    sudo btrfs subvolume delete "$path"
  done
  sudo find "$native_workspaces" -mindepth 1 -depth -delete
  # Subvolume deletion is asynchronous.  Synchronize before measuring or
  # trimming so a subsequent backend cannot inherit transient allocator use.
  sudo btrfs subvolume sync "$runtime_root" >/dev/null 2>&1 || true
  sudo btrfs filesystem sync "$runtime_root" >/dev/null 2>&1 || true
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
  until curl --retry 3 --retry-all-errors --retry-delay 1 -fsS \
    "$qdrant_url/collections" >/dev/null; do
    if (( SECONDS >= deadline )); then
      printf 'Qdrant did not become ready\n' >&2
      return 1
    fi
    sleep 2
  done
  # The replay clients use Qdrant gRPC.  REST readiness alone is not enough:
  # Qdrant can recover persisted collections and open HTTP before its gRPC
  # listener accepts connections.  Do not start ingestion until both
  # endpoints are reachable.
  until (echo >/dev/tcp/127.0.0.1/"$qdrant_grpc_port") 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      printf 'Qdrant gRPC listener did not become ready\n' >&2
      return 1
    fi
    sleep 2
  done
}

qdrant_collections() {
  # The REST listener can accept a connection while the storage service is
  # still initializing. Retry the list operation as well as the readiness
  # probe so a transient connection reset cannot abort a long benchmark.
  local attempt response
  for attempt in {1..60}; do
    if response=$(curl --connect-timeout 5 --max-time 60 \
      --retry 3 --retry-all-errors --retry-delay 1 -fsS \
      "$qdrant_url/collections"); then
      printf '%s\n' "$response"
      return 0
    fi
    sleep 2
  done
  return 1
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
    qdrant_collections |
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
      qdrant_collections |
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
    status = state.get("status")
    # A restored Qdrant collection can report grey while its persisted shard
    # state is being adopted.  Likewise, a local collection with bulk HNSW
    # disabled can remain yellow indefinitely after every shard is active and
    # its optimizer queue is drained.  Both states are safe to use once all
    # local shards are active and no transfer is in progress; requiring green
    # here would make resumable runs fail or wait forever for an index that
    # the experiment intentionally does not require.
    shards_active = False
    if (
        status in {"grey", "yellow"}
        and queue == 0
        and state.get("optimizer_status") == "ok"
    ):
        try:
            with urllib.request.urlopen(
                f"{url}/collections/{name}/cluster", timeout=30
            ) as response:
                cluster = json.load(response).get("result", {})
            local_shards = cluster.get("local_shards") or []
            shards_active = bool(local_shards) and all(
                shard.get("state") == "Active" for shard in local_shards
            ) and not cluster.get("shard_transfers")
        except Exception:
            shards_active = False
    ready = (
        queue == 0
        and (
            status == "green"
            or (status in {"grey", "yellow"} and shards_active)
        )
    )
    if not ready:
        pending.append("{}:{}:queue={}".format(name, status, queue))
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
  # Keep the same Qdrant indexing configuration for every backend. Placeholder
  # runs may disable HNSW because dense vectors carry no signal; a backfilled
  # run uses the configured query index for real dense retrieval.
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
    qdrant_collections |
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
  if [[ "$preserve" == 1 ]] &&
    docker inspect --format '{{.State.Running}}' "$qdrant_container" \
      2>/dev/null | grep -qx true; then
    wait_for_qdrant
    tune_existing_qdrant_collections
    wait_for_qdrant_idle
    return
  fi
  docker stop "$qdrant_container" >/dev/null 2>&1 || true
  docker rm "$qdrant_container" >/dev/null 2>&1 || true
  mkdir -p "$qdrant_storage"
  if [[ "$preserve" != 1 ]]; then
    delete_service_contents "$qdrant_storage"
  fi
  sudo chmod 0777 "$qdrant_storage"
  docker run -d \
    --name "$qdrant_container" \
    --memory "$qdrant_memory_limit" \
    --memory-swap "$qdrant_memory_swap_limit" \
    -e QDRANT__SERVICE__MAX_WORKERS="$qdrant_service_workers" \
    -p "$qdrant_port:6333" \
    -p "$qdrant_grpc_port:6334" \
    -v "$qdrant_storage:/qdrant/storage" \
    "$qdrant_image" \
    >>"$log"
  wait_for_qdrant
  local actual_image expected_image
  actual_image=$(docker inspect --format '{{.Image}}' "$qdrant_container")
  expected_image=$(docker image inspect --format '{{.Id}}' "$qdrant_image")
  if [[ "$actual_image" != "$expected_image" ]]; then
    printf 'Qdrant image mismatch: running=%s expected=%s\n' \
      "$actual_image" "$expected_image" >&2
    return 1
  fi
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

wait_for_chronos_postgres() {
  "$python" - "$chronos_postgres_dsn" <<'PY'
import sys
import time

import psycopg

dsn = sys.argv[1]
deadline = time.monotonic() + 300
while True:
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute("SELECT 1").fetchone()
        break
    except Exception:
        if time.monotonic() >= deadline:
            raise
        time.sleep(2)
PY
}

start_chronos_postgres() {
  local preserve=${1:-0}
  wait_for_docker
  if [[ "$preserve" == 1 ]] &&
    docker inspect --format '{{.State.Running}}' "$chronos_postgres_container" \
      2>/dev/null | grep -qx true; then
    wait_for_chronos_postgres
    return
  fi
  docker stop "$chronos_postgres_container" >/dev/null 2>&1 || true
  docker rm "$chronos_postgres_container" >/dev/null 2>&1 || true
  mkdir -p "$chronos_postgres_storage"
  if [[ "$preserve" != 1 ]]; then
    delete_service_contents "$chronos_postgres_storage"
  fi
  sudo chmod 0777 "$chronos_postgres_storage"
  docker run -d \
    --name "$chronos_postgres_container" \
    --memory "$chronos_postgres_memory_limit" \
    --memory-swap "$chronos_postgres_memory_swap_limit" \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_DB="$chronos_postgres_database" \
    -p "$chronos_postgres_port:5432" \
    -v "$chronos_postgres_storage:/var/lib/postgresql/data" \
    "$chronos_postgres_image" \
    >>"$log"
  wait_for_chronos_postgres
  local actual_image expected_image
  actual_image=$(docker inspect --format '{{.Image}}' "$chronos_postgres_container")
  expected_image=$(docker image inspect --format '{{.Id}}' "$chronos_postgres_image")
  if [[ "$actual_image" != "$expected_image" ]]; then
    printf 'Chronos PostgreSQL image mismatch: running=%s expected=%s\n' \
      "$actual_image" "$expected_image" >&2
    return 1
  fi
  # The official image initializes PGDATA as mode 0700 owned by the container
  # user. Chronos reports physical bytes from the host-mounted volume, so make
  # metadata readable after startup without changing the server's data path or
  # any measured operation.
  sudo chmod -R a+rX "$chronos_postgres_storage"
}

stop_chronos_postgres() {
  docker stop "$chronos_postgres_container" >/dev/null 2>&1 || true
  docker rm "$chronos_postgres_container" >/dev/null 2>&1 || true
}

backend_args() {
  local backend=$1
  local state=$2
  printf '%s\n' \
    --backend "$backend" \
    --state-dir "$state" \
    --dimensions 384 \
    --qdrant-url "$qdrant_url" \
    --qdrant-storage-dir "$qdrant_storage"
  if [[ "$use_backfilled_embeddings" != 1 ]]; then
    printf '%s\n' --placeholder-zero-embeddings
  fi
  if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
    printf '%s\n' \
      --doltgres-dsn "$dolt_dsn" \
      --btrfs-root "$native_workspaces" \
      --doltgres-data-dir "$dolt_storage"
  fi
  if [[ "$backend" == chronos ]]; then
    printf '%s\n' \
      --chronos-postgres-dsn "$chronos_postgres_dsn" \
      --chronos-postgres-data-dir "$chronos_postgres_storage"
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
  if [[ "$include_code_snapshot" == 1 ]]; then
    mapfile -t code_selection_args < <(
      snapshot_selection_args \
        "$code_max_documents" \
        "$required_code_ids_file"
    )
  fi

  if [[ "$resume" == 1 && -f "$output/.complete" ]]; then
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
    if [[ "$backend" == chronos ]]; then
      delete_service_contents "$chronos_postgres_storage"
    fi
    if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
      delete_service_contents "$dolt_storage"
      delete_btrfs_workspace_contents
    fi
  fi
  mkdir -p "$state" "$backend_work" "$output"
  # ChronosFS mounts use this directory as disposable checkout scratch.  A
  # killed replay can leave files (or symlinks from an older run) underneath a
  # later mountpoint; clear them before creating the next isolated branch so
  # the replay observes only versioned workspace state.
  delete_tree_contents "$state/checkouts"
  if [[ "$backend" == app-managed ]]; then
    # Checkouts are replay scratch state.  Remove leftovers from an
    # interrupted replay before the next isolated workflow and keep them on
    # the service volume rather than under the agent's home tree.
    delete_tree_contents "$app_checkout_root"
    export CHRONOS_APP_CHECKOUT_ROOT="$app_checkout_root"
  else
    unset CHRONOS_APP_CHECKOUT_ROOT || true
  fi
  printf '%s\n' "$label" >"$active_backend_file"
  export CHRONOS_QDRANT_HNSW_M="$qdrant_bulk_hnsw_m"
  if [[ "$backend" == chronos ]]; then
    start_chronos_postgres "$preserve"
  fi
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

  if [[ "$include_code_snapshot" == 1 ]]; then
    timed_phase "$label" ingest-code \
      "$cli" "${args[@]}" ingest-snapshot "$code_snapshot" \
        --branch main \
        --batch-size "$batch_size" \
        --resume \
        --progress-every 1000 \
        "${code_selection_args[@]}"
  fi

  timed_phase "$label" finalize-vector-store \
    finalize_qdrant_storage

  local -a verifier_args=(
    "$python" "$qdrant_verifier"
    --qdrant-url "$qdrant_url"
    --backend "$backend"
    --dimensions 384
    --shards "$qdrant_shards"
    --output "$output/qdrant-collection.json"
  )
  if [[ "$use_backfilled_embeddings" == 1 ]]; then
    verifier_args+=(--require-dense-vectors)
  fi
  timed_phase "$label" verify-vector-store "${verifier_args[@]}"

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
    --base-state "$backend=$state"
    --in-place-backend "$backend"
    --qdrant-url "$qdrant_url"
    --require-remote-qdrant
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
  if [[ "$use_backfilled_embeddings" == 1 ]]; then
    runner_args+=(--real-embeddings)
  fi
  if [[ "$include_code_snapshot" == 1 ]]; then
    runner_args+=(--snapshot-manifest "$code_snapshot/manifest.json")
  fi
  if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
    runner_args+=(
      --doltgres-dsn "$dolt_dsn"
      --btrfs-root "$native_workspaces"
      --doltgres-data-dir "$dolt_storage"
    )
  fi
  if [[ "$backend" == chronos ]]; then
    runner_args+=(
      --chronos-postgres-dsn "$chronos_postgres_dsn"
      --chronos-postgres-data-dir "$chronos_postgres_storage"
    )
  fi
  timed_phase "$label" workflows "${runner_args[@]}"
  timed_phase "$label" summarize-suite \
    "$python" "$suite_summarizer" "$output" \
      --output-dir "$output/summary" \
      --traces-dir "$traces"

  printf 'complete\n' >"$output/.complete"
  timed_phase "$label" destroy-external-state \
    "$cli" "${args[@]}" destroy-state
  if [[ "$backend" == chronos ]]; then
    stop_chronos_postgres
  fi
  if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
    stop_doltgres
  fi
  stop_qdrant
  delete_tree_contents "$state_root/$label"
  if [[ "$backend" == app-managed ]]; then
    delete_tree_contents "$app_checkout_root"
  fi
  delete_service_contents "$qdrant_storage"
  if [[ "$backend" == chronos ]]; then
    delete_service_contents "$chronos_postgres_storage"
  fi
  if [[ "$backend" == doltgres-qdrant-btrfs ]]; then
    delete_service_contents "$dolt_storage"
    delete_btrfs_workspace_contents
  fi
  # The runtime image is sparse on the host filesystem.  Reclaim blocks freed
  # by the completed backend before the next backend starts so one backend's
  # deleted state does not consume host space throughout the comparison.
  sudo btrfs filesystem sync "$runtime_root" >>"$log" 2>&1 || true
  sudo fstrim -v "$runtime_root" >>"$log" 2>&1 || true
  rm -f "$active_backend_file"
}

cleanup_services() {
  stop_chronos_postgres
  stop_doltgres
  stop_qdrant
}

unmount_btrfs_runtime() {
  if findmnt -rn -M "$runtime_root" >/dev/null 2>&1; then
    sudo btrfs filesystem sync "$runtime_root" >/dev/null 2>&1 || true
    sudo umount "$runtime_root"
  fi
}
trap cleanup_services EXIT

log_phase preflight
printf '%s ingest_config qdrant_image=%s batch_size=%s bm25_threads=%s qdrant_upload_workers=%s qdrant_service_workers=%s qdrant_shards=%s qdrant_point_batch_size=%s qdrant_indexing_threshold_kib=%s qdrant_bulk_hnsw_m=%s qdrant_query_hnsw_m=%s qdrant_query_timeout_seconds=%s qdrant_on_disk=%s qdrant_query_indexed_only=%s qdrant_text_index=absent qdrant_optimization_threads=%s qdrant_indexing_threads=%s qdrant_transport=grpc qdrant_memory=%s qdrant_memory_swap=%s chronos_postgres_image=%s chronos_postgres_memory=%s chronos_postgres_memory_swap=%s chronos_postgres_port=%s chronos_postgres_database=%s\n' \
  "$(date -u +%FT%TZ)" \
  "$qdrant_image" \
  "$batch_size" \
  "$bm25_threads" \
  "$qdrant_upload_workers" \
  "$qdrant_service_workers" \
  "$qdrant_shards" \
  "$qdrant_point_batch_size" \
  "$qdrant_indexing_threshold_kib" \
  "$qdrant_bulk_hnsw_m" \
  "$qdrant_query_hnsw_m" \
  "$qdrant_query_timeout_seconds" \
  "$qdrant_on_disk" \
  "$qdrant_query_indexed_only" \
  "$qdrant_max_optimization_threads" \
  "$qdrant_max_indexing_threads" \
  "$qdrant_memory_limit" \
  "$qdrant_memory_swap_limit" \
  "$chronos_postgres_image" \
  "$chronos_postgres_memory_limit" \
  "$chronos_postgres_memory_swap_limit" \
  "$chronos_postgres_port" \
  "$chronos_postgres_database" \
  | tee -a "$log"
"$python" - \
  "$document_snapshot" \
  "$code_snapshot" \
  "$traces" \
  "$required_document_ids_file" \
  "$required_code_ids_file" \
  "$expected_trace_count" \
  "$include_code_snapshot" \
  "$use_backfilled_embeddings" <<'PY'
import json
import re
import sqlite3
import sys
from pathlib import Path

document_snapshot, code_snapshot, traces, document_output, code_output = (
    Path(value) for value in sys.argv[1:6]
)
expected_trace_count = int(sys.argv[6])
include_code_snapshot = bool(int(sys.argv[7]))
use_backfilled_embeddings = bool(int(sys.argv[8]))
manifest_paths = [document_snapshot / "manifest.json"]
if include_code_snapshot:
    manifest_paths.append(code_snapshot / "manifest.json")
for manifest_path in manifest_paths:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if not manifest.get("complete"):
        raise SystemExit(f"incomplete snapshot: {manifest_path}")
    if use_backfilled_embeddings and not manifest.get("embeddings_complete"):
        raise SystemExit(
            f"snapshot embeddings are incomplete: {manifest_path}"
        )
paths = sorted(traces.glob("[0-9][0-9]-*.jsonl"))
if len(paths) != expected_trace_count:
    raise SystemExit(
        f"expected {expected_trace_count} real Codex traces, found {len(paths)}"
    )

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
code_ids = (
    required_ids(code_snapshot, path_prefix="/code/")
    if include_code_snapshot
    else []
)
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
if backend_selected chronos && backend_selected native-branching; then
  if backend_selected app-managed; then
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
  else
    "$python" "$merger" \
      --primary-root "$output_root/01-chronos" \
      --additional-root "$output_root/02-native-branching" \
      --traces-dir "$traces" \
      --summarizer "$summarizer" \
      --output-root "$final" \
      >>"$log" 2>&1
  fi
  "$python" "$suite_summarizer" "$final" \
    --output-dir "$final/summary" \
    --traces-dir "$traces" \
    >>"$log" 2>&1
  delete_tree_contents "$combined_two"
else
  log_phase merge-backend-results:skipped-for-subset
fi

log_phase complete
printf 'complete\n' >"$output_root/.complete"
trap - EXIT
cleanup_services
unmount_btrfs_runtime
