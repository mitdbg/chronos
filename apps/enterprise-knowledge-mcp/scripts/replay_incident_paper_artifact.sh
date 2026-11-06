#!/usr/bin/env bash
# Replay released traces, not fresh model generations. Provision services first.
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
app_dir=$(cd -- "$script_dir/.." && pwd)
repo_dir=$(cd -- "$app_dir/../.." && pwd)
python=${PYTHON:-"$repo_dir/.venv/bin/python"}
artifact="$app_dir/.results/incident-response-swarm-v1"
if [[ $# -lt 1 ]]; then
  echo "Usage: bash $0 prepare chronos|native | run [sweep arguments]" >&2
  exit 2
fi
action=$1
shift
: "${EXPERIMENT_ROOT:?Set an absolute path to a disposable benchmark directory}"
if [[ "$EXPERIMENT_ROOT" != /* || "$EXPERIMENT_ROOT" == / ]]; then
  echo 'EXPERIMENT_ROOT must be an absolute, non-root path' >&2
  exit 2
fi
chronos_state="$EXPERIMENT_ROOT/states/chronos"
native_state="$EXPERIMENT_ROOT/states/native"
case "$action" in
  prepare)
    : "${SNAPSHOT_DIR:?Set to restored document-snapshot directory}"
    backend=${1:?Specify chronos or native}
    args=(--dimensions 384 --placeholder-zero-embeddings)
    if [[ "$backend" == chronos ]]; then
      : "${CHRONOS_POSTGRES_DSN:?Set dedicated Chronos PostgreSQL database URL}"
      : "${CHRONOS_QDRANT_URL:?Set Chronos Qdrant service URL}"
      args+=(--backend chronos --state-dir "$chronos_state"
        --chronos-postgres-dsn "$CHRONOS_POSTGRES_DSN" --qdrant-url "$CHRONOS_QDRANT_URL")
    elif [[ "$backend" == native ]]; then
      : "${DOLTGRES_DSN:?Set dedicated Doltgres database URL}"
      : "${NATIVE_QDRANT_URL:?Set native Qdrant service URL}"
      : "${BTRFS_ROOT:?Set a dedicated directory on a Btrfs filesystem}"
      args+=(--backend doltgres-qdrant-btrfs --state-dir "$native_state"
        --doltgres-dsn "$DOLTGRES_DSN" --qdrant-url "$NATIVE_QDRANT_URL" --btrfs-root "$BTRFS_ROOT")
    else
      echo 'Expected chronos or native' >&2
      exit 2
    fi
    "$python" -m chronos_enterprise_knowledge.cli "${args[@]}" \
      ingest-snapshot "$SNAPSHOT_DIR" --branch main --batch-size 256 --resume --progress-every 1000
    "$python" -m chronos_enterprise_knowledge.cli "${args[@]}" init-hierarchy --snapshot "$SNAPSHOT_DIR"
    ;;
  run)
    : "${CHRONOS_POSTGRES_DSN:?}" "${DOLTGRES_DSN:?}" "${BTRFS_ROOT:?}"
    : "${CHRONOS_QDRANT_URL:?}" "${NATIVE_QDRANT_URL:?}"
    : "${CHRONOS_QDRANT_STORAGE_DIR:?}" "${NATIVE_QDRANT_STORAGE_DIR:?}"
    : "${CHRONOS_POSTGRES_DATA_DIR:?}" "${DOLTGRES_DATA_DIR:?}"
    "$python" "$script_dir/reproduce_incident_paper_artifact.py" --verify-only
    exec "$python" "$script_dir/run_incident_response_swarm_sweep.py" \
      --output-root "$EXPERIMENT_ROOT/results" \
      --traces-dir "$artifact/traces" --inputs "$artifact/incident-inputs.jsonl" \
      --repo-dir "$repo_dir" --chronos-state-dir "$chronos_state" --native-state-dir "$native_state" \
      --chronos-qdrant-url "$CHRONOS_QDRANT_URL" --native-qdrant-url "$NATIVE_QDRANT_URL" \
      --chronos-qdrant-storage-dir "$CHRONOS_QDRANT_STORAGE_DIR" \
      --native-qdrant-storage-dir "$NATIVE_QDRANT_STORAGE_DIR" \
      --chronos-postgres-dsn "$CHRONOS_POSTGRES_DSN" --chronos-postgres-data-dir "$CHRONOS_POSTGRES_DATA_DIR" \
      --doltgres-dsn "$DOLTGRES_DSN" --doltgres-data-dir "$DOLTGRES_DATA_DIR" --btrfs-root "$BTRFS_ROOT" \
      --dimensions 384 --no-session-epochs --merge-retries unlimited --startup-batch-size 1 "$@"
    ;;
  *) echo 'Expected prepare or run' >&2; exit 2 ;;
esac
