#!/usr/bin/env bash
set -Eeuo pipefail

repo=/home/ubuntu/TAR-OS/chronos
app="$repo/apps/enterprise-knowledge-mcp"

# Reuse the controlled three-backend pipeline. It restarts the same pinned
# Docker Qdrant service recipe with fresh storage for each backend, passes the
# same remote URL and collection knobs to all three systems, and rejects any
# attempt to fall back to embedded Qdrant.
export ENTERPRISE_RUN_ID="${ENTERPRISE_RUN_ID:-atomic-updates-docker-qdrant-v1}"
export ENTERPRISE_DOCUMENT_SNAPSHOT="${ENTERPRISE_DOCUMENT_SNAPSHOT:-$repo/.enterprise-knowledge/enterprise-rag-atomic-updates-v1/snapshot}"
export ENTERPRISE_TRACES_DIR="${ENTERPRISE_TRACES_DIR:-$app/workflows/curated-v2/traces/atomic-updates-v1}"
export ENTERPRISE_EXPECTED_TRACE_COUNT="${ENTERPRISE_EXPECTED_TRACE_COUNT:-3}"
export ENTERPRISE_WORKFLOW_START="${ENTERPRISE_WORKFLOW_START:-14}"
export ENTERPRISE_WORKFLOW_END="${ENTERPRISE_WORKFLOW_END:-16}"
export ENTERPRISE_BACKENDS="${ENTERPRISE_BACKENDS:-chronos,native-branching,app-managed}"
export ENTERPRISE_INCLUDE_CODE_SNAPSHOT="${ENTERPRISE_INCLUDE_CODE_SNAPSHOT:-0}"
export ENTERPRISE_RESUME="${ENTERPRISE_RESUME:-0}"

# Keep this evaluation independent from prior state-division runs.
export ENTERPRISE_HOST_RUNTIME_ROOT="${ENTERPRISE_HOST_RUNTIME_ROOT:-$repo/.enterprise-knowledge/runtime/atomic-updates-docker-qdrant-v1-host}"
export ENTERPRISE_QDRANT_STORAGE="${ENTERPRISE_QDRANT_STORAGE:-$repo/.enterprise-knowledge/runtime/atomic-updates-docker-qdrant-v1-qdrant}"
export ENTERPRISE_QDRANT_CONTAINER="${ENTERPRISE_QDRANT_CONTAINER:-chronos-atomic-updates-controlled-qdrant}"
export ENTERPRISE_QDRANT_PORT="${ENTERPRISE_QDRANT_PORT:-6352}"
export ENTERPRISE_QDRANT_GRPC_PORT="${ENTERPRISE_QDRANT_GRPC_PORT:-6353}"
export ENTERPRISE_DOLTGRES_CONTAINER="${ENTERPRISE_DOLTGRES_CONTAINER:-chronos-atomic-updates-controlled-doltgres}"
export ENTERPRISE_DOLTGRES_PORT="${ENTERPRISE_DOLTGRES_PORT:-55442}"
export ENTERPRISE_DOLTGRES_DATABASE="${ENTERPRISE_DOLTGRES_DATABASE:-chronos_atomic_updates_controlled}"
export ENTERPRISE_NATIVE_WORKSPACES="${ENTERPRISE_NATIVE_WORKSPACES:-/mnt/chronos-enterprise-state-division-v2/atomic-updates-controlled-native-workspaces}"

exec "$app/scripts/run_state_division_v2_pipeline.sh"
