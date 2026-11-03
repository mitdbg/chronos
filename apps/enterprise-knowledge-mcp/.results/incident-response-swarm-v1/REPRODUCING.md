# Reproduce the incident-response swarm

Run commands from the repository root. The released traces replay recorded
operations and model delays; no model API access is required.

## Verify results and generate figures

Install Git LFS and the application with its plotting extra:

```bash
git lfs install
git lfs pull --include='apps/enterprise-knowledge-mcp/.results/**'
uv pip install --python .venv/bin/python \
  -e 'packages/chronos-core[qdrant,mcp]' \
  -e 'apps/enterprise-knowledge-mcp[experiment]'
.venv/bin/python apps/enterprise-knowledge-mcp/scripts/reproduce_incident_paper_artifact.py
```

This verifies checksums and writes figures/tables to `analysis/`.
Use `--verify-only` to skip plotting or `--output /another/directory` to write
elsewhere. No database services are needed for this step. Completion percentages
count finished workers; verifier violations are reported separately. The
Serialized 128-worker trial is marked partial (121/128 completed).

## Prepare services and replay

Restore the snapshot using the [dataset guide](../../datasets/enterprise_rag_infra_v1/REPRODUCING.md).
It contains 628,081 documents and 10,704,651 chunks. Use its placeholder-zero
dense vectors for this state-management experiment; do not backfill embeddings.

Provision Linux/FUSE, PostgreSQL, Doltgres, a writable Btrfs mount, and separate
but equivalently configured Qdrant services for the two backends. The service
configuration uses Qdrant v1.18.2, an 8 GiB limit and eight search workers.
See the app's native-backend documentation and
`apps/enterprise-knowledge-mcp/infra/native-branching/docker-compose.yml`.

Use dedicated disposable services: the sweep resets `team/site-reliability`
before every trial. Review traces before replay; captured shell/patch operations
are enabled. Begin with one worker before increasing concurrency.

```bash
export EXPERIMENT_ROOT=/data/incident-reproduction
export SNAPSHOT_DIR=/data/paper-data/document-snapshot
export CHRONOS_POSTGRES_DSN='postgresql://USER:PASSWORD@HOST:PORT/DEDICATED_DB'
export DOLTGRES_DSN='postgresql://USER:PASSWORD@HOST:PORT/DEDICATED_DB'
export CHRONOS_QDRANT_URL='http://HOST:PORT'
export NATIVE_QDRANT_URL='http://OTHER_HOST:PORT'
export CHRONOS_POSTGRES_DATA_DIR=/data/services/chronos-postgres
export DOLTGRES_DATA_DIR=/data/services/doltgres
export CHRONOS_QDRANT_STORAGE_DIR=/data/services/chronos-qdrant
export NATIVE_QDRANT_STORAGE_DIR=/data/services/native-qdrant
export BTRFS_ROOT=/mnt/benchmark-btrfs/incident-reproduction

bash apps/enterprise-knowledge-mcp/scripts/replay_incident_paper_artifact.sh prepare chronos
bash apps/enterprise-knowledge-mcp/scripts/replay_incident_paper_artifact.sh prepare native
bash apps/enterprise-knowledge-mcp/scripts/replay_incident_paper_artifact.sh run --workers 1
# Full worker-count sweep, once resources have been validated:
bash apps/enterprise-knowledge-mcp/scripts/replay_incident_paper_artifact.sh run \
  --workers 1 2 4 8 16 32 64 128
```

Outputs go to `$EXPERIMENT_ROOT/results`, leaving released results unchanged.
The wrapper applies bounded connection pools, unlimited transient merge retries
and startup batch size 1. Above eight workers, the eight traces are reused with
distinct namespaces. The verifier uses a separate busy-loop process. Serialized
holds a process-shared lock for the complete agent replay, including model
delays. Per-trial settings are recorded in `config.json`.
