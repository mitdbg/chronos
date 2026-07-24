# Chronos Lake

Chronos Lake is one native process with two interfaces:

- an S3-compatible data plane at `/`;
- an Iceberg REST catalog under `/iceberg`.

The S3 layer treats every key and body as opaque. It stores logical-key to
immutable-physical-key mappings in the ordinary branchable SQLite table
`chronos_s3_objects`. The Iceberg router is a separate module that stores its
catalog heads and metadata JSON through that generic object interface.

Start MinIO:

```bash
docker compose -f deploy/chronos-lake/docker-compose.yml up -d
```

Start Chronos Lake:

```bash
chronos-lake \
  --database /tmp/chronos-lake.sqlite \
  --create-bucket warehouse \
  --warehouse s3://warehouse/iceberg
```

DuckDB setup:

```sql
INSTALL iceberg;
LOAD iceberg;
CREATE SECRET chronos_s3 (
  TYPE S3,
  KEY_ID 'minioadmin',
  SECRET 'minioadmin',
  ENDPOINT '127.0.0.1:9100',
  URL_STYLE 'path',
  USE_SSL false,
  REGION 'us-east-1'
);
ATTACH '' AS lake (
  TYPE ICEBERG,
  ENDPOINT 'http://127.0.0.1:9100/iceberg',
  AUTHORIZATION_TYPE 'none',
  ACCESS_DELEGATION_MODE 'none'
);
```

To adopt a quiesced pre-existing bucket, add `--bootstrap-bucket BUCKET`.
Adopted mappings point at the existing physical keys; later writes use
copy-on-write immutable objects under `_chronos/objects/`.

Bind the S3 and catalog credentials for a branch together:

```bash
chronos-lake \
  --database /tmp/chronos-lake.sqlite \
  --warehouse s3://warehouse/iceberg \
  --branch-credential trial:trial-key:trial-secret \
  --catalog-token trial:trial-token
```

DuckDB selects `trial` by using `trial-key` in its S3 secret and
`AUTHORIZATION_TYPE 'oauth2', TOKEN 'trial-token'` in its Iceberg attachment.
Catalog heads, Iceberg metadata, manifests, and data files then resolve through
the same Chronos branch.

Managed physical objects are collected asynchronously after a five-minute
grace period. GC scans references in every live branch and conservatively does
nothing while a Chronos checkpoint exists. Adopted objects are never deleted.

Run the MinIO-backed protocol, branching, bootstrap, GC, and DuckDB TPC-H
tests. The lake integration creates all eight TPC-H tables at SF 1 and
validates Q1 through Q22 against native DuckDB:

```bash
scripts/test_chronos_lake.sh
```
