from __future__ import annotations

import argparse
import json
import os
import signal
import threading

from chronos_core._native_interval import NativeChronosLakeServer


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the combined native ChronosS3 and Iceberg REST service."
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("CHRONOS_LAKE_DATABASE", "chronos-lake.sqlite"),
    )
    parser.add_argument(
        "--upstream-endpoint",
        default=os.environ.get("CHRONOS_S3_UPSTREAM_ENDPOINT", "http://127.0.0.1:9000"),
    )
    parser.add_argument(
        "--upstream-access-key",
        default=os.environ.get("CHRONOS_S3_UPSTREAM_ACCESS_KEY", "minioadmin"),
    )
    parser.add_argument(
        "--upstream-secret-key",
        default=os.environ.get("CHRONOS_S3_UPSTREAM_SECRET_KEY", "minioadmin"),
    )
    parser.add_argument("--host", default=os.environ.get("CHRONOS_LAKE_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("CHRONOS_LAKE_PORT", "9100")),
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("CHRONOS_S3_REGION", "us-east-1"),
    )
    parser.add_argument(
        "--access-key",
        default=os.environ.get("CHRONOS_S3_ACCESS_KEY", ""),
    )
    parser.add_argument(
        "--secret-key",
        default=os.environ.get("CHRONOS_S3_SECRET_KEY", ""),
    )
    parser.add_argument(
        "--warehouse",
        default=os.environ.get("CHRONOS_LAKE_WAREHOUSE", "s3://warehouse/iceberg"),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.environ.get("CHRONOS_LAKE_WORKERS", "16")),
    )
    parser.add_argument(
        "--gc-interval-seconds",
        type=int,
        default=int(os.environ.get("CHRONOS_LAKE_GC_INTERVAL_SECONDS", "30")),
    )
    parser.add_argument(
        "--gc-grace-seconds",
        type=int,
        default=int(os.environ.get("CHRONOS_LAKE_GC_GRACE_SECONDS", "300")),
    )
    parser.add_argument(
        "--create-bucket",
        action="append",
        default=[],
        help="Create an upstream bucket before accepting requests; repeatable.",
    )
    parser.add_argument(
        "--bootstrap-bucket",
        action="append",
        default=[],
        help="Adopt existing objects into main before accepting requests; repeatable.",
    )
    parser.add_argument(
        "--branch-credential",
        action="append",
        default=[],
        metavar="BRANCH:ACCESS_KEY:SECRET_KEY",
        help="Bind an additional S3 credential to a Chronos branch; repeatable.",
    )
    parser.add_argument(
        "--catalog-token",
        action="append",
        default=[],
        metavar="BRANCH:TOKEN",
        help="Bind an Iceberg bearer token to a Chronos branch; repeatable.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    server = NativeChronosLakeServer(
        args.database,
        args.upstream_endpoint,
        args.upstream_access_key,
        args.upstream_secret_key,
        args.host,
        args.port,
        args.region,
        args.access_key,
        args.secret_key,
        args.warehouse,
        args.workers,
        args.gc_interval_seconds,
        args.gc_grace_seconds,
    )
    for bucket in args.create_bucket:
        server.create_bucket(bucket)
    for bucket in args.bootstrap_bucket:
        imported = server.bootstrap("main", bucket)
        print(json.dumps({"bootstrap_bucket": bucket, "imported": imported}), flush=True)
    for value in args.branch_credential:
        try:
            branch, access_key, secret_key = value.split(":", 2)
        except ValueError as error:
            raise SystemExit(
                "--branch-credential must be BRANCH:ACCESS_KEY:SECRET_KEY"
            ) from error
        server.bind_credentials(access_key, secret_key, branch)
    for value in args.catalog_token:
        try:
            branch, token = value.split(":", 1)
        except ValueError as error:
            raise SystemExit("--catalog-token must be BRANCH:TOKEN") from error
        server.bind_catalog_token(token, branch)

    stopped = threading.Event()

    def stop(_signum: int, _frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    server.start()
    print(
        json.dumps(
            {
                "status": "ready",
                "s3_endpoint": f"http://{args.host}:{server.port}",
                "iceberg_endpoint": f"http://{args.host}:{server.port}/iceberg",
                "warehouse": args.warehouse,
            }
        ),
        flush=True,
    )
    try:
        stopped.wait()
    finally:
        server.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
