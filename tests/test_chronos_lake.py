from __future__ import annotations

import io
import os
import tempfile
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from chronos_core._native_interval import NativeChronosLakeServer


def _upstream_endpoint() -> str:
    endpoint = os.environ.get("CHRONOS_TEST_S3_ENDPOINT", "")
    if not endpoint:
        pytest.skip("set CHRONOS_TEST_S3_ENDPOINT to run Chronos Lake tests")
    try:
        urllib.request.urlopen(endpoint + "/minio/health/live", timeout=2)
    except Exception as error:
        pytest.skip(f"S3 test service is unavailable: {error}")
    return endpoint


def _client(endpoint: str, access_key: str, secret_key: str):
    boto3 = pytest.importorskip("boto3")
    config_module = pytest.importorskip("botocore.config")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=config_module.Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1},
        ),
    )


def _server(bucket: str) -> NativeChronosLakeServer:
    database = Path(tempfile.mkdtemp(prefix="chronos-lake-test-")) / "metadata.sqlite"
    server = NativeChronosLakeServer(
        str(database),
        _upstream_endpoint(),
        warehouse_location=f"s3://{bucket}/iceberg",
        worker_threads=32,
    )
    server.create_bucket(bucket)
    server.start()
    return server


def _bucket(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest.mark.chronos_lake
def test_s3_protocol_multipart_copy_delete_and_parallel_requests():
    bucket = _bucket("chronos-s3")
    server = _server(bucket)
    try:
        endpoint = f"http://127.0.0.1:{server.port}"
        client = _client(endpoint, "minioadmin", "minioadmin")
        client.put_object(Bucket=bucket, Key="data/a.txt", Body=b"abcdef")
        assert client.get_object(
            Bucket=bucket, Key="data/a.txt", Range="bytes=2-4"
        )["Body"].read() == b"cde"

        boto3 = pytest.importorskip("boto3")
        transfer = pytest.importorskip("boto3.s3.transfer")
        large = (b"chronos-lake-" * (11 * 1024 * 1024 // 13 + 1))[: 11 * 1024 * 1024]
        client.upload_fileobj(
            io.BytesIO(large),
            bucket,
            "data/large.bin",
            Config=transfer.TransferConfig(
                multipart_threshold=1024,
                multipart_chunksize=5 * 1024 * 1024,
                max_concurrency=3,
            ),
        )
        assert client.get_object(Bucket=bucket, Key="data/large.bin")["Body"].read() == large

        client.copy_object(
            Bucket=bucket,
            Key="data/copy.bin",
            CopySource={"Bucket": bucket, "Key": "data/large.bin"},
        )
        assert client.head_object(Bucket=bucket, Key="data/copy.bin")["ContentLength"] == len(large)

        def put(index: int) -> int:
            thread_client = _client(endpoint, "minioadmin", "minioadmin")
            thread_client.put_object(
                Bucket=bucket,
                Key=f"parallel/{index:03d}",
                Body=str(index).encode(),
            )
            return index

        with ThreadPoolExecutor(max_workers=8) as executor:
            assert list(executor.map(put, range(32))) == list(range(32))
        listed = client.list_objects_v2(Bucket=bucket, Prefix="parallel/")
        assert listed["KeyCount"] == 32

        deleted = client.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [
                    {"Key": "data/a.txt"},
                    {"Key": "data/large.bin"},
                    {"Key": "data/copy.bin"},
                ]
            },
        )
        assert len(deleted["Deleted"]) == 3
    finally:
        server.stop()


@pytest.mark.chronos_lake
def test_s3_branch_credentials_isolate_overwrites_and_deletes():
    bucket = _bucket("chronos-branch")
    server = _server(bucket)
    try:
        endpoint = f"http://127.0.0.1:{server.port}"
        main = _client(endpoint, "minioadmin", "minioadmin")
        main.put_object(Bucket=bucket, Key="state/value", Body=b"main")
        server.create_branch("trial", "main")
        server.bind_credentials("trial-key", "trial-secret", "trial")
        trial = _client(endpoint, "trial-key", "trial-secret")

        assert trial.get_object(Bucket=bucket, Key="state/value")["Body"].read() == b"main"
        trial.put_object(Bucket=bucket, Key="state/value", Body=b"trial")
        trial.put_object(Bucket=bucket, Key="state/new", Body=b"new")
        assert main.get_object(Bucket=bucket, Key="state/value")["Body"].read() == b"main"
        assert trial.get_object(Bucket=bucket, Key="state/value")["Body"].read() == b"trial"
        assert main.list_objects_v2(Bucket=bucket, Prefix="state/")["KeyCount"] == 1
        assert trial.list_objects_v2(Bucket=bucket, Prefix="state/")["KeyCount"] == 2

        trial.delete_object(Bucket=bucket, Key="state/value")
        assert main.get_object(Bucket=bucket, Key="state/value")["Body"].read() == b"main"
        with pytest.raises(Exception):
            trial.get_object(Bucket=bucket, Key="state/value")
    finally:
        server.stop()


@pytest.mark.chronos_lake
def test_bootstrap_adopts_existing_bucket_objects():
    bucket = _bucket("chronos-bootstrap")
    upstream = _upstream_endpoint()
    direct = _client(upstream, "minioadmin", "minioadmin")
    direct.create_bucket(Bucket=bucket)
    direct.put_object(Bucket=bucket, Key="existing/data.parquet", Body=b"legacy")

    database = Path(tempfile.mkdtemp(prefix="chronos-lake-bootstrap-")) / "metadata.sqlite"
    server = NativeChronosLakeServer(
        str(database),
        upstream,
        warehouse_location=f"s3://{bucket}/iceberg",
    )
    assert server.bootstrap("main", bucket) == 1
    server.start()
    try:
        client = _client(
            f"http://127.0.0.1:{server.port}", "minioadmin", "minioadmin"
        )
        assert client.get_object(
            Bucket=bucket, Key="existing/data.parquet"
        )["Body"].read() == b"legacy"
    finally:
        server.stop()


@pytest.mark.chronos_lake
def test_gc_preserves_shared_blobs_until_the_last_branch_is_deleted():
    bucket = _bucket("chronos-gc")
    server = _server(bucket)
    try:
        old = server.put_object("main", bucket, "state/value", "old")
        server.create_branch("reader", "main")
        current = server.put_object("main", bucket, "state/value", "current")
        assert server.collect_garbage(0) == 0

        upstream = _client(_upstream_endpoint(), "minioadmin", "minioadmin")
        assert upstream.head_object(
            Bucket=bucket, Key=old.physical_key
        )["ResponseMetadata"]["HTTPStatusCode"] == 200
        server.delete_branch("reader")
        assert server.collect_garbage(0) == 1
        with pytest.raises(Exception):
            upstream.head_object(Bucket=bucket, Key=old.physical_key)
        assert upstream.head_object(
            Bucket=bucket, Key=current.physical_key
        )["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert server.get_object("main", bucket, "state/value") == "current"
    finally:
        server.stop()


@pytest.mark.chronos_lake
def test_duckdb_tpch_iceberg_end_to_end():
    duckdb = pytest.importorskip("duckdb")
    bucket = _bucket("chronos-tpch")
    server = _server(bucket)
    connection = duckdb.connect()
    try:
        try:
            connection.execute("LOAD iceberg")
            connection.execute("LOAD tpch")
        except Exception as error:
            pytest.skip(f"DuckDB Iceberg/TPC-H extensions are unavailable: {error}")
        connection.execute("CALL dbgen(sf=1)")
        query_numbers = tuple(range(1, 23))
        queries = {
            number: connection.execute(
                "SELECT query FROM tpch_queries() WHERE query_nr = ?", [number]
            ).fetchone()[0]
            for number in query_numbers
        }
        expected = {
            number: connection.execute(query).fetchall()
            for number, query in queries.items()
        }

        connection.execute(
            f"""
            CREATE SECRET chronos_s3 (
                TYPE S3,
                KEY_ID 'minioadmin',
                SECRET 'minioadmin',
                ENDPOINT '127.0.0.1:{server.port}',
                URL_STYLE 'path',
                USE_SSL false,
                REGION 'us-east-1'
            )
            """
        )
        connection.execute(
            f"""
            ATTACH '' AS lake (
                TYPE ICEBERG,
                ENDPOINT 'http://127.0.0.1:{server.port}/iceberg',
                AUTHORIZATION_TYPE 'none',
                ACCESS_DELEGATION_MODE 'none'
            )
            """
        )
        connection.execute("CREATE SCHEMA lake.tpch")
        tables = (
            "region",
            "nation",
            "supplier",
            "customer",
            "part",
            "partsupp",
            "orders",
            "lineitem",
        )
        for table in tables:
            connection.execute(
                f"CREATE TABLE lake.tpch.{table} AS SELECT * FROM main.{table}"
            )
        for table in tables:
            connection.execute(f"DROP TABLE main.{table}")
        connection.execute("SET search_path = 'lake.tpch'")

        for number, query in queries.items():
            try:
                actual = connection.execute(query).fetchall()
            except Exception as error:
                pytest.fail(f"TPC-H Q{number} failed against Chronos Lake: {error}")
            assert actual == expected[number], f"TPC-H Q{number} result mismatch"
        assert len(server.list_object_keys("main", bucket, "")) >= 40

        server.create_branch("trial", "main")
        server.bind_credentials("trial-key", "trial-secret", "trial")
        server.bind_catalog_token("trial-token", "trial")
        trial = duckdb.connect()
        try:
            trial.execute("LOAD iceberg")
            trial.execute(
                f"""
                CREATE SECRET trial_s3 (
                    TYPE S3,
                    KEY_ID 'trial-key',
                    SECRET 'trial-secret',
                    ENDPOINT '127.0.0.1:{server.port}',
                    URL_STYLE 'path',
                    USE_SSL false,
                    REGION 'us-east-1'
                )
                """
            )
            trial.execute(
                f"""
                ATTACH '' AS trial_lake (
                    TYPE ICEBERG,
                    ENDPOINT 'http://127.0.0.1:{server.port}/iceberg',
                    AUTHORIZATION_TYPE 'oauth2',
                    TOKEN 'trial-token',
                    ACCESS_DELEGATION_MODE 'none'
                )
                """
            )
            assert trial.execute(
                "SELECT count(*) FROM trial_lake.tpch.region"
            ).fetchone() == (5,)
            trial.execute(
                """
                INSERT INTO trial_lake.tpch.region
                VALUES (99, 'TRIAL', 'branch-only row')
                """
            )
            trial.execute(
                "ALTER TABLE trial_lake.tpch.region ADD COLUMN branch_note VARCHAR"
            )
            trial.execute(
                """
                UPDATE trial_lake.tpch.region
                SET branch_note = 'isolated'
                WHERE r_regionkey = 99
                """
            )
            assert trial.execute(
                "SELECT count(*) FROM trial_lake.tpch.region"
            ).fetchone() == (6,)
            assert trial.execute(
                """
                SELECT branch_note
                FROM trial_lake.tpch.region
                WHERE r_regionkey = 99
                """
            ).fetchone() == ("isolated",)
            assert connection.execute(
                "SELECT count(*) FROM lake.tpch.region"
            ).fetchone() == (5,)
        finally:
            trial.close()
    finally:
        connection.close()
        server.stop()


@pytest.mark.chronos_lake
def test_duckdb_concurrent_iceberg_commits_retry_without_lost_updates():
    duckdb = pytest.importorskip("duckdb")
    bucket = _bucket("chronos-cas")
    server = _server(bucket)

    def attach(connection, alias: str) -> None:
        connection.execute("LOAD iceberg")
        connection.execute(
            f"""
            CREATE SECRET s3_{alias} (
                TYPE S3,
                KEY_ID 'minioadmin',
                SECRET 'minioadmin',
                ENDPOINT '127.0.0.1:{server.port}',
                URL_STYLE 'path',
                USE_SSL false,
                REGION 'us-east-1'
            )
            """
        )
        connection.execute(
            f"""
            ATTACH '' AS {alias} (
                TYPE ICEBERG,
                ENDPOINT 'http://127.0.0.1:{server.port}/iceberg',
                AUTHORIZATION_TYPE 'none',
                ACCESS_DELEGATION_MODE 'none'
            )
            """
        )

    setup = duckdb.connect()
    try:
        attach(setup, "setup_lake")
        setup.execute("CREATE SCHEMA setup_lake.default")
        setup.execute("CREATE TABLE setup_lake.default.events(id BIGINT)")
        setup.execute("INSERT INTO setup_lake.default.events VALUES (0)")
    finally:
        setup.close()

    def insert_with_retry(identifier: int) -> int:
        for attempt in range(10):
            connection = duckdb.connect()
            try:
                alias = f"lake_{identifier}_{attempt}"
                attach(connection, alias)
                connection.execute(
                    f"INSERT INTO {alias}.default.events VALUES (?)", [identifier]
                )
                return identifier
            except Exception:
                if attempt == 9:
                    raise
                time.sleep(0.01 * (attempt + 1) * identifier)
            finally:
                connection.close()
        raise AssertionError("unreachable")

    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            assert sorted(executor.map(insert_with_retry, range(1, 5))) == [1, 2, 3, 4]
        verify = duckdb.connect()
        try:
            attach(verify, "verify_lake")
            assert verify.execute(
                "SELECT id FROM verify_lake.default.events ORDER BY id"
            ).fetchall() == [(0,), (1,), (2,), (3,), (4,)]
        finally:
            verify.close()
    finally:
        server.stop()
