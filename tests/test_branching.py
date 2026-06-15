from __future__ import annotations

import atexit
import glob
import math
import os
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from chronos_core.branching import (
    BranchAlreadyExistsError,
    BranchNotFoundError,
    BranchingError,
    DuplicateKeyError,
    MergePolicy,
    MergeResolution,
    MergeValidationResult,
    TableNotRegisteredError,
    ChronosBranchContext,
)
from chronos_core.branching.sql_adapters import connect_sql_database
from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter


BRANCH_BACKENDS = ("copy", "interval", "log", "orpheus", "litetree")
POSTGRES_BRANCH_BACKENDS = tuple(
    backend for backend in BRANCH_BACKENDS if backend != "litetree"
)
SQL_BACKENDS = ("sqlite", "postgres")
TEST_POSTGRES_CONTAINER = os.environ.get(
    "CHRONOS_BRANCH_TEST_POSTGRES_NAME", "chronos-branch-postgres-tests"
)
TEST_POSTGRES_PORT = os.environ.get("CHRONOS_BRANCH_TEST_POSTGRES_PORT", "55434")
TEST_POSTGRES_DB = os.environ.get("CHRONOS_BRANCH_TEST_POSTGRES_DB", "chronos_branch_test")
TEST_POSTGRES_PASSWORD = os.environ.get("CHRONOS_BRANCH_TEST_POSTGRES_PASSWORD", "postgres")
TEST_POSTGRES_IMAGE = os.environ.get("CHRONOS_BRANCH_TEST_POSTGRES_IMAGE", "postgres:16-alpine")
_POSTGRES_DSN: str | None = os.environ.get("CHRONOS_BRANCH_POSTGRES_DSN") or os.environ.get(
    "CHRONOS_POSTGRES_DSN"
)
_POSTGRES_STARTED_BY_TESTS = False
_LITETREE_TEST_PATHS: list[str] = []


def _cleanup_litetree_files() -> None:
    for path in _LITETREE_TEST_PATHS:
        for candidate in glob.glob(f"{path}*"):
            try:
                os.remove(candidate)
            except OSError:
                pass


atexit.register(_cleanup_litetree_files)


def test_postgres_param_translation_ignores_sql_literals() -> None:
    sql = """
    SELECT ':not_a_param', "also:not_a_param", $$:still_not_a_param$$
    FROM okg.graph_nodes
    WHERE node_id = :node_id
      AND kind = 'review:alpha'
      AND attrs ? ':literal_key'
      AND payload::jsonb ? :json_key
    """

    translated = PostgresDatabaseAdapter._translate_named(sql)

    assert "%(node_id)s" in translated
    assert "%(json_key)s" in translated
    assert "review:alpha" in translated
    assert "':not_a_param'" in translated
    assert '"also:not_a_param"' in translated
    assert "$$:still_not_a_param$$" in translated
    assert "%(not_a_param)s" not in translated
    assert "%(alpha)s" not in translated
    assert "%(literal_key)s" not in translated
    assert "payload::jsonb" in translated


def test_postgres_percent_escaping_preserves_placeholders() -> None:
    sql = "SELECT 'ad-%', score % 10 FROM docs WHERE id = %(id)s AND body LIKE %s"

    escaped = PostgresDatabaseAdapter._escape_pyformat_percents(sql)

    assert "'ad-%%'" in escaped
    assert "score %% 10" in escaped
    assert "%(id)s" in escaped
    assert "LIKE %s" in escaped


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_register_table_adds_new_source_columns(sql_backend: str) -> None:
    if sql_backend == "postgres":
        _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(_database_url(sql_backend), backend="interval")
    try:
        ctx.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, title TEXT)")
        ctx.db.commit()
        ctx.register_table("docs", ["id"])
        ctx.db.execute("ALTER TABLE docs ADD COLUMN source TEXT")
        ctx.db.commit()

        ctx.register_table("docs", ["id"])
        session = ctx.checkout("main")
        session.upsert_rows("docs", [{
            "id": "doc:1",
            "title": "one",
            "source": "fixture",
        }])

        rows = session.query(
            "SELECT source FROM docs WHERE id = :id",
            {"id": "doc:1"},
        )
        assert rows == [{"source": "fixture"}]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_batched_upsert_rows_bulk_insert_and_replace(sql_backend: str) -> None:
    if sql_backend == "postgres":
        _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(_database_url(sql_backend), backend="interval")
    try:
        ctx.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, title TEXT, revision INTEGER)")
        ctx.db.commit()
        ctx.register_table("docs", ["id"])
        session = ctx.checkout("main")

        session.upsert_rows(
            "docs",
            [
                {"id": f"doc:{idx}", "title": f"v1:{idx}", "revision": 1}
                for idx in range(1200)
            ]
            + [
                {"id": "doc:0", "title": "v1:duplicate-overwritten", "revision": 11}
            ],
        )
        assert session.query("SELECT count(*) AS c FROM docs") == [{"c": 1200}]
        assert session.query("SELECT title, revision FROM docs WHERE id = :id", {"id": "doc:0"}) == [
            {"title": "v1:duplicate-overwritten", "revision": 11}
        ]
        assert session.query("SELECT title FROM docs WHERE id = :id", {"id": "doc:1100"}) == [
            {"title": "v1:1100"}
        ]

        session.upsert_rows(
            "docs",
            [
                {"id": f"doc:{idx}", "title": f"v2:{idx}", "revision": 2}
                for idx in range(500)
            ]
            + [
                {"id": f"doc:{idx}", "title": f"v1:{idx}", "revision": 1}
                for idx in range(1200, 1500)
            ],
        )

        assert session.query("SELECT count(*) AS c FROM docs") == [{"c": 1500}]
        assert session.query(
            "SELECT id FROM docs GROUP BY id HAVING count(*) > 1"
        ) == []
        assert session.query("SELECT title, revision FROM docs WHERE id = :id", {"id": "doc:0"}) == [
            {"title": "v2:0", "revision": 2}
        ]
        assert session.query("SELECT title, revision FROM docs WHERE id = :id", {"id": "doc:900"}) == [
            {"title": "v1:900", "revision": 1}
        ]
        assert session.query("SELECT title, revision FROM docs WHERE id = :id", {"id": "doc:1400"}) == [
            {"title": "v1:1400", "revision": 1}
        ]
        assert ctx.db.execute(
            "SELECT count(*) AS c FROM _chronos_b_interval_docs"
        ).fetchone()["c"] == 1500
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_batched_delete_keys_records_absent_key_tombstones(sql_backend: str) -> None:
    if sql_backend == "postgres":
        _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(_database_url(sql_backend), backend="interval")
    try:
        ctx.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, title TEXT)")
        ctx.db.commit()
        ctx.register_table("docs", ["id"])
        session = ctx.checkout("main")

        session.delete_keys("docs", [{"id": f"missing:{idx}"} for idx in range(1200)])
        session.upsert_rows(
            "docs",
            [
                {"id": "missing:0", "title": "recreated"},
                {"id": "present", "title": "present"},
            ],
        )

        assert session.query("SELECT count(*) AS c FROM docs") == [{"c": 2}]
        assert session.query("SELECT title FROM docs WHERE id = :id", {"id": "missing:0"}) == [
            {"title": "recreated"}
        ]
    finally:
        ctx.close()


@pytest.mark.parametrize("sql_backend", SQL_BACKENDS)
def test_interval_batched_upsert_rows_uses_branch_local_schema(sql_backend: str) -> None:
    if sql_backend == "postgres":
        _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(
        _database_url(sql_backend),
        backend="interval",
        enable_schema_branching=True,
    )
    try:
        ctx.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, title TEXT)")
        ctx.db.commit()
        ctx.register_table("docs", ["id"])
        ctx.create_branch("exp", from_branch="main")
        session = ctx.checkout("exp")
        session.execute("ALTER TABLE docs ADD COLUMN score INTEGER")

        session.upsert_rows(
            "docs",
            [
                {"id": f"doc:{idx}", "title": f"title:{idx}", "score": idx}
                for idx in range(1005)
            ],
        )
        assert session.query("SELECT count(*) AS c FROM docs") == [{"c": 1005}]
        assert session.query("SELECT score FROM docs WHERE id = :id", {"id": "doc:1001"}) == [
            {"score": 1001}
        ]
        with pytest.raises(Exception):
            ctx.checkout("main").query("SELECT score FROM docs")
    finally:
        ctx.close()


def test_postgres_interval_batched_upsert_rows_large_scale_performance() -> None:
    _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(_postgres_dsn(), backend="interval")
    try:
        ctx.db.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, title TEXT, revision INTEGER)")
        ctx.db.commit()
        ctx.register_table("docs", ["id"])
        session = ctx.checkout("main")

        initial_rows = [
            {"id": f"doc:{idx}", "title": f"v1:{idx}", "revision": 1}
            for idx in range(20_000)
        ]
        start = time.perf_counter()
        session.upsert_rows("docs", initial_rows)
        initial_elapsed = time.perf_counter() - start

        assert session.query("SELECT count(*) AS c FROM docs") == [{"c": 20_000}]
        assert 20_000 / initial_elapsed > 2_000

        mixed_rows = [
            {"id": f"doc:{idx}", "title": f"v2:{idx}", "revision": 2}
            for idx in range(5_000)
        ] + [
            {"id": f"doc:{idx}", "title": f"v1:{idx}", "revision": 1}
            for idx in range(20_000, 25_000)
        ]
        start = time.perf_counter()
        session.upsert_rows("docs", mixed_rows)
        mixed_elapsed = time.perf_counter() - start

        assert session.query("SELECT count(*) AS c FROM docs") == [{"c": 25_000}]
        assert session.query(
            "SELECT id FROM docs GROUP BY id HAVING count(*) > 1 LIMIT 1"
        ) == []
        assert session.query("SELECT title, revision FROM docs WHERE id = :id", {"id": "doc:0"}) == [
            {"title": "v2:0", "revision": 2}
        ]
        assert 10_000 / mixed_elapsed > 1_000
    finally:
        ctx.close()


def _database_url(sql_backend: str, branch_backend: str | None = None) -> str:
    if sql_backend == "sqlite":
        if branch_backend == "litetree":
            path = tempfile.mktemp(prefix="chronos-litetree-test-", suffix=".db")
            _LITETREE_TEST_PATHS.append(path)
            return f"file:{path}?branches=on"
        return "sqlite:///:memory:"
    return _postgres_dsn()


def _run_docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _docker_container_names(all_containers: bool) -> set[str]:
    args = ["ps", "--format", "{{.Names}}"]
    if all_containers:
        args.insert(1, "-a")
    result = _run_docker(*args)
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def _wait_for_postgres() -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        result = _run_docker(
            "exec",
            TEST_POSTGRES_CONTAINER,
            "pg_isready",
            "-U",
            "postgres",
            "-d",
            TEST_POSTGRES_DB,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError(f"PostgreSQL test container did not become ready: {TEST_POSTGRES_CONTAINER}")


def _stop_managed_postgres() -> None:
    if _POSTGRES_STARTED_BY_TESTS:
        _run_docker("stop", TEST_POSTGRES_CONTAINER, check=False)


def _postgres_dsn() -> str:
    global _POSTGRES_DSN, _POSTGRES_STARTED_BY_TESTS
    if _POSTGRES_DSN is not None:
        return _POSTGRES_DSN

    running = _docker_container_names(all_containers=False)
    if TEST_POSTGRES_CONTAINER in running:
        _wait_for_postgres()
    else:
        all_names = _docker_container_names(all_containers=True)
        if TEST_POSTGRES_CONTAINER in all_names:
            _run_docker("rm", TEST_POSTGRES_CONTAINER)
        _run_docker(
            "run",
            "--rm",
            "-d",
            "--name",
            TEST_POSTGRES_CONTAINER,
            "-e",
            f"POSTGRES_PASSWORD={TEST_POSTGRES_PASSWORD}",
            "-e",
            f"POSTGRES_DB={TEST_POSTGRES_DB}",
            "-p",
            f"{TEST_POSTGRES_PORT}:5432",
            TEST_POSTGRES_IMAGE,
        )
        _POSTGRES_STARTED_BY_TESTS = True
        atexit.register(_stop_managed_postgres)
        _wait_for_postgres()

    _POSTGRES_DSN = (
        f"postgresql://postgres:{TEST_POSTGRES_PASSWORD}"
        f"@localhost:{TEST_POSTGRES_PORT}/{TEST_POSTGRES_DB}"
    )
    return _POSTGRES_DSN


def _reset_postgres_schema() -> None:
    from chronos_core.branching._interval_backend import (
        _wait_for_all_interval_gc_jobs,
        _wait_for_all_async_schema_index_jobs,
    )

    _wait_for_all_async_schema_index_jobs()
    _wait_for_all_interval_gc_jobs()
    db = connect_sql_database(_postgres_dsn())
    try:
        db.execute("DROP SCHEMA IF EXISTS public CASCADE")
        db.execute("CREATE SCHEMA public")
        db.commit()
    finally:
        db.close()


def _make_context(sql_backend: str, branch_backend: str) -> ChronosBranchContext:
    if branch_backend == "litetree" and sql_backend != "sqlite":
        pytest.skip("LiteTree backend only runs on SQLite")
    if branch_backend == "orpheus" and sql_backend != "postgres":
        pytest.skip("Orpheus backend requires PostgreSQL array-backed rlist")
    if sql_backend == "postgres":
        _reset_postgres_schema()
    try:
        context = ChronosBranchContext.connect(
            _database_url(sql_backend, branch_backend), backend=branch_backend
        )
    except BranchingError as exc:
        if branch_backend == "litetree" and "LiteTree backend requires" in str(exc):
            pytest.skip(str(exc))
        raise
    context._test_sql_backend = sql_backend  # type: ignore[attr-defined]
    db = context.db
    db.execute(
        "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
    )
    db.execute(
        "CREATE TABLE orders (order_id TEXT PRIMARY KEY, sku TEXT, quantity INTEGER)"
    )
    db.execute(
        "CREATE TABLE nodes (node_id TEXT PRIMARY KEY, label TEXT, score INTEGER)"
    )
    db.execute(
        "CREATE TABLE edges (edge_id TEXT PRIMARY KEY, src_id TEXT, dst_id TEXT)"
    )
    db.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    db.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [("o1", "abc", 2), ("o2", "def", 1)],
    )
    db.executemany(
        "INSERT INTO nodes VALUES (?, ?, ?)",
        [("n1", "one", 1), ("n2", "two", 2)],
    )
    db.execute("INSERT INTO edges VALUES (?, ?, ?)", ("e1", "n1", "n2"))
    db.commit()
    context.register_table("products", ["sku"])
    context.register_table("orders", ["order_id"])
    context.register_table("nodes", ["node_id"])
    context.register_table("edges", ["edge_id"])
    return context


@pytest.fixture(params=SQL_BACKENDS)
def sql_backend(request) -> str:
    return request.param


@pytest.fixture(params=BRANCH_BACKENDS)
def branch_backend(request) -> str:
    return request.param


@pytest.fixture
def ctx(sql_backend: str, branch_backend: str) -> ChronosBranchContext:
    context = _make_context(sql_backend, branch_backend)
    yield context
    context.close()


def _product(session, sku: str) -> dict:
    rows = session.query(
        "SELECT sku, name, price FROM products WHERE sku = :sku", {"sku": sku}
    )
    assert len(rows) == 1
    return rows[0]


def _physical_change_count(ctx: ChronosBranchContext, table: str) -> int:
    if ctx.backend_name == "interval":
        row = ctx.db.execute(
            f"SELECT COUNT(*) AS count FROM _chronos_b_interval_{table}"
        ).fetchone()
        return int(row["count"])
    if ctx.backend_name == "log":
        row = ctx.db.execute(
            f"SELECT COUNT(*) AS count FROM _chronos_b_log_{table}"
        ).fetchone()
        return int(row["count"])
    if ctx.backend_name == "orpheus":
        row = ctx.db.execute(
            f"SELECT COUNT(*) AS count FROM _chronos_b_orpheus_{table}_datatable"
        ).fetchone()
        return int(row["count"])
    if ctx.backend_name == "litetree":
        row = ctx.db.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
        return int(row["count"])
    backend = ctx._backend  # type: ignore[attr-defined]
    return sum(
        int(
            ctx.db.execute(
                "SELECT COUNT(*) AS count "
                f"FROM {backend._branch_table(branch.branch_id, table)}"
            ).fetchone()["count"]
        )
        for branch in ctx.list_branches()
    )


def _sql_backend(ctx: ChronosBranchContext) -> str:
    return ctx._test_sql_backend  # type: ignore[attr-defined]


def _index_exists(
    ctx: ChronosBranchContext, *, name: str | None = None, pattern: str | None = None
) -> bool:
    if _sql_backend(ctx) == "postgres":
        if name is not None:
            row = ctx.db.execute(
                """
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname = :name
                """,
                {"name": name},
            ).fetchone()
        else:
            row = ctx.db.execute(
                """
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE :pattern
                """,
                {"pattern": pattern},
            ).fetchone()
        return row is not None
    if name is not None:
        row = ctx.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
    else:
        row = ctx.db.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'index'
              AND name LIKE ?
            """,
            (pattern,),
        ).fetchone()
    return row is not None


def _table_exists(ctx: ChronosBranchContext, table: str) -> bool:
    if _sql_backend(ctx) == "postgres":
        row = ctx.db.execute(
            """
            SELECT 1
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename = :table
            """,
            {"table": table},
        ).fetchone()
    else:
        row = ctx.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
    return row is not None


def _make_products_only_context(
    sql_backend: str, branch_backend: str
) -> ChronosBranchContext:
    if branch_backend == "litetree" and sql_backend != "sqlite":
        pytest.skip("LiteTree backend only runs on SQLite")
    if branch_backend == "orpheus" and sql_backend != "postgres":
        pytest.skip("Orpheus backend requires PostgreSQL array-backed rlist")
    if sql_backend == "postgres":
        _reset_postgres_schema()
    try:
        ctx = ChronosBranchContext.connect(
            _database_url(sql_backend, branch_backend), backend=branch_backend
        )
    except BranchingError as exc:
        if branch_backend == "litetree" and "LiteTree backend requires" in str(exc):
            pytest.skip(str(exc))
        raise
    ctx._test_sql_backend = sql_backend  # type: ignore[attr-defined]
    ctx.db.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)")
    ctx.db.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    ctx.db.commit()
    ctx.register_table("products", ["sku"])
    return ctx


def _make_orpheus_tracking_context() -> ChronosBranchContext:
    _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(
        _postgres_dsn(),
        backend="orpheus",
        enable_diff_merge_tracking=True,
    )
    ctx._test_sql_backend = "postgres"  # type: ignore[attr-defined]
    ctx.db.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)")
    ctx.db.executemany(
        "INSERT INTO products VALUES (?, ?, ?)",
        [("abc", "Alpha", 10), ("def", "Delta", 20)],
    )
    ctx.db.commit()
    ctx.register_table("products", ["sku"])
    return ctx


def test_create_branch_storage_behavior_for_user_records(ctx: ChronosBranchContext) -> None:
    before = _physical_change_count(ctx, "products")
    ctx.create_branch("exp", from_branch="main")
    after = _physical_change_count(ctx, "products")

    if ctx.backend_name == "copy":
        assert after > before
    else:
        assert before == after
    assert {branch.branch_id for branch in ctx.list_branches()} == {"main", "exp"}


def test_postgres_interval_create_branch_locks_parent_before_split(monkeypatch) -> None:
    ctx = _make_context("postgres", "interval")
    try:
        original_execute = ctx.db.execute
        calls: list[str] = []

        def counted_execute(sql, params=()):
            calls.append(str(sql))
            return original_execute(sql, params)

        monkeypatch.setattr(ctx.db, "execute", counted_execute)
        ctx.create_branch("exp", from_branch="main")

        assert any("FOR UPDATE" in call for call in calls)
        assert any("SET current_segment_id" in call for call in calls)
        assert _product(ctx.checkout("exp"), "abc")["price"] == 10
        assert _product(ctx.checkout("main"), "abc")["price"] == 10
    finally:
        ctx.close()


def test_postgres_interval_create_branch_fast_path_preserves_errors() -> None:
    ctx = _make_context("postgres", "interval")
    try:
        with pytest.raises(BranchNotFoundError):
            ctx.create_branch("orphan", from_branch="missing")

        ctx.create_branch("exp", from_branch="main")
        with pytest.raises(BranchAlreadyExistsError):
            ctx.create_branch("exp", from_branch="main")
    finally:
        ctx.close()


def test_postgres_interval_uses_numeric_32_visibility_columns() -> None:
    ctx = _make_context("postgres", "interval")
    try:
        rows = ctx.db.execute(
            """
            SELECT table_name, column_name, data_type, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN (
                '_chronos_branch_interval_segments',
                '_chronos_b_interval_products'
              )
              AND column_name IN ('live_lo', 'live_hi', 'branch_point')
            ORDER BY table_name, column_name
            """
        ).fetchall()
        assert rows
        for row in rows:
            assert row["data_type"] == "numeric"
            assert int(row["numeric_precision"]) == 32
            assert int(row["numeric_scale"]) == 0
    finally:
        ctx.close()


def test_postgres_orpheus_uses_rlist_schema_types_by_default() -> None:
    ctx = _make_context("postgres", "orpheus")
    try:
        rows = ctx.db.execute(
            """
            SELECT table_name, column_name, data_type, udt_name, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND (
                table_name IN (
                  '_chronos_branch_orpheus_versiontable',
                  '_chronos_b_orpheus_products_indextable'
                )
                OR table_name = '_chronos_b_orpheus_products_datatable'
              )
              AND column_name IN ('rid', 'vid', 'parent', 'children', 'rlist')
            """
        ).fetchall()
        by_column = {
            (row["table_name"], row["column_name"]): row
            for row in rows
        }

        datatable_rid = by_column[("_chronos_b_orpheus_products_datatable", "rid")]
        assert datatable_rid["data_type"] == "integer"
        assert "nextval" in datatable_rid["column_default"]

        version_vid = by_column[("_chronos_branch_orpheus_versiontable", "vid")]
        assert version_vid["data_type"] == "integer"

        for column in ("parent", "children"):
            version_array = by_column[
                ("_chronos_branch_orpheus_versiontable", column)
            ]
            assert version_array["data_type"] == "ARRAY"
            assert version_array["udt_name"] == "_int4"

        index_vid = by_column[("_chronos_b_orpheus_products_indextable", "vid")]
        assert index_vid["data_type"] == "integer"

        rlist = by_column[("_chronos_b_orpheus_products_indextable", "rlist")]
        assert rlist["data_type"] == "ARRAY"
        assert rlist["udt_name"] == "_int4"
    finally:
        ctx.close()


def test_postgres_orpheus_materializes_version_on_checkpoint() -> None:
    ctx = _make_context("postgres", "orpheus")
    try:
        ctx.create_branch("work", from_branch="main")
        session = ctx.checkout("work")

        with session.transaction():
            session.execute(
                "UPDATE products SET price = :price WHERE sku = :sku",
                {"price": 11, "sku": "abc"},
            )
            session.execute(
                "UPDATE products SET price = :price WHERE sku = :sku",
                {"price": 22, "sku": "def"},
            )
            session.execute(
                """
                INSERT INTO products (sku, name, price)
                VALUES (:sku, :name, :price)
                """,
                {"sku": "ghi", "name": "Gamma", "price": 33},
            )

        version_count = ctx.db.execute(
            "SELECT COUNT(*) AS count, MAX(vid) AS max_vid "
            "FROM _chronos_branch_orpheus_versiontable"
        ).fetchone()
        work = ctx.db.execute(
            "SELECT current_vid FROM _chronos_branch_orpheus_branches "
            "WHERE branch_id = 'work'"
        ).fetchone()

        assert dict(version_count) == {"count": 1, "max_vid": 1}
        assert int(work["current_vid"]) == 1
        assert session.query("SELECT sku, price FROM products ORDER BY sku") == [
            {"sku": "abc", "price": 11},
            {"sku": "def", "price": 22},
            {"sku": "ghi", "price": 33},
        ]

        ctx.create_checkpoint("work-snap", branch="work")

        version_count = ctx.db.execute(
            "SELECT COUNT(*) AS count, MAX(vid) AS max_vid "
            "FROM _chronos_branch_orpheus_versiontable"
        ).fetchone()
        work = ctx.db.execute(
            "SELECT current_vid FROM _chronos_branch_orpheus_branches "
            "WHERE branch_id = 'work'"
        ).fetchone()

        assert dict(version_count) == {"count": 2, "max_vid": 2}
        assert int(work["current_vid"]) == 2
    finally:
        ctx.close()


def test_postgres_orpheus_tracking_records_durable_version_deltas() -> None:
    ctx = _make_orpheus_tracking_context()
    try:
        ctx.create_branch("work", from_branch="main")
        work = ctx.checkout("work")
        work.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 11, "sku": "abc"},
        )
        work.execute("DELETE FROM products WHERE sku = :sku", {"sku": "def"})
        work.execute(
            "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
            {"sku": "ghi", "name": "Gamma", "price": 33},
        )

        ctx.create_checkpoint("work-snap", branch="work")

        rows = ctx.db.execute(
            """
            SELECT key_text, op, before_rid IS NOT NULL AS has_before,
                   after_rid IS NOT NULL AS has_after
            FROM _chronos_branch_orpheus_version_delta
            WHERE table_name = 'products'
            ORDER BY key_text
            """
        ).fetchall()
        assert [dict(row) for row in rows] == [
            {
                "key_text": '["abc"]',
                "op": "modified",
                "has_before": True,
                "has_after": True,
            },
            {
                "key_text": '["def"]',
                "op": "deleted",
                "has_before": True,
                "has_after": False,
            },
            {
                "key_text": '["ghi"]',
                "op": "added",
                "has_before": False,
                "has_after": True,
            },
        ]

        changes = ctx.diff_rows("main", "work", "products")
        assert [(change.key, change.change) for change in changes] == [
            ({"sku": "abc"}, "modified"),
            ({"sku": "def"}, "deleted"),
            ({"sku": "ghi"}, "added"),
        ]
    finally:
        ctx.close()


def test_postgres_orpheus_tracking_merge_applies_source_only_changes() -> None:
    ctx = _make_orpheus_tracking_context()
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "def", "price": 22},
        )

        preview = ctx.merge_preview(source="agent", target="main")
        assert preview.conflicts == []
        assert [(change.key, change.change, change.after) for change in preview.changes] == [
            ({"sku": "abc"}, "modified", {"sku": "abc", "name": "Alpha", "price": 11})
        ]

        result = ctx.merge_apply(source="agent", target="main")
        assert result.applied == 1
        assert _product(ctx.checkout("main"), "abc")["price"] == 11
        assert _product(ctx.checkout("main"), "def")["price"] == 22

        merge = ctx.db.execute(
            """
            SELECT current_vid
            FROM _chronos_branch_orpheus_branches
            WHERE branch_id = 'main'
            """
        ).fetchone()
        parents = ctx.db.execute(
            """
            SELECT parent
            FROM _chronos_branch_orpheus_versiontable
            WHERE vid = ?
            """,
            (int(merge["current_vid"]),),
        ).fetchone()
        assert len(parents["parent"]) == 2

        delta = ctx.db.execute(
            """
            SELECT op, before_rid IS NOT NULL AS has_before,
                   after_rid IS NOT NULL AS has_after
            FROM _chronos_branch_orpheus_version_delta
            WHERE vid = ? AND table_name = 'products' AND key_text = '["abc"]'
            """,
            (int(merge["current_vid"]),),
        ).fetchone()
        assert dict(delta) == {"op": "modified", "has_before": True, "has_after": True}
    finally:
        ctx.close()


def test_postgres_orpheus_tracking_merge_applies_source_delete() -> None:
    ctx = _make_orpheus_tracking_context()
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute("DELETE FROM products WHERE sku = :sku", {"sku": "def"})
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )

        preview = ctx.merge_preview(source="agent", target="main")
        assert preview.conflicts == []
        assert [(change.key, change.change) for change in preview.changes] == [
            ({"sku": "def"}, "deleted")
        ]

        result = ctx.merge_apply(source="agent", target="main")
        assert result.applied == 1
        assert _product(ctx.checkout("main"), "abc")["price"] == 12
        assert ctx.checkout("main").query("SELECT * FROM products WHERE sku = 'def'") == []
    finally:
        ctx.close()


def test_postgres_orpheus_tracking_merge_detects_conflicts_without_preview_mutation() -> None:
    ctx = _make_orpheus_tracking_context()
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )

        preview = ctx.merge_preview(source="agent", target="main")
        assert preview.changes == []
        assert [
            (conflict.key, conflict.change, conflict.before, conflict.after)
            for conflict in preview.conflicts
        ] == [
            (
                {"sku": "abc"},
                "modified",
                {"sku": "abc", "name": "Alpha", "price": 12},
                {"sku": "abc", "name": "Alpha", "price": 11},
            )
        ]
        assert ctx.db.execute(
            "SELECT COUNT(*) AS count FROM _chronos_branch_orpheus_versiontable"
        ).fetchone()["count"] == 1

        with pytest.raises(BranchingError):
            ctx.merge_apply(source="agent", target="main")

        assert ctx.db.execute(
            "SELECT COUNT(*) AS count FROM _chronos_branch_orpheus_versiontable"
        ).fetchone()["count"] == 1
        assert _product(ctx.checkout("main"), "abc")["price"] == 12
    finally:
        ctx.close()


def test_postgres_interval_numeric_space_expands_repeated_root_fanout() -> None:
    ctx = _make_products_only_context("postgres", "interval")
    try:
        width = 20
        for idx in range(width):
            ctx.create_branch(f"trial_{idx}", from_branch="main")

        branches = {branch.branch_id for branch in ctx.list_branches()}
        assert len(branches) == width + 1
        assert "main" in branches

        first = ctx.checkout("trial_0")
        middle = ctx.checkout("trial_10")
        last = ctx.checkout("trial_19")
        last.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 999, "sku": "abc"},
        )

        assert _product(last, "abc")["price"] == 999
        assert _product(first, "abc")["price"] == 10
        assert _product(middle, "abc")["price"] == 10
        assert _product(ctx.checkout("main"), "abc")["price"] == 10

        row = ctx.db.execute(
            """
            SELECT MIN(live_hi - live_lo) AS min_width
            FROM _chronos_branch_interval_segments
            """
        ).fetchone()
        assert int(row["min_width"]) > 0
    finally:
        ctx.close()


def test_postgres_interval_concurrent_wide_branch_creation_serializes_parent() -> None:
    _reset_postgres_schema()
    root = ChronosBranchContext.connect(
        _postgres_dsn(),
        backend="interval",
        interval_continuation_percent=98,
    )
    try:
        root.db.execute(
            "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
        )
        root.db.execute("INSERT INTO products VALUES (?, ?, ?)", ("abc", "Alpha", 10))
        root.db.commit()
        root.register_table("products", ["sku"])
    finally:
        root.close()

    def create_child(index: int) -> None:
        ctx = ChronosBranchContext.connect(
            _postgres_dsn(),
            backend="interval",
            interval_continuation_percent=98,
        )
        try:
            ctx.create_branch(f"trial_{index}", from_branch="main")
        finally:
            ctx.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(create_child, range(30)))

    ctx = ChronosBranchContext.connect(
        _postgres_dsn(),
        backend="interval",
        interval_continuation_percent=98,
    )
    try:
        assert len(ctx.list_branches()) == 31
        ctx.checkout("trial_29").execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 29, "sku": "abc"},
        )
        assert _product(ctx.checkout("trial_29"), "abc")["price"] == 29
        assert _product(ctx.checkout("trial_0"), "abc")["price"] == 10
        assert _product(ctx.checkout("main"), "abc")["price"] == 10
    finally:
        ctx.close()


def test_interval_split_default_uses_adaptive_sqrt_before_percentage() -> None:
    default_ctx = _make_products_only_context("sqlite", "interval")
    try:
        default_backend = default_ctx._backend  # type: ignore[attr-defined]
        assert default_backend.continuation_percent == 5
        assert default_backend.allocation_strategy == "adaptive"
        initial_main = default_backend._current_segment("main")
        default_ctx.create_branch("wide_child", from_branch="main")
        child = default_backend._current_segment("wide_child")
        assert child.live_hi - child.live_lo == math.isqrt(
            initial_main.live_hi - initial_main.live_lo - 1
        )
    finally:
        default_ctx.close()

    wide_ctx = ChronosBranchContext.connect(
        "sqlite:///:memory:",
        backend="interval",
        interval_allocation_strategy="percentage",
        interval_continuation_percent=98,
    )
    try:
        wide_ctx.db.execute(
            "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
        )
        wide_ctx.db.execute("INSERT INTO products VALUES (?, ?, ?)", ("abc", "Alpha", 10))
        wide_ctx.db.commit()
        wide_ctx.register_table("products", ["sku"])
        wide_backend = wide_ctx._backend  # type: ignore[attr-defined]
        assert wide_backend.continuation_percent == 98
        assert wide_backend.allocation_strategy == "percentage"

        for index in range(30):
            wide_ctx.create_branch(f"trial_{index}", from_branch="main")

        last = wide_ctx.checkout("trial_29")
        last.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 29, "sku": "abc"},
        )
        assert _product(last, "abc")["price"] == 29
        assert _product(wide_ctx.checkout("trial_0"), "abc")["price"] == 10
        assert _product(wide_ctx.checkout("main"), "abc")["price"] == 10
    finally:
        wide_ctx.close()


def test_interval_terminal_branch_uses_minimal_width_and_cannot_branch() -> None:
    ctx = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
    try:
        ctx.db.execute(
            "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
        )
        ctx.db.execute("INSERT INTO products VALUES (?, ?, ?)", ("abc", "Alpha", 10))
        ctx.db.commit()
        ctx.register_table("products", ["sku"])
        initial_main = ctx._backend._current_segment("main")  # type: ignore[attr-defined]

        for index in range(10):
            ctx.create_branch(f"txn_{index}", from_branch="main", terminal=True)

        main_segment = ctx._backend._current_segment("main")  # type: ignore[attr-defined]
        assert main_segment.live_lo - initial_main.live_lo == 30
        assert main_segment.live_hi == initial_main.live_hi

        child_rows = ctx.db.execute(
            """
            SELECT b.branch_id, b.branch_kind, s.live_lo, s.live_hi,
                   s.live_hi - s.live_lo AS width
            FROM _chronos_branch_interval_branches AS b
            JOIN _chronos_branch_interval_segments AS s
              ON s.segment_id = b.current_segment_id
            WHERE b.branch_id LIKE 'txn_%'
            ORDER BY b.branch_id
            """
        ).fetchall()
        assert [row["branch_kind"] for row in child_rows] == ["terminal"] * 10
        assert [int(row["width"]) for row in child_rows] == [2] * 10
        assert [int(row["live_lo"]) for row in child_rows] == [
            initial_main.live_lo + 1 + index * 3 for index in range(10)
        ]

        terminal = ctx.checkout("txn_9")
        terminal.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 99, "sku": "abc"},
        )
        assert _product(terminal, "abc")["price"] == 99
        assert _product(ctx.checkout("main"), "abc")["price"] == 10

        with pytest.raises(BranchingError, match="terminal branch is not branchable"):
            ctx.create_branch("bad_child", from_branch="txn_9")
        with pytest.raises(BranchingError, match="terminal branch cannot be checkpointed"):
            ctx.create_checkpoint("bad_checkpoint", branch="txn_9")
    finally:
        ctx.close()


def test_interval_split_can_use_fixed_child_width_for_serial_transactions() -> None:
    ctx = ChronosBranchContext.connect(
        "sqlite:///:memory:",
        backend="interval",
        interval_child_width=2,
    )
    try:
        ctx.db.execute(
            "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
        )
        ctx.db.execute("INSERT INTO products VALUES (?, ?, ?)", ("abc", "Alpha", 10))
        ctx.db.commit()
        ctx.register_table("products", ["sku"])
        initial_main = ctx._backend._current_segment("main")  # type: ignore[attr-defined]

        for index in range(10):
            ctx.create_branch(f"txn_{index}", from_branch="main")

        main_segment = ctx._backend._current_segment("main")  # type: ignore[attr-defined]
        assert main_segment.live_lo - initial_main.live_lo == 30
        assert main_segment.live_hi == initial_main.live_hi

        child_rows = ctx.db.execute(
            """
            SELECT live_lo, live_hi, live_hi - live_lo AS width
            FROM _chronos_branch_interval_segments
            WHERE owner_branch_id LIKE 'txn_%'
              AND segment_kind = 'mutable'
            ORDER BY owner_branch_id
            """
        ).fetchall()
        assert [int(row["width"]) for row in child_rows] == [2] * 10
        assert [int(row["live_lo"]) for row in child_rows] == [
            initial_main.live_lo + 1 + index * 3 for index in range(10)
        ]

        fork_base_rows = ctx.db.execute(
            """
            SELECT live_lo, live_hi, live_hi - live_lo AS width
            FROM _chronos_branch_interval_segments
            WHERE segment_kind = 'fork_base'
            ORDER BY live_lo
            """
        ).fetchall()
        assert [int(row["width"]) for row in fork_base_rows] == [1] * 10
        assert [int(row["live_lo"]) for row in fork_base_rows] == [
            initial_main.live_lo + index * 3 for index in range(10)
        ]

        ctx.checkout("txn_9").execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 99, "sku": "abc"},
        )
        assert _product(ctx.checkout("txn_9"), "abc")["price"] == 99
        assert _product(ctx.checkout("main"), "abc")["price"] == 10
    finally:
        ctx.close()


def test_interval_split_rejects_invalid_continuation_percent() -> None:
    with pytest.raises(ValueError):
        ChronosBranchContext.connect(
            "sqlite:///:memory:",
            backend="interval",
            interval_continuation_percent=0,
        )
    with pytest.raises(ValueError):
        ChronosBranchContext.connect(
            "sqlite:///:memory:",
            backend="interval",
            interval_continuation_percent=100,
        )


def test_interval_split_rejects_invalid_child_width() -> None:
    with pytest.raises(ValueError):
        ChronosBranchContext.connect(
            "sqlite:///:memory:",
            backend="interval",
            interval_child_width=1,
        )


def test_interval_split_rejects_invalid_allocation_strategy() -> None:
    with pytest.raises(ValueError):
        ChronosBranchContext.connect(
            "sqlite:///:memory:",
            backend="interval",
            interval_allocation_strategy="unknown",  # type: ignore[arg-type]
        )


def test_postgres_concurrent_metadata_initialization_and_registration() -> None:
    _reset_postgres_schema()
    db = connect_sql_database(_postgres_dsn())
    try:
        db.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, price INTEGER)")
        db.execute("INSERT INTO products VALUES (?, ?)", ("abc", 10))
        db.commit()
    finally:
        db.close()

    def register_from_new_context(_: int) -> None:
        context = ChronosBranchContext.connect(_postgres_dsn(), backend="interval")
        try:
            context.register_table("products", ["sku"])
        finally:
            context.close()

    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(register_from_new_context, range(3)))

    context = ChronosBranchContext.connect(_postgres_dsn(), backend="interval")
    try:
        row = context.db.execute(
            """
            SELECT COUNT(*) AS count
            FROM _chronos_branch_tables
            WHERE backend = 'interval' AND table_name = 'products'
            """
        ).fetchone()
        assert int(row["count"]) == 1
        assert context.checkout("main").query("SELECT sku, price FROM products") == [
            {"sku": "abc", "price": 10}
        ]
    finally:
        context.close()


def test_postgres_metadata_bootstrap_waits_for_advisory_lock() -> None:
    _reset_postgres_schema()
    holder = connect_sql_database(_postgres_dsn())
    try:
        holder.execute("SELECT pg_advisory_lock(1720812901, 19840717)")

        def connect_context() -> None:
            context = ChronosBranchContext.connect(_postgres_dsn(), backend="interval")
            context.close()

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(connect_context)
            time.sleep(0.25)
            assert not future.done()
            holder.execute("SELECT pg_advisory_unlock(1720812901, 19840717)")
            future.result(timeout=5)
    finally:
        try:
            holder.execute("SELECT pg_advisory_unlock(1720812901, 19840717)")
        except Exception:
            holder.rollback()
        holder.close()


def test_users_can_add_logical_indexes_to_registered_tables(ctx: ChronosBranchContext) -> None:
    index = ctx.create_index("products", ["price", "sku"], name="products_price_sku")

    assert index.name == "products_price_sku"
    assert index.table == "products"
    assert index.columns == ("price", "sku")
    assert ctx.list_indexes("products") == [index]

    if ctx.backend_name == "copy":
        assert _index_exists(ctx, pattern="_chronos_idx_copy_%products_price_sku_%")
    else:
        index_name = (
            "_chronos_idx_interval_products_price_sku"
            if ctx.backend_name == "interval"
            else "_chronos_idx_log_products_price_sku"
            if ctx.backend_name == "log"
            else "products_price_sku"
            if ctx.backend_name == "litetree"
            else "_chronos_idx_orpheus_products_price_sku"
        )
        assert _index_exists(ctx, name=index_name)
    assert ctx.checkout("main").query(
        "SELECT sku FROM products WHERE price >= :price ORDER BY sku",
        {"price": 10},
    ) == [{"sku": "abc"}, {"sku": "def"}]


def test_indexes_validate_registered_tables_and_columns(ctx: ChronosBranchContext) -> None:
    with pytest.raises(TableNotRegisteredError):
        ctx.create_index("missing", ["id"])
    with pytest.raises(TableNotRegisteredError):
        ctx.create_index("products", ["missing"])
    with pytest.raises(ValueError):
        ctx.create_index("products", [])


def test_branch_update_isolated_from_source_and_source_remains_mutable(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    main = ctx.checkout("main")

    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )
    main.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 12, "sku": "abc"},
    )

    assert _product(exp, "abc")["price"] == 15
    assert _product(main, "abc")["price"] == 12


def test_many_transactions_can_mutate_same_branch(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("work", from_branch="main")
    session = ctx.checkout("work")

    with session.transaction():
        session.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 30, "sku": "abc"},
        )
        session.execute(
            """
            INSERT INTO products (sku, name, price)
            VALUES (:sku, :name, :price)
            """,
            {"sku": "ghi", "name": "Gamma", "price": 40},
        )

    session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 41, "sku": "ghi"},
    )

    assert _product(session, "abc")["price"] == 30
    assert _product(session, "ghi")["price"] == 41
    assert ctx.checkout("main").query("SELECT * FROM products WHERE sku = 'ghi'") == []


@pytest.mark.parametrize("branch_backend", POSTGRES_BRANCH_BACKENDS)
def test_postgres_schema_qualified_tables_and_batch_edits(branch_backend: str) -> None:
    _reset_postgres_schema()
    context = ChronosBranchContext.connect(_postgres_dsn(), backend=branch_backend)
    try:
        context.db.execute("DROP SCHEMA IF EXISTS okg CASCADE")
        context.db.execute("CREATE SCHEMA okg")
        context.db.execute(
            """
            CREATE TABLE okg.graph_nodes (
              node_id TEXT PRIMARY KEY,
              subtype TEXT,
              attrs JSONB
            )
            """
        )
        context.db.execute(
            """
            INSERT INTO okg.graph_nodes VALUES
              ('n1', 'concept', '{"name": "one"}'::jsonb)
            """
        )
        context.db.commit()
        context.register_table("okg.graph_nodes", ["node_id"])

        context.create_branch("default", from_branch="main")
        session = context.checkout("default")
        with session.transaction():
            session.upsert_rows(
                "okg.graph_nodes",
                [
                    {"node_id": "n1", "subtype": "concept", "attrs": {"name": "uno"}},
                    {"node_id": "n2", "subtype": "claim", "attrs": {"name": "two"}},
                ],
            )
            session.delete_keys("okg.graph_nodes", [{"node_id": "missing"}])

        assert session.query(
            """
            SELECT node_id, subtype, attrs
            FROM okg.graph_nodes
            ORDER BY node_id
            """
        ) == [
            {"node_id": "n1", "subtype": "concept", "attrs": {"name": "uno"}},
            {"node_id": "n2", "subtype": "claim", "attrs": {"name": "two"}},
        ]
        assert context.checkout("main").query(
            "SELECT node_id, attrs FROM okg.graph_nodes ORDER BY node_id"
        ) == [{"node_id": "n1", "attrs": {"name": "one"}}]
    finally:
        context.close()


def test_rollback_discards_branch_writes(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("work", from_branch="main")
    session = ctx.checkout("work")

    with pytest.raises(RuntimeError):
        with session.transaction():
            session.execute(
                "UPDATE products SET price = :price WHERE sku = :sku",
                {"price": 99, "sku": "abc"},
            )
            raise RuntimeError("abort")

    assert _product(session, "abc")["price"] == 10


def test_delete_is_logical_and_hides_inherited_rows(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")

    exp.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})

    assert exp.query("SELECT * FROM products WHERE sku = 'abc'") == []
    assert _product(ctx.checkout("main"), "abc")["price"] == 10


def test_delete_then_branch_out_inherits_tombstone_and_parent_can_resurrect(
    ctx: ChronosBranchContext,
) -> None:
    main = ctx.checkout("main")
    main.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})
    ctx.create_branch("after_delete", from_branch="main")
    child = ctx.checkout("after_delete")

    assert main.query("SELECT * FROM products WHERE sku = 'abc'") == []
    assert child.query("SELECT * FROM products WHERE sku = 'abc'") == []

    main.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "abc", "name": "Alpha v2", "price": 17},
    )

    assert _product(main, "abc") == {"sku": "abc", "name": "Alpha v2", "price": 17}
    assert child.query("SELECT * FROM products WHERE sku = 'abc'") == []


def test_child_can_resurrect_deleted_inherited_row_without_affecting_parent(
    ctx: ChronosBranchContext,
) -> None:
    main = ctx.checkout("main")
    main.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})
    ctx.create_branch("child", from_branch="main")
    child = ctx.checkout("child")

    child.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "abc", "name": "Child Alpha", "price": 18},
    )

    assert _product(child, "abc") == {
        "sku": "abc",
        "name": "Child Alpha",
        "price": 18,
    }
    assert main.query("SELECT * FROM products WHERE sku = 'abc'") == []


def test_graph_branch_mutations_are_isolated_from_main(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("graph_exp", from_branch="main")
    graph = ctx.checkout("graph_exp")

    with graph.transaction():
        graph.execute(
            "UPDATE nodes SET score = :score WHERE node_id = :node_id",
            {"score": 20, "node_id": "n2"},
        )
        graph.execute("DELETE FROM edges WHERE edge_id = :edge_id", {"edge_id": "e1"})
        graph.execute(
            """
            INSERT INTO nodes (node_id, label, score)
            VALUES (:node_id, :label, :score)
            """,
            {"node_id": "n3", "label": "three", "score": 3},
        )
        graph.execute(
            """
            INSERT INTO edges (edge_id, src_id, dst_id)
            VALUES (:edge_id, :src_id, :dst_id)
            """,
            {"edge_id": "e2", "src_id": "n2", "dst_id": "n3"},
        )

    assert graph.query(
        """
        SELECT e.edge_id, e.src_id, e.dst_id, n.label AS dst_label
        FROM edges AS e
        JOIN nodes AS n ON n.node_id = e.dst_id
        ORDER BY e.edge_id
        """
    ) == [{"edge_id": "e2", "src_id": "n2", "dst_id": "n3", "dst_label": "three"}]
    assert graph.query("SELECT node_id, score FROM nodes ORDER BY node_id") == [
        {"node_id": "n1", "score": 1},
        {"node_id": "n2", "score": 20},
        {"node_id": "n3", "score": 3},
    ]

    main = ctx.checkout("main")
    assert main.query("SELECT edge_id, src_id, dst_id FROM edges ORDER BY edge_id") == [
        {"edge_id": "e1", "src_id": "n1", "dst_id": "n2"}
    ]
    assert main.query("SELECT node_id, score FROM nodes ORDER BY node_id") == [
        {"node_id": "n1", "score": 1},
        {"node_id": "n2", "score": 2},
    ]


def test_graph_child_branch_inherits_fork_point_not_later_parent_changes(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("graph_parent", from_branch="main")
    parent = ctx.checkout("graph_parent")
    parent.execute(
        """
        INSERT INTO nodes (node_id, label, score)
        VALUES (:node_id, :label, :score)
        """,
        {"node_id": "n3", "label": "three", "score": 3},
    )
    parent.execute(
        """
        INSERT INTO edges (edge_id, src_id, dst_id)
        VALUES (:edge_id, :src_id, :dst_id)
        """,
        {"edge_id": "e2", "src_id": "n2", "dst_id": "n3"},
    )

    ctx.create_branch("graph_child", from_branch="graph_parent")
    child = ctx.checkout("graph_child")

    parent.execute(
        "UPDATE nodes SET label = :label, score = :score WHERE node_id = :node_id",
        {"label": "three parent", "score": 30, "node_id": "n3"},
    )
    parent.execute(
        """
        INSERT INTO edges (edge_id, src_id, dst_id)
        VALUES (:edge_id, :src_id, :dst_id)
        """,
        {"edge_id": "e3", "src_id": "n3", "dst_id": "n1"},
    )
    child.execute("DELETE FROM edges WHERE edge_id = :edge_id", {"edge_id": "e2"})
    child.execute(
        "UPDATE nodes SET label = :label, score = :score WHERE node_id = :node_id",
        {"label": "three child", "score": 13, "node_id": "n3"},
    )

    assert parent.query(
        "SELECT node_id, label, score FROM nodes WHERE node_id = 'n3'"
    ) == [{"node_id": "n3", "label": "three parent", "score": 30}]
    assert parent.query("SELECT edge_id FROM edges ORDER BY edge_id") == [
        {"edge_id": "e1"},
        {"edge_id": "e2"},
        {"edge_id": "e3"},
    ]
    assert child.query(
        "SELECT node_id, label, score FROM nodes WHERE node_id = 'n3'"
    ) == [{"node_id": "n3", "label": "three child", "score": 13}]
    assert child.query("SELECT edge_id FROM edges ORDER BY edge_id") == [
        {"edge_id": "e1"}
    ]


def test_graph_recursive_reachability_uses_branch_visible_edges(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("graph_walk", from_branch="main")
    graph = ctx.checkout("graph_walk")
    with graph.transaction():
        graph.execute(
            "INSERT INTO nodes (node_id, label, score) VALUES (:node_id, :label, :score)",
            {"node_id": "n3", "label": "three", "score": 3},
        )
        graph.execute(
            "INSERT INTO nodes (node_id, label, score) VALUES (:node_id, :label, :score)",
            {"node_id": "n4", "label": "four", "score": 4},
        )
        graph.execute(
            "INSERT INTO edges (edge_id, src_id, dst_id) VALUES (:edge_id, :src_id, :dst_id)",
            {"edge_id": "e2", "src_id": "n2", "dst_id": "n3"},
        )
        graph.execute(
            "INSERT INTO edges (edge_id, src_id, dst_id) VALUES (:edge_id, :src_id, :dst_id)",
            {"edge_id": "e3", "src_id": "n3", "dst_id": "n4"},
        )

    assert graph.query(
        """
        WITH RECURSIVE reach(node_id, depth) AS (
          SELECT dst_id, 1
          FROM edges
          WHERE src_id = :start
          UNION ALL
          SELECT e.dst_id, r.depth + 1
          FROM edges AS e
          JOIN reach AS r ON e.src_id = r.node_id
          WHERE r.depth < 4
        )
        SELECT node_id, MIN(depth) AS depth
        FROM reach
        GROUP BY node_id
        ORDER BY depth, node_id
        """,
        {"start": "n1"},
    ) == [
        {"node_id": "n2", "depth": 1},
        {"node_id": "n3", "depth": 2},
        {"node_id": "n4", "depth": 3},
    ]
    assert ctx.checkout("main").query(
        """
        WITH RECURSIVE reach(node_id, depth) AS (
          SELECT dst_id, 1
          FROM edges
          WHERE src_id = :start
          UNION ALL
          SELECT e.dst_id, r.depth + 1
          FROM edges AS e
          JOIN reach AS r ON e.src_id = r.node_id
          WHERE r.depth < 4
        )
        SELECT node_id, MIN(depth) AS depth
        FROM reach
        GROUP BY node_id
        ORDER BY depth, node_id
        """,
        {"start": "n1"},
    ) == [{"node_id": "n2", "depth": 1}]


def test_graph_diff_and_merge_apply_nodes_and_edges(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("graph_merge", from_branch="main")
    graph = ctx.checkout("graph_merge")
    with graph.transaction():
        graph.execute(
            "UPDATE nodes SET score = :score WHERE node_id = :node_id",
            {"score": 11, "node_id": "n1"},
        )
        graph.execute("DELETE FROM edges WHERE edge_id = :edge_id", {"edge_id": "e1"})
        graph.execute(
            "INSERT INTO nodes (node_id, label, score) VALUES (:node_id, :label, :score)",
            {"node_id": "n3", "label": "three", "score": 3},
        )
        graph.execute(
            "INSERT INTO edges (edge_id, src_id, dst_id) VALUES (:edge_id, :src_id, :dst_id)",
            {"edge_id": "e2", "src_id": "n1", "dst_id": "n3"},
        )

    node_changes = {(change.key["node_id"], change.change) for change in ctx.diff_rows("main", "graph_merge", "nodes")}
    edge_changes = {(change.key["edge_id"], change.change) for change in ctx.diff_rows("main", "graph_merge", "edges")}
    assert node_changes == {("n1", "modified"), ("n3", "added")}
    assert edge_changes == {("e1", "deleted"), ("e2", "added")}

    assert ctx.merge_apply(source="graph_merge", target="main").applied == 4
    main = ctx.checkout("main")
    assert main.query("SELECT node_id, score FROM nodes ORDER BY node_id") == [
        {"node_id": "n1", "score": 11},
        {"node_id": "n2", "score": 2},
        {"node_id": "n3", "score": 3},
    ]
    assert main.query("SELECT edge_id, src_id, dst_id FROM edges ORDER BY edge_id") == [
        {"edge_id": "e2", "src_id": "n1", "dst_id": "n3"}
    ]


def test_join_subquery_ordering_and_aggregation_use_logical_tables(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )

    joined = exp.query(
        """
        SELECT p.sku, p.price, o.quantity
        FROM products AS p
        JOIN orders AS o ON p.sku = o.sku
        WHERE p.price >= :min_price
        ORDER BY p.sku
        """,
        {"min_price": 10},
    )
    aggregate = exp.query(
        """
        SELECT COUNT(*) AS count, SUM(price) AS total
        FROM products
        WHERE sku IN (SELECT sku FROM orders WHERE quantity >= :quantity)
        """,
        {"quantity": 1},
    )

    assert joined == [
        {"sku": "abc", "price": 15, "quantity": 2},
        {"sku": "def", "price": 20, "quantity": 1},
    ]
    assert aggregate == [{"count": 2, "total": 35}]


def test_complex_join_cte_group_by_and_anti_join_after_branch_writes(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("analytics", from_branch="main")
    session = ctx.checkout("analytics")
    with session.transaction():
        session.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 15, "sku": "abc"},
        )
        session.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o2"})
        session.execute(
            "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
            {"sku": "ghi", "name": "Gamma", "price": 7},
        )
        session.execute(
            """
            INSERT INTO orders (order_id, sku, quantity)
            VALUES (:order_id, :sku, :quantity)
            """,
            {"order_id": "o3", "sku": "ghi", "quantity": 5},
        )

    revenue = session.query(
        """
        WITH revenue AS (
          SELECT p.sku, p.name, SUM(p.price * o.quantity) AS total
          FROM products AS p
          JOIN orders AS o ON o.sku = p.sku
          GROUP BY p.sku, p.name
        )
        SELECT sku, name, total
        FROM revenue
        WHERE total >= :min_total
        ORDER BY total DESC, sku
        """,
        {"min_total": 10},
    )
    missing_orders = session.query(
        """
        SELECT p.sku
        FROM products AS p
        WHERE NOT EXISTS (
          SELECT 1 FROM orders AS o WHERE o.sku = p.sku
        )
        ORDER BY p.sku
        """
    )
    main_revenue = ctx.checkout("main").query(
        """
        SELECT p.sku, SUM(p.price * o.quantity) AS total
        FROM products p
        JOIN orders o ON p.sku = o.sku
        GROUP BY p.sku
        ORDER BY p.sku
        """
    )

    assert revenue == [
        {"sku": "ghi", "name": "Gamma", "total": 35},
        {"sku": "abc", "name": "Alpha", "total": 30},
    ]
    assert missing_orders == [{"sku": "def"}]
    assert main_revenue == [
        {"sku": "abc", "total": 20},
        {"sku": "def", "total": 20},
    ]


def test_left_join_aggregation_sees_branch_local_deletes_and_inserts(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("report", from_branch="main")
    session = ctx.checkout("report")
    session.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o1"})
    session.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "zzz", "name": "No Orders", "price": 100},
    )

    rows = session.query(
        """
        SELECT p.sku, COUNT(o.order_id) AS order_count, COALESCE(SUM(o.quantity), 0) AS units
        FROM products AS p
        LEFT JOIN orders AS o ON o.sku = p.sku
        GROUP BY p.sku
        ORDER BY p.sku
        """
    )

    assert rows == [
        {"sku": "abc", "order_count": 0, "units": 0},
        {"sku": "def", "order_count": 1, "units": 1},
        {"sku": "zzz", "order_count": 0, "units": 0},
    ]


def test_subquery_driven_update_and_delete_match_branch_view(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("mutate", from_branch="main")
    session = ctx.checkout("mutate")
    session.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o2"})
    session.execute(
        """
        UPDATE products
        SET price = :price
        WHERE sku IN (
          SELECT sku FROM orders WHERE quantity >= :min_quantity
        )
        """,
        {"price": 99, "min_quantity": 2},
    )
    deleted = session.execute(
        """
        DELETE FROM products
        WHERE sku NOT IN (SELECT sku FROM orders)
        """
    )

    assert deleted.rowcount == 1
    assert session.query("SELECT sku, price FROM products ORDER BY sku") == [
        {"sku": "abc", "price": 99}
    ]
    assert ctx.checkout("main").query("SELECT sku, price FROM products ORDER BY sku") == [
        {"sku": "abc", "price": 10},
        {"sku": "def", "price": 20},
    ]


def test_noop_update_and_delete_return_zero_and_do_not_change_physical_rows(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("noop", from_branch="main")
    session = ctx.checkout("noop")

    updated = session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 1000, "sku": "missing"},
    )
    deleted = session.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "missing"})

    assert updated.rowcount == 0
    assert deleted.rowcount == 0
    assert session.query("SELECT sku, price FROM products ORDER BY sku") == [
        {"sku": "abc", "price": 10},
        {"sku": "def", "price": 20},
    ]
    assert session.query("SELECT order_id, sku FROM orders ORDER BY order_id") == [
        {"order_id": "o1", "sku": "abc"},
        {"order_id": "o2", "sku": "def"},
    ]


def test_multi_table_transaction_rollback_restores_all_tables(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("rollback_all", from_branch="main")
    session = ctx.checkout("rollback_all")

    with pytest.raises(RuntimeError):
        with session.transaction():
            session.execute(
                "UPDATE products SET price = :price WHERE sku = :sku",
                {"price": 55, "sku": "abc"},
            )
            session.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o1"})
            session.execute(
                "INSERT INTO nodes (node_id, label, score) VALUES (:node_id, :label, :score)",
                {"node_id": "n3", "label": "three", "score": 3},
            )
            raise RuntimeError("abort")

    assert _product(session, "abc")["price"] == 10
    assert session.query("SELECT order_id FROM orders ORDER BY order_id") == [
        {"order_id": "o1"},
        {"order_id": "o2"},
    ]
    assert session.query("SELECT node_id FROM nodes ORDER BY node_id") == [
        {"node_id": "n1"},
        {"node_id": "n2"},
    ]


def test_repeated_delete_and_reinsert_same_key_across_forks(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("left", from_branch="main")
    left = ctx.checkout("left")
    left.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})
    ctx.create_branch("right", from_branch="left")
    right = ctx.checkout("right")

    left.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "abc", "name": "Left Alpha", "price": 31},
    )
    right.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "abc", "name": "Right Alpha", "price": 32},
    )
    left.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})

    assert left.query("SELECT * FROM products WHERE sku = 'abc'") == []
    assert _product(right, "abc") == {
        "sku": "abc",
        "name": "Right Alpha",
        "price": 32,
    }
    assert _product(ctx.checkout("main"), "abc")["price"] == 10


def test_deduplicating_backends_match_copy_reference_on_complex_scenario(
    sql_backend: str,
) -> None:
    def run_scenario(backend: str) -> dict[str, list[dict]]:
        ctx = _make_context(sql_backend, backend)
        try:
            ctx.create_index("products", ["price", "sku"], name="price_sku")
            ctx.create_branch("exp", from_branch="main")
            exp = ctx.checkout("exp")
            with exp.transaction():
                exp.execute(
                    "UPDATE products SET price = :price WHERE sku = :sku",
                    {"price": 15, "sku": "abc"},
                )
                exp.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o2"})
                exp.execute(
                    "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
                    {"sku": "ghi", "name": "Gamma", "price": 7},
                )
                exp.execute(
                    "INSERT INTO orders (order_id, sku, quantity) VALUES (:order_id, :sku, :quantity)",
                    {"order_id": "o3", "sku": "ghi", "quantity": 5},
                )
            ctx.create_branch("child", from_branch="exp")
            child = ctx.checkout("child")
            child.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})
            child.execute(
                "UPDATE products SET price = :price WHERE sku IN (SELECT sku FROM orders)",
                {"price": 33},
            )
            return {
                "main_products": ctx.checkout("main").query(
                    "SELECT sku, price FROM products ORDER BY sku"
                ),
                "exp_revenue": exp.query(
                    """
                    SELECT p.sku, SUM(p.price * o.quantity) AS revenue
                    FROM products p
                    JOIN orders o ON p.sku = o.sku
                    GROUP BY p.sku
                    ORDER BY p.sku
                    """
                ),
                "child_left_join": child.query(
                    """
                    SELECT p.sku, COUNT(o.order_id) AS orders, COALESCE(SUM(o.quantity), 0) AS units
                    FROM products p
                    LEFT JOIN orders o ON p.sku = o.sku
                    GROUP BY p.sku
                    ORDER BY p.sku
                    """
                ),
            }
        finally:
            ctx.close()

    reference = run_scenario("copy")
    assert run_scenario("interval") == reference
    assert run_scenario("log") == reference


def test_log_backend_reads_do_not_use_temp_tables(sql_backend: str) -> None:
    ctx = _make_context(sql_backend, "log")
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 15, "sku": "abc"},
        )
        exp.execute("DELETE FROM orders WHERE order_id = :order_id", {"order_id": "o2"})
        exp.execute(
            "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
            {"sku": "ghi", "name": "Gamma", "price": 7},
        )
        exp.execute(
            "INSERT INTO orders (order_id, sku, quantity) VALUES (:order_id, :sku, :quantity)",
            {"order_id": "o3", "sku": "ghi", "quantity": 5},
        )

        def fail_temp(*args, **kwargs):
            raise AssertionError("log read path should not use temp tables")

        ctx.db.create_temp_table = fail_temp  # type: ignore[method-assign]
        ctx.db.drop_table = fail_temp  # type: ignore[method-assign]

        assert exp.query(
            """
            SELECT p.sku, SUM(p.price * o.quantity) AS revenue
            FROM products p
            JOIN orders o ON p.sku = o.sku
            GROUP BY p.sku
            ORDER BY p.sku
            """
        ) == [
            {"sku": "abc", "revenue": 30},
            {"sku": "ghi", "revenue": 35},
        ]
    finally:
        ctx.close()


def test_log_backend_does_not_create_txn_allocator_table(sql_backend: str) -> None:
    ctx = _make_context(sql_backend, "log")
    try:
        exp = ctx.checkout("main")
        exp.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 13, "sku": "abc"},
        )

        assert not _table_exists(ctx, "_chronos_branch_log_txns")
        assert _product(exp, "abc")["price"] == 13
    finally:
        ctx.close()


def test_checkpoint_is_stable_after_later_branch_writes(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )
    ctx.create_checkpoint("before_more_work", branch="exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 25, "sku": "abc"},
    )

    checkpoint = ctx.checkout_checkpoint("before_more_work")
    assert _product(checkpoint, "abc")["price"] == 15
    assert _product(exp, "abc")["price"] == 25
    with pytest.raises(BranchingError):
        checkpoint.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 1, "sku": "abc"},
        )


def test_interval_checkpoint_split_allocates_snapshot_forward() -> None:
    ctx = _make_context("sqlite", "interval")
    try:
        ctx.create_branch("exp", from_branch="main")
        before = ctx._backend._current_segment("exp")  # type: ignore[attr-defined]

        created = ctx.create_checkpoint("snap", branch="exp")

        after = ctx._backend._current_segment("exp")  # type: ignore[attr-defined]
        snapshot = ctx._backend._segment(int(created.ref))  # type: ignore[attr-defined]
        assert snapshot.live_lo == before.live_lo
        assert snapshot.live_hi == after.live_lo
        assert after.live_hi == before.live_hi
        assert after.live_lo > before.live_lo
    finally:
        ctx.close()


def test_create_branch_from_checkpoint(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )
    ctx.create_checkpoint("snap", branch="exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 25, "sku": "abc"},
    )
    ctx.create_branch_from_checkpoint("restored", "snap")

    restored = ctx.checkout("restored")
    restored.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 16, "sku": "abc"},
    )

    assert _product(restored, "abc")["price"] == 16
    assert _product(exp, "abc")["price"] == 25


@pytest.mark.parametrize("branch_backend", BRANCH_BACKENDS)
def test_branch_metadata_round_trips_across_backends(branch_backend: str) -> None:
    ctx = _make_context("sqlite", branch_backend)
    try:
        ctx.create_branch(
            "exp",
            from_branch="main",
            metadata={"owner": "okg", "state": "mutable"},
        )
        assert ctx.get_branch("exp").metadata == {"owner": "okg", "state": "mutable"}

        updated = ctx.update_branch_metadata(
            "exp", {"owner": "okg", "state": "abandoned"}
        )

        assert updated.metadata == {"owner": "okg", "state": "abandoned"}
        assert ctx.get_branch("exp").metadata == updated.metadata
        assert {
            branch.branch_id: branch.metadata for branch in ctx.list_branches()
        }["exp"] == updated.metadata
    finally:
        ctx.close()


@pytest.mark.parametrize("branch_backend", BRANCH_BACKENDS)
def test_checkpoint_metadata_round_trips_across_backends(branch_backend: str) -> None:
    ctx = _make_context("sqlite", branch_backend)
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 15, "sku": "abc"},
        )

        metadata = {
            "deployment": "test",
            "catalog_version_id": 7,
            "status": "published",
        }
        created = ctx.create_checkpoint("generation-1", branch="exp", metadata=metadata)

        assert created.metadata == metadata
        assert ctx.get_checkpoint("generation-1").metadata == metadata
        assert [
            checkpoint.checkpoint_id
            for checkpoint in ctx.list_checkpoints(metadata_filter={"status": "published"})
        ] == ["generation-1"]
        assert [
            checkpoint.checkpoint_id
            for checkpoint in ctx.list_checkpoints(branch="exp")
        ] == ["generation-1"]
    finally:
        ctx.close()


@pytest.mark.parametrize("branch_backend", BRANCH_BACKENDS)
def test_checkpoint_list_filters_by_branch_and_metadata(branch_backend: str) -> None:
    ctx = _make_context("sqlite", branch_backend)
    try:
        ctx.create_branch("left", from_branch="main")
        ctx.create_branch("right", from_branch="main")
        ctx.create_checkpoint("left-draft", branch="left", metadata={"status": "draft"})
        ctx.create_checkpoint(
            "right-published",
            branch="right",
            metadata={"status": "published"},
        )

        assert [
            checkpoint.checkpoint_id
            for checkpoint in ctx.list_checkpoints(branch="left")
        ] == ["left-draft"]
        assert [
            checkpoint.checkpoint_id
            for checkpoint in ctx.list_checkpoints(metadata_filter={"status": "published"})
        ] == ["right-published"]
        assert ctx.checkout_checkpoint("right-published").branch_id == "right"
    finally:
        ctx.close()


def test_diff_rows_and_merge_apply(ctx: ChronosBranchContext) -> None:
    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    exp.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )
    exp.execute("DELETE FROM products WHERE sku = :sku", {"sku": "def"})
    exp.execute(
        "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
        {"sku": "ghi", "name": "Gamma", "price": 40},
    )

    changes = ctx.diff_rows("main", "exp", "products")
    assert [(c.key, c.change) for c in changes] == [
        ({"sku": "abc"}, "modified"),
        ({"sku": "def"}, "deleted"),
        ({"sku": "ghi"}, "added"),
    ]
    preview = ctx.merge_preview(source="exp", target="main")
    assert not preview.conflicts
    assert ctx.merge_apply(source="exp", target="main").applied == 3

    main = ctx.checkout("main")
    assert _product(main, "abc")["price"] == 15
    assert main.query("SELECT * FROM products WHERE sku = 'def'") == []
    assert _product(main, "ghi")["price"] == 40


def test_interval_branch_creation_records_one_unit_fork_base(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        rows = ctx.db.execute(
            """
            SELECT segment_id, parent_segment_id, owner_branch_id, segment_kind,
                   live_lo, live_hi, branch_point
            FROM _chronos_branch_interval_segments
            ORDER BY segment_id
            """
        ).fetchall()
        fork_bases = [row for row in rows if row["segment_kind"] == "fork_base"]
        assert len(fork_bases) == 1
        fork_base = fork_bases[0]
        assert int(fork_base["live_hi"]) - int(fork_base["live_lo"]) == 1
        assert int(fork_base["branch_point"]) == int(fork_base["live_lo"])

        main_segment = int(ctx.get_branch("main").current_ref)
        agent_segment = int(ctx.get_branch("agent").current_ref)
        children = ctx.db.execute(
            """
            SELECT segment_id, parent_segment_id, segment_kind, live_lo, live_hi
            FROM _chronos_branch_interval_segments
            WHERE segment_id IN (?, ?)
            ORDER BY segment_id
            """,
            (main_segment, agent_segment),
        ).fetchall()
        assert {int(row["parent_segment_id"]) for row in children} == {
            int(fork_base["segment_id"])
        }
        assert {row["segment_kind"] for row in children} == {"mutable"}
        assert int(fork_base["live_lo"]) <= int(fork_base["live_hi"]) <= min(
            int(row["live_lo"]) for row in children
        )

        main = ctx.checkout("main")
        agent = ctx.checkout("agent")
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 30},
        )
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 40},
        )

        base_row = ctx.db.execute(
            """
            SELECT price
            FROM _chronos_b_interval_products
            WHERE sku = 'abc'
              AND live_lo <= ?
              AND ? < live_hi
              AND deleted = FALSE
            """,
            (fork_base["branch_point"], fork_base["branch_point"]),
        ).fetchone()
        assert base_row["price"] == 10
    finally:
        ctx.close()


def test_interval_three_way_merge_applies_only_source_changes(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "def", "price": 22},
        )

        preview = ctx.merge_preview(source="agent", target="main")
        assert preview.conflicts == []
        assert [(change.key, change.change, change.after) for change in preview.changes] == [
            ({"sku": "abc"}, "modified", {"sku": "abc", "name": "Alpha", "price": 11})
        ]

        assert ctx.merge_apply(source="agent", target="main").applied == 1
        main = ctx.checkout("main")
        assert _product(main, "abc")["price"] == 11
        assert _product(main, "def")["price"] == 22
    finally:
        ctx.close()


def test_interval_direct_sibling_merge_does_not_walk_ancestry(
    sql_backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "def", "price": 22},
        )

        def fail_ancestry_walk(segment_id: int) -> list[dict[str, object]]:
            raise AssertionError(f"unexpected ancestry walk for segment {segment_id}")

        monkeypatch.setattr(
            ctx._backend,
            "_segment_ancestry_rows",
            fail_ancestry_walk,
        )

        preview = ctx.merge_preview(source="agent", target="main")
        assert preview.conflicts == []
        assert [(change.key, change.change) for change in preview.changes] == [
            ({"sku": "abc"}, "modified")
        ]
        assert ctx.merge_apply(source="agent", target="main").applied == 1
        assert _product(ctx.checkout("main"), "abc")["price"] == 11
        assert _product(ctx.checkout("main"), "def")["price"] == 22
    finally:
        ctx.close()


def test_interval_merge_apply_batches_large_clean_changes(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        main = ctx.checkout("main")
        main.upsert_rows(
            "products",
            [
                {"sku": f"base:{idx}", "name": f"Base {idx}", "price": idx}
                for idx in range(300)
            ],
        )
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        agent.upsert_rows(
            "products",
            [
                {"sku": f"base:{idx}", "name": f"Changed {idx}", "price": idx + 1000}
                for idx in range(150)
            ]
            + [
                {"sku": f"new:{idx}", "name": f"New {idx}", "price": idx + 2000}
                for idx in range(150)
            ],
        )
        agent.delete_keys("products", [{"sku": f"base:{idx}"} for idx in range(150, 225)])

        result = ctx.merge_apply(source="agent", target="main")

        assert result.applied == 375
        rows = main.query(
            """
            SELECT
              SUM(CASE WHEN sku LIKE 'new:%' THEN 1 ELSE 0 END) AS new_count,
              SUM(CASE WHEN sku LIKE 'base:%' AND price >= 1000 THEN 1 ELSE 0 END) AS changed_count,
              SUM(CASE WHEN sku LIKE 'base:%' THEN 1 ELSE 0 END) AS base_count
            FROM products
            """
        )[0]
        assert rows == {"new_count": 150, "changed_count": 150, "base_count": 225}
        assert main.query("SELECT * FROM products WHERE sku = :sku", {"sku": "base:175"}) == []
    finally:
        ctx.close()


def test_interval_three_way_merge_detects_conflicts(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )
        agent.execute("DELETE FROM products WHERE sku = :sku", {"sku": "def"})

        preview = ctx.merge_preview(source="agent", target="main")
        assert [(change.key, change.change) for change in preview.changes] == [
            ({"sku": "def"}, "deleted")
        ]
        assert [
            (conflict.key, conflict.change, conflict.before, conflict.after)
            for conflict in preview.conflicts
        ] == [
            (
                {"sku": "abc"},
                "modified",
                {"sku": "abc", "name": "Alpha", "price": 12},
                {"sku": "abc", "name": "Alpha", "price": 11},
            )
        ]
        with pytest.raises(BranchingError):
            ctx.merge_apply(source="agent", target="main")
        main = ctx.checkout("main")
        assert _product(main, "abc")["price"] == 12
        assert _product(main, "def")["price"] == 20
    finally:
        ctx.close()


def test_interval_merge_apply_conflict_rolls_back_clean_changes(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        agent.execute("DELETE FROM products WHERE sku = :sku", {"sku": "def"})
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )

        with pytest.raises(BranchingError):
            ctx.merge_apply(source="agent", target="main")

        assert _product(main, "abc")["price"] == 12
        assert _product(main, "def")["price"] == 20
    finally:
        ctx.close()


@pytest.mark.parametrize(
    ("policy", "expected_price", "expected_applied"),
    [
        ("source_wins", 11, 1),
        ("target_wins", 12, 0),
    ],
)
def test_interval_merge_builtin_conflict_policies(
    sql_backend: str, policy: str, expected_price: int, expected_applied: int
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )

        preview = ctx.merge_preview(source="agent", target="main", policy=policy)
        assert len(preview.conflicts) == 1
        assert preview.conflicts[0].conflict_id

        result = ctx.merge_apply(source="agent", target="main", policy=policy)

        assert result.applied == expected_applied
        assert _product(ctx.checkout("main"), "abc")["price"] == expected_price
    finally:
        ctx.close()


def test_interval_merge_snapshot_isolation_rejects_write_write_conflict(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )

        with pytest.raises(BranchingError):
            ctx.merge_apply(source="agent", target="main", policy="snapshot_isolation")

        assert _product(main, "abc")["price"] == 12
    finally:
        ctx.close()


def test_interval_merge_snapshot_isolation_first_committer_wins(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("txn_a", from_branch="main")
        ctx.create_branch("txn_b", from_branch="main")
        txn_a = ctx.checkout("txn_a")
        txn_b = ctx.checkout("txn_b")
        main = ctx.checkout("main")
        txn_a.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        txn_b.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )

        first = ctx.merge_apply(source="txn_a", target="main", policy="snapshot_isolation")
        assert first.applied == 1
        assert _product(main, "abc")["price"] == 11

        preview = ctx.merge_preview(source="txn_b", target="main", policy="snapshot_isolation")
        assert len(preview.conflicts) == 1
        assert preview.conflicts[0].before == {
            "sku": "abc",
            "name": "Alpha",
            "price": 11,
        }
        assert preview.conflicts[0].after == {
            "sku": "abc",
            "name": "Alpha",
            "price": 12,
        }
        with pytest.raises(BranchingError, match="write-write"):
            ctx.merge_apply(source="txn_b", target="main", policy="snapshot_isolation")

        assert _product(main, "abc")["price"] == 11
    finally:
        ctx.close()


def test_interval_merge_snapshot_isolation_same_value_write_conflicts(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("txn_a", from_branch="main")
        ctx.create_branch("txn_b", from_branch="main")
        txn_a = ctx.checkout("txn_a")
        txn_b = ctx.checkout("txn_b")
        main = ctx.checkout("main")
        txn_a.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        txn_b.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )

        assert ctx.merge_apply(source="txn_a", target="main", policy="snapshot_isolation").applied == 1
        preview = ctx.merge_preview(source="txn_b", target="main", policy="snapshot_isolation")
        assert len(preview.conflicts) == 1
        assert preview.conflicts[0].before == preview.conflicts[0].after

        with pytest.raises(BranchingError, match="write-write"):
            ctx.merge_apply(source="txn_b", target="main", policy="snapshot_isolation")

        assert _product(main, "abc")["price"] == 11
    finally:
        ctx.close()


def test_interval_merge_weak_snapshot_isolation_does_not_check_write_write_conflicts(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )

        preview = ctx.merge_preview(
            source="agent",
            target="main",
            policy="weak_snapshot_isolation",
        )
        assert len(preview.conflicts) == 1
        assert preview.resolution.conflict_choices == {
            preview.conflicts[0].conflict_id: "source"
        }

        result = ctx.merge_apply(
            source="agent",
            target="main",
            policy="weak_snapshot_isolation",
        )

        assert result.applied == 1
        assert _product(main, "abc")["price"] == 11
    finally:
        ctx.close()


def test_interval_merge_weak_snapshot_isolation_later_committer_overwrites(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("txn_a", from_branch="main")
        ctx.create_branch("txn_b", from_branch="main")
        txn_a = ctx.checkout("txn_a")
        txn_b = ctx.checkout("txn_b")
        main = ctx.checkout("main")
        txn_a.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        txn_b.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )

        assert ctx.merge_apply(source="txn_a", target="main", policy="snapshot_isolation").applied == 1
        result = ctx.merge_apply(
            source="txn_b",
            target="main",
            policy="weak_snapshot_isolation",
        )

        assert result.applied == 1
        assert _product(main, "abc")["price"] == 12
    finally:
        ctx.close()


def test_interval_manual_review_resolution_is_revalidated(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )

        preview = ctx.merge_preview(source="agent", target="main", policy="manual_review")
        conflict_id = preview.conflicts[0].conflict_id
        assert conflict_id is not None
        resolution = MergeResolution({conflict_id: "source"})

        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 13},
        )
        with pytest.raises(BranchingError, match="stale"):
            ctx.merge_apply(
                source="agent",
                target="main",
                policy="manual_review",
                resolution=resolution,
            )

        assert _product(main, "abc")["price"] == 13
    finally:
        ctx.close()


def test_interval_manual_review_resolution_applies_source_choice(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )

        preview = ctx.merge_preview(source="agent", target="main", policy="manual_review")
        conflict_id = preview.conflicts[0].conflict_id
        assert conflict_id is not None
        result = ctx.merge_apply(
            source="agent",
            target="main",
            policy="manual_review",
            resolution=MergeResolution({conflict_id: "source"}),
        )

        assert result.applied == 1
        assert _product(main, "abc")["price"] == 11
    finally:
        ctx.close()


def test_interval_custom_validator_rejects_and_rolls_back(
    sql_backend: str,
) -> None:
    class RejectExpensiveMerge:
        def validate(self, context, preview):
            assert context.backend == "interval"
            assert context.phase == "apply"
            for change in preview.changes:
                if change.after is not None and change.after["price"] > 100:
                    return MergeValidationResult.reject("price too high")
            return MergeValidationResult.accept()

    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 101},
        )
        agent.execute("DELETE FROM products WHERE sku = :sku", {"sku": "def"})
        policy = MergePolicy(
            name="reject_expensive",
            mode="custom",
            validators=(RejectExpensiveMerge(),),
        )

        with pytest.raises(BranchingError, match="price too high"):
            ctx.merge_apply(source="agent", target="main", policy=policy)

        main = ctx.checkout("main")
        assert _product(main, "abc")["price"] == 10
        assert _product(main, "def")["price"] == 20
    finally:
        ctx.close()


def test_interval_custom_resolver_source_wins(
    sql_backend: str,
) -> None:
    class SourceResolver:
        def resolve(self, context, preview):
            assert context.backend == "interval"
            return MergeResolution(
                {
                    conflict.conflict_id: "source"
                    for conflict in preview.conflicts
                    if conflict.conflict_id is not None
                }
            )

    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )
        policy = MergePolicy(name="source_resolver", mode="custom", resolver=SourceResolver())
        preview = ctx.merge_preview(source="agent", target="main", policy=policy)

        result = ctx.merge_apply(
            source="agent",
            target="main",
            policy=policy,
            resolution=preview.resolution,
        )

        assert result.applied == 1
        assert _product(main, "abc")["price"] == 11
    finally:
        ctx.close()


def test_interval_custom_resolver_is_not_called_inside_apply(
    sql_backend: str,
) -> None:
    class FailingResolver:
        def resolve(self, context, preview):
            raise AssertionError("resolver must run during preview, not atomic apply")

    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        main = ctx.checkout("main")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 11},
        )
        main.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 12},
        )
        policy = MergePolicy(name="failing_resolver", mode="custom", resolver=FailingResolver())

        with pytest.raises(BranchingError, match="unresolved"):
            ctx.merge_apply(source="agent", target="main", policy=policy)

        assert _product(main, "abc")["price"] == 12
    finally:
        ctx.close()


def test_interval_snapshot_isolation_many_branch_transactions(
    sql_backend: str,
) -> None:
    if sql_backend == "postgres":
        _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(
        _database_url(sql_backend, "interval"),
        backend="interval",
        interval_child_width=2,
    )
    try:
        ctx.db.execute("CREATE TABLE items (id TEXT PRIMARY KEY, quantity INTEGER)")
        ctx.db.executemany(
            "INSERT INTO items VALUES (?, ?)",
            [(f"item:{idx}", idx) for idx in range(100)],
        )
        ctx.db.commit()
        ctx.register_table("items", ["id"])

        for iteration in range(200):
            branch_id = f"txn_{iteration}"
            key = f"item:{iteration % 100}"
            ctx.create_branch(branch_id, from_branch="main")
            branch = ctx.checkout(branch_id)
            with branch.transaction():
                assert branch.query(
                    "SELECT quantity FROM items WHERE id = :id",
                    {"id": key},
                )
                branch.execute(
                    """
                    UPDATE items
                    SET quantity = quantity + 1
                    WHERE id = :id
                    """,
                    {"id": key},
                )
            result = ctx.merge_apply(
                source=branch_id,
                target="main",
                policy="snapshot_isolation",
            )
            assert result.applied == 1
            ctx.delete_branch(branch_id)
        ctx.wait_for_background_work()

        rows = ctx.checkout("main").query(
            "SELECT SUM(quantity) AS total FROM items"
        )
        assert rows == [{"total": 5150}]
    finally:
        ctx.close()


def test_interval_nested_merge_uses_fork_time_parent_state(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("parent", from_branch="main")
        parent = ctx.checkout("parent")
        parent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 15},
        )
        ctx.create_branch("child", from_branch="parent")
        child = ctx.checkout("child")
        parent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 17},
        )
        child.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 16},
        )

        preview = ctx.merge_preview(source="child", target="parent")
        assert preview.changes == []
        assert [
            (conflict.key, conflict.before, conflict.after)
            for conflict in preview.conflicts
        ] == [
            (
                {"sku": "abc"},
                {"sku": "abc", "name": "Alpha", "price": 17},
                {"sku": "abc", "name": "Alpha", "price": 16},
            )
        ]
    finally:
        ctx.close()


def test_interval_delete_branch_cascades_to_subbranches(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("parent", from_branch="main")
        ctx.create_branch("child", from_branch="parent")
        ctx.create_branch("grandchild", from_branch="child")

        ctx.delete_branch("parent")
        ctx.wait_for_background_work()

        assert [branch.branch_id for branch in ctx.list_branches()] == ["main"]
        for branch in ("parent", "child", "grandchild"):
            with pytest.raises(BranchNotFoundError):
                ctx.get_branch(branch)
    finally:
        ctx.close()


def test_interval_delete_branch_cascades_branch_from_checkpoint(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("parent", from_branch="main")
        ctx.create_checkpoint("snap", branch="parent")
        ctx.create_branch_from_checkpoint("restored", "snap")

        row = ctx.db.execute(
            """
            SELECT parent_branch_id, child_count
            FROM _chronos_branch_interval_branches
            WHERE branch_id = 'restored'
            """
        ).fetchone()
        assert row["parent_branch_id"] == "parent"
        assert row["child_count"] == 0

        parent = ctx.db.execute(
            """
            SELECT child_count
            FROM _chronos_branch_interval_branches
            WHERE branch_id = 'parent'
            """
        ).fetchone()
        assert parent["child_count"] == 1

        ctx.delete_branch("parent")
        ctx.wait_for_background_work()

        assert [branch.branch_id for branch in ctx.list_branches()] == ["main"]
        for branch in ("parent", "restored"):
            with pytest.raises(BranchNotFoundError):
                ctx.get_branch(branch)
    finally:
        ctx.close()


def test_postgres_interval_delete_leaf_branch_from_wide_main_fanout() -> None:
    _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(
        _postgres_dsn(), backend="interval", interval_child_width=2
    )
    ctx._test_sql_backend = "postgres"  # type: ignore[attr-defined]
    try:
        ctx.db.execute(
            "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price INTEGER)"
        )
        ctx.db.executemany(
            "INSERT INTO products VALUES (?, ?, ?)",
            [("abc", "Alpha", 10), ("def", "Delta", 20)],
        )
        ctx.db.commit()
        ctx.register_table("products", ["sku"])

        for index in range(200):
            ctx.create_branch(f"txn_{index}", from_branch="main")

        before = ctx.db.execute(
            """
            SELECT child_count
            FROM _chronos_branch_interval_branches
            WHERE branch_id = 'main'
            """
        ).fetchone()
        assert before["child_count"] == 200

        ctx.delete_branch("txn_100")

        with pytest.raises(BranchNotFoundError):
            ctx.get_branch("txn_100")
        assert ctx.get_branch("txn_101").branch_id == "txn_101"
        after = ctx.db.execute(
            """
            SELECT child_count
            FROM _chronos_branch_interval_branches
            WHERE branch_id = 'main'
            """
        ).fetchone()
        assert after["child_count"] == 199
        ctx.wait_for_background_work()
    finally:
        ctx.close()


def test_interval_delete_branch_gc_removes_unreachable_writer_rows_and_segments(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 99},
        )
        agent_segment = int(ctx.get_branch("agent").current_ref)
        assert ctx.db.execute(
            """
            SELECT 1
            FROM _chronos_b_interval_products
            WHERE writer_segment_id = ?
            """,
            (agent_segment,),
        ).fetchone() is not None

        ctx.delete_branch("agent")
        ctx.wait_for_background_work()

        assert ctx.db.execute(
            """
            SELECT 1
            FROM _chronos_b_interval_products
            WHERE writer_segment_id = ?
            """,
            (agent_segment,),
        ).fetchone() is None
        assert ctx.db.execute(
            """
            SELECT 1
            FROM _chronos_branch_interval_segments
            WHERE segment_id = ?
            """,
            (agent_segment,),
        ).fetchone() is None
        assert _product(ctx.checkout("main"), "abc")["price"] == 10
    finally:
        ctx.close()


def test_interval_delete_branch_gc_keeps_checkpoint_visible_rows(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("agent", from_branch="main")
        agent = ctx.checkout("agent")
        agent.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"sku": "abc", "price": 99},
        )
        writer_segment = int(ctx.get_branch("agent").current_ref)
        ctx.create_checkpoint("agent-snap", branch="agent")

        ctx.delete_branch("agent")
        ctx.wait_for_background_work()

        assert _product(ctx.checkout_checkpoint("agent-snap"), "abc")["price"] == 99
        assert ctx.db.execute(
            """
            SELECT 1
            FROM _chronos_b_interval_products
            WHERE writer_segment_id = ?
            """,
            (writer_segment,),
        ).fetchone() is not None
    finally:
        ctx.close()


def test_duplicate_insert_and_duplicate_branch_errors(ctx: ChronosBranchContext) -> None:
    session = ctx.checkout("main")
    with pytest.raises(DuplicateKeyError):
        session.execute(
            "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
            {"sku": "abc", "name": "Other", "price": 1},
        )

    ctx.create_branch("exp", from_branch="main")
    with pytest.raises(BranchAlreadyExistsError):
        ctx.create_branch("exp", from_branch="main")


def test_arithmetic_update_expressions_are_branch_local(ctx: ChronosBranchContext) -> None:
    session = ctx.checkout("main")
    result = session.execute(
        """
        UPDATE products
        SET price = (price + :delta) * 2 - 5
        WHERE sku = :sku AND price >= :min_price
        """,
        {"delta": 3, "sku": "abc", "min_price": 5},
    )

    assert result.rowcount == 1
    assert _product(session, "abc")["price"] == 21

    ctx.create_branch("exp", from_branch="main")
    exp = ctx.checkout("exp")
    exp.execute(
        "UPDATE products SET price = price - :delta WHERE sku = :sku",
        {"delta": 4, "sku": "abc"},
    )

    assert _product(exp, "abc")["price"] == 17
    assert _product(session, "abc")["price"] == 21


def test_deep_branch_chain_has_isolated_leaf_state(ctx: ChronosBranchContext) -> None:
    parent = "main"
    for index in range(12):
        child = f"b{index}"
        ctx.create_branch(child, from_branch=parent)
        ctx.checkout(child).execute(
            "UPDATE nodes SET score = :score WHERE node_id = :node_id",
            {"score": index + 10, "node_id": "n1"},
        )
        parent = child

    assert ctx.checkout("b11").query(
        "SELECT score FROM nodes WHERE node_id = 'n1'"
    ) == [{"score": 21}]
    assert ctx.checkout("b5").query(
        "SELECT score FROM nodes WHERE node_id = 'n1'"
    ) == [{"score": 15}]
    assert ctx.checkout("main").query(
        "SELECT score FROM nodes WHERE node_id = 'n1'"
    ) == [{"score": 1}]


def test_interval_backend_supports_deep_spine_beyond_midpoint_limit(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    parent = "main"
    for index in range(100):
        child = f"deep_{index}"
        ctx.create_branch(child, from_branch=parent)
        if index in {0, 62, 99}:
            ctx.checkout(child).execute(
                "UPDATE products SET price = :price WHERE sku = :sku",
                {"price": index + 100, "sku": "abc"},
            )
        parent = child

    assert _product(ctx.checkout("deep_99"), "abc")["price"] == 199
    assert _product(ctx.checkout("deep_62"), "abc")["price"] == 162
    assert _product(ctx.checkout("deep_0"), "abc")["price"] == 100
    assert _product(ctx.checkout("main"), "abc")["price"] == 10
    ctx.close()


def test_parent_writes_after_child_fork_do_not_leak_to_child(
    ctx: ChronosBranchContext,
) -> None:
    ctx.create_branch("child", from_branch="main")
    child = ctx.checkout("child")
    main = ctx.checkout("main")

    main.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 99, "sku": "abc"},
    )

    assert _product(child, "abc")["price"] == 10
    assert _product(main, "abc")["price"] == 99


def test_interval_backend_splices_physical_rows_without_copying_whole_table(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    assert _physical_change_count(ctx, "products") == 2
    ctx.create_branch("exp", from_branch="main")
    ctx.create_branch("child", from_branch="exp")
    assert _physical_change_count(ctx, "products") == 2
    ctx.checkout("exp").execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )

    physical_rows = ctx.db.execute(
        """
        SELECT sku, price, live_lo, live_hi, deleted
        FROM _chronos_b_interval_products
        WHERE sku = 'abc'
        ORDER BY live_lo
        """
    ).fetchall()
    assert len(physical_rows) == 3
    assert [row["price"] for row in physical_rows] == [10, 15, 10]
    assert all(not row["deleted"] for row in physical_rows)
    assert _physical_change_count(ctx, "products") == 4
    ctx.close()


def test_interval_segment_ids_are_integers(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("exp", from_branch="main")
        rows = ctx.db.execute(
            """
            SELECT segment_id, parent_segment_id
            FROM _chronos_branch_interval_segments
            ORDER BY segment_id
            """
        ).fetchall()
        assert rows
        assert all(isinstance(row["segment_id"], int) for row in rows)
        assert all(
            row["parent_segment_id"] is None
            or isinstance(row["parent_segment_id"], int)
            for row in rows
        )
        assert all(-(2**31) <= int(row["segment_id"]) < 2**31 for row in rows)
    finally:
        ctx.close()


def test_interval_writer_provenance_ignores_sibling_preservation_rows(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("b", from_branch="main")
        ctx.create_branch("sibling", from_branch="main")
        ctx.checkout("sibling").execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 31, "sku": "abc"},
        )

        b_segment_id = int(ctx.get_branch("b").current_ref)
        b_segment = ctx.db.execute(
            """
            SELECT live_lo, live_hi, branch_point
            FROM _chronos_branch_interval_segments
            WHERE segment_id = ?
            """,
            (b_segment_id,),
        ).fetchone()
        preservation = ctx.db.execute(
            """
            SELECT price, writer_segment_id
            FROM _chronos_b_interval_products
            WHERE sku = 'abc'
              AND live_lo <= ?
              AND ? < live_hi
              AND deleted = FALSE
            """,
            (b_segment["branch_point"], b_segment["branch_point"]),
        ).fetchone()

        assert preservation is not None
        assert preservation["price"] == 10
        assert int(preservation["writer_segment_id"]) != b_segment_id
        assert ctx.diff_rows("main", "b", "products") == []
        assert [(c.key, c.change, c.after) for c in ctx.diff_rows("main", "sibling", "products")] == [
            ({"sku": "abc"}, "modified", {"sku": "abc", "name": "Alpha", "price": 31})
        ]
    finally:
        ctx.close()


def test_interval_diff_uses_divergent_writer_segments_for_arbitrary_branches(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("left", from_branch="main")
        left = ctx.checkout("left")
        left.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 11, "sku": "abc"},
        )
        ctx.create_branch("left_child", from_branch="left")
        ctx.checkout("left_child").execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 22, "sku": "def"},
        )

        ctx.create_branch("right", from_branch="main")
        right = ctx.checkout("right")
        right.execute("DELETE FROM products WHERE sku = :sku", {"sku": "abc"})
        right.execute(
            "INSERT INTO products (sku, name, price) VALUES (:sku, :name, :price)",
            {"sku": "ghi", "name": "Gamma", "price": 7},
        )

        changes = ctx.diff_rows("left_child", "right", "products")
        assert [(c.key, c.change, c.before, c.after) for c in changes] == [
            (
                {"sku": "abc"},
                "deleted",
                {"sku": "abc", "name": "Alpha", "price": 11},
                None,
            ),
            (
                {"sku": "def"},
                "modified",
                {"sku": "def", "name": "Delta", "price": 22},
                {"sku": "def", "name": "Delta", "price": 20},
            ),
            (
                {"sku": "ghi"},
                "added",
                None,
                {"sku": "ghi", "name": "Gamma", "price": 7},
            ),
        ]
    finally:
        ctx.close()


def test_interval_diff_suppresses_update_revert_candidates(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 15, "sku": "abc"},
        )
        exp.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 10, "sku": "abc"},
        )

        rows = ctx.db.execute(
            """
            SELECT DISTINCT writer_segment_id
            FROM _chronos_b_interval_products
            WHERE sku = 'abc'
            """
        ).fetchall()
        assert int(ctx.get_branch("exp").current_ref) in {
            int(row["writer_segment_id"]) for row in rows
        }
        assert ctx.diff_rows("main", "exp", "products") == []
    finally:
        ctx.close()


def test_interval_diff_finds_branch_delete_tombstone(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        exp.execute("DELETE FROM products WHERE sku = :sku", {"sku": "def"})

        exp_segment_id = int(ctx.get_branch("exp").current_ref)
        tombstone = ctx.db.execute(
            """
            SELECT deleted, writer_segment_id
            FROM _chronos_b_interval_products
            WHERE sku = 'def'
              AND writer_segment_id = ?
            """,
            (exp_segment_id,),
        ).fetchone()
        assert tombstone is not None
        assert bool(tombstone["deleted"])
        assert [(c.key, c.change) for c in ctx.diff_rows("main", "exp", "products")] == [
            ({"sku": "def"}, "deleted")
        ]
    finally:
        ctx.close()


def test_postgres_interval_batch_update_assigns_writer_segments_for_diff() -> None:
    ctx = _make_products_only_context("postgres", "interval")
    try:
        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        assert exp.execute("UPDATE products SET price = price + 5").rowcount == 2

        exp_segment_id = int(ctx.get_branch("exp").current_ref)
        rows = ctx.db.execute(
            """
            SELECT sku, price, writer_segment_id
            FROM _chronos_b_interval_products
            WHERE writer_segment_id = ?
            ORDER BY sku
            """,
            (exp_segment_id,),
        ).fetchall()
        assert [(row["sku"], row["price"]) for row in rows] == [
            ("abc", 15),
            ("def", 25),
        ]
        assert [(c.key, c.change, c.after["price"]) for c in ctx.diff_rows("main", "exp", "products")] == [
            ({"sku": "abc"}, "modified", 15),
            ({"sku": "def"}, "modified", 25),
        ]
    finally:
        ctx.close()


def test_postgres_interval_large_sparse_diff_is_change_proportional() -> None:
    _reset_postgres_schema()
    ctx = ChronosBranchContext.connect(_postgres_dsn(), backend="interval")
    row_count = 50_000
    changed_ids = [0, 7, 103, 999, 5_001, 9_999, 20_000, 31_337, 42_000, 49_999]
    try:
        ctx.db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, payload TEXT, score INTEGER)")
        ctx.db.executemany(
            "INSERT INTO docs VALUES (?, ?, ?)",
            ((idx, f"doc-{idx}", idx % 17) for idx in range(row_count)),
        )
        ctx.db.commit()
        ctx.register_table("docs", ["id"])

        ctx.create_branch("exp", from_branch="main")
        exp = ctx.checkout("exp")
        for idx in changed_ids:
            exp.execute(
                "UPDATE docs SET score = score + 1000 WHERE id = :id",
                {"id": idx},
            )

        backend = ctx._backend  # type: ignore[attr-defined]
        start = time.perf_counter()
        fast_changes = ctx.diff_rows("main", "exp", "docs")
        fast_elapsed = time.perf_counter() - start

        start = time.perf_counter()
        snapshot_changes = backend._snapshot_diff_rows(  # type: ignore[attr-defined]
            "main",
            "exp",
            "docs",
            backend.table_meta_for_branch("main", "docs"),
        )
        snapshot_elapsed = time.perf_counter() - start

        assert {change.key["id"] for change in fast_changes} == set(changed_ids)
        assert [
            (change.key, change.change, change.before, change.after)
            for change in fast_changes
        ] == [
            (change.key, change.change, change.before, change.after)
            for change in snapshot_changes
        ]
        writer_rows = ctx.db.execute(
            """
            SELECT COUNT(*) AS count
            FROM _chronos_b_interval_docs
            WHERE writer_segment_id = ?
            """,
            (int(ctx.get_branch("exp").current_ref),),
        ).fetchone()
        assert int(writer_rows["count"]) == len(changed_ids)
        assert fast_elapsed < snapshot_elapsed
        assert snapshot_elapsed / max(fast_elapsed, 1e-9) >= 3
    finally:
        ctx.close()


def test_interval_session_reuses_prepared_segment_metadata(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    ctx.create_branch("exp", from_branch="main")

    session = ctx.checkout("exp")
    metadata_reads = 0
    original_execute = ctx.db.execute

    def counting_execute(sql, params=()):
        nonlocal metadata_reads
        if (
            "_chronos_branch_interval_branches" in sql
            or "_chronos_branch_interval_segments" in sql
        ) and sql.lstrip().upper().startswith("SELECT"):
            metadata_reads += 1
        return original_execute(sql, params)

    ctx.db.execute = counting_execute  # type: ignore[method-assign]
    for _ in range(5):
        assert session.query("SELECT price FROM products WHERE sku = 'abc'") == [
            {"price": 10}
        ]
    session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 11, "sku": "abc"},
    )
    assert session.query("SELECT price FROM products WHERE sku = 'abc'") == [
        {"price": 11}
    ]

    assert metadata_reads == 0
    ctx.close()


def test_interval_checkout_uses_current_ref_without_extra_branch_lookup(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    ctx.create_branch("exp", from_branch="main")
    metadata_reads = {"branch": 0, "segment": 0}
    original_execute = ctx.db.execute

    def counting_execute(sql, params=()):
        normalized = " ".join(str(sql).split())
        if (
            "_chronos_branch_interval_branches" in normalized
            and normalized.upper().startswith("SELECT")
        ):
            metadata_reads["branch"] += 1
        if (
            "_chronos_branch_interval_segments" in normalized
            and normalized.upper().startswith("SELECT")
        ):
            metadata_reads["segment"] += 1
        return original_execute(sql, params)

    ctx.db.execute = counting_execute  # type: ignore[method-assign]
    session = ctx.checkout("exp")

    assert metadata_reads == {"branch": 1, "segment": 1}
    assert _product(session, "abc")["price"] == 10
    ctx.close()


def test_interval_reuses_sql_parse_cache_across_checkouts(
    sql_backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("left", from_branch="main")
        ctx.create_branch("right", from_branch="main")

        from chronos_core.branching import _interval_backend

        original_parse_one = _interval_backend.sqlglot.parse_one
        parse_count = {"count": 0}

        def counting_parse_one(*args, **kwargs):
            parse_count["count"] += 1
            return original_parse_one(*args, **kwargs)

        monkeypatch.setattr(_interval_backend.sqlglot, "parse_one", counting_parse_one)
        query_sql = "SELECT price FROM products WHERE sku = :sku"
        update_sql = "UPDATE products SET price = :price WHERE sku = :sku"

        left = ctx.checkout("left")
        right = ctx.checkout("right")
        assert left.query(query_sql, {"sku": "abc"}) == [{"price": 10}]
        assert right.query(query_sql, {"sku": "abc"}) == [{"price": 10}]
        left.execute(update_sql, {"sku": "abc", "price": 11})
        right.execute(update_sql, {"sku": "def", "price": 22})

        # One parse for the SELECT, one parse for the visible replacement
        # subquery, and one parse for the UPDATE plan. The second checkout uses
        # the backend-level cache instead of paying per-branch sqlglot cost.
        assert parse_count["count"] == 3
    finally:
        ctx.close()


def test_interval_stale_session_refreshes_to_latest_segment_after_branching(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    main = ctx.checkout("main")
    ctx.create_branch("child", from_branch="main")

    main.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 55, "sku": "abc"},
    )

    assert _product(main, "abc")["price"] == 55
    assert _product(ctx.checkout("child"), "abc")["price"] == 10
    ctx.close()


def test_interval_session_caches_repeated_statement_parses(
    sql_backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    ctx.create_branch("exp", from_branch="main")
    session = ctx.checkout("exp")

    import chronos_core.branching._runtime as runtime

    parse_calls = 0
    original_parse_one = runtime.sqlglot.parse_one

    def counting_parse_one(*args, **kwargs):
        nonlocal parse_calls
        parse_calls += 1
        return original_parse_one(*args, **kwargs)

    monkeypatch.setattr(runtime.sqlglot, "parse_one", counting_parse_one)
    for price in range(5):
        session.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": price + 30, "sku": "abc"},
        )

    assert parse_calls == 1
    assert _product(session, "abc")["price"] == 34
    ctx.close()


def test_interval_multi_row_insert_batches_direct_physical_rows(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    ctx.create_branch("exp", from_branch="main")
    session = ctx.checkout("exp")
    executemany_calls = 0
    physical_insert_executes = 0
    original_execute = ctx.db.execute
    original_executemany = ctx.db.executemany

    def counting_execute(sql, params=()):
        nonlocal physical_insert_executes
        if (
            sql.lstrip().upper().startswith("INSERT INTO")
            and "_chronos_b_interval_products" in sql
        ):
            physical_insert_executes += 1
        return original_execute(sql, params)

    def counting_executemany(sql, params):
        nonlocal executemany_calls
        if "_chronos_b_interval_products" in sql:
            executemany_calls += 1
        return original_executemany(sql, params)

    ctx.db.execute = counting_execute  # type: ignore[method-assign]
    ctx.db.executemany = counting_executemany  # type: ignore[method-assign]
    result = session.execute(
        """
        INSERT INTO products (sku, name, price)
        VALUES ('ghi', 'Gamma', 30), ('jkl', 'Juliet', 40)
        """
    )

    assert result.rowcount == 2
    assert executemany_calls == 1
    assert physical_insert_executes == 0
    assert session.query("SELECT sku, name, price FROM products WHERE sku >= 'ghi' ORDER BY sku") == [
        {"sku": "ghi", "name": "Gamma", "price": 30},
        {"sku": "jkl", "name": "Juliet", "price": 40},
    ]
    assert ctx.checkout("main").query("SELECT sku FROM products WHERE sku = 'ghi'") == []
    ctx.close()


def test_interval_multi_row_insert_duplicate_detection(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    ctx.create_branch("exp", from_branch="main")
    session = ctx.checkout("exp")

    with pytest.raises(DuplicateKeyError):
        session.execute(
            """
            INSERT INTO products (sku, name, price)
            VALUES ('abc', 'Again', 99), ('ghi', 'Gamma', 30)
            """
        )
    with pytest.raises(DuplicateKeyError):
        session.execute(
            """
            INSERT INTO products (sku, name, price)
            VALUES ('dup', 'First', 30), ('dup', 'Second', 40)
            """
        )
    assert session.query("SELECT sku FROM products WHERE sku IN ('ghi', 'dup')") == []
    ctx.close()


def test_interval_multi_row_insert_splices_deleted_keys_and_batches_new_keys(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    ctx.create_branch("exp", from_branch="main")
    session = ctx.checkout("exp")
    session.execute("DELETE FROM products WHERE sku = 'abc'")

    result = session.execute(
        """
        INSERT INTO products (sku, name, price)
        VALUES ('abc', 'Alpha Reloaded', 15), ('ghi', 'Gamma', 30)
        """
    )

    assert result.rowcount == 2
    assert session.query(
        "SELECT sku, name, price FROM products WHERE sku IN ('abc', 'ghi') ORDER BY sku"
    ) == [
        {"sku": "abc", "name": "Alpha Reloaded", "price": 15},
        {"sku": "ghi", "name": "Gamma", "price": 30},
    ]
    assert _product(ctx.checkout("main"), "abc")["price"] == 10
    ctx.close()


def test_interval_update_fetches_matching_rows_in_batch(sql_backend: str) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    ctx.create_branch("exp", from_branch="main")
    session = ctx.checkout("exp")
    visible_row_selects = 0
    original_execute = ctx.db.execute

    def counting_execute(sql, params=()):
        nonlocal visible_row_selects
        normalized = " ".join(sql.split())
        if (
            normalized.upper().startswith("SELECT")
            and "_chronos_b_interval_products" in normalized
            and "AND ? < live_hi" in normalized
            and "deleted = FALSE" in normalized
        ):
            visible_row_selects += 1
        return original_execute(sql, params)

    ctx.db.execute = counting_execute  # type: ignore[method-assign]
    result = session.execute(
        "UPDATE products SET price = :price WHERE price >= :min_price",
        {"price": 77, "min_price": 10},
    )

    assert result.rowcount == 2
    assert visible_row_selects == 0
    assert session.query("SELECT sku, price FROM products ORDER BY sku") == [
        {"sku": "abc", "price": 77},
        {"sku": "def", "price": 77},
    ]
    assert ctx.checkout("main").query("SELECT sku, price FROM products ORDER BY sku") == [
        {"sku": "abc", "price": 10},
        {"sku": "def", "price": 20},
    ]
    ctx.close()


def test_context_autocommit_false_leaves_logical_write_uncommitted(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    ctx.autocommit = False
    session = ctx.checkout("main")

    session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 44, "sku": "abc"},
    )

    assert ctx.db.in_transaction
    assert _product(session, "abc")["price"] == 44
    ctx.db.rollback()
    assert _product(session, "abc")["price"] == 10
    ctx.close()


def test_interval_autocommit_rolls_back_failed_logical_write(
    sql_backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    ctx.create_branch("exp", from_branch="main")
    session = ctx.checkout("exp")
    backend = ctx._backend  # type: ignore[attr-defined]
    original_insert_physical_row = backend._insert_physical_row
    insert_calls = 0

    def failing_insert_physical_row(*args, **kwargs):
        nonlocal insert_calls
        insert_calls += 1
        if insert_calls > 1:
            raise RuntimeError("injected failure after partial interval splice")
        return original_insert_physical_row(*args, **kwargs)

    monkeypatch.setattr(backend, "_insert_physical_row", failing_insert_physical_row)

    with pytest.raises(RuntimeError):
        session.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 44, "sku": "abc"},
        )

    assert session.query("SELECT sku, price FROM products ORDER BY sku") == [
        {"sku": "abc", "price": 10},
        {"sku": "def", "price": 20},
    ]
    ctx.close()


class UniqueViolation(Exception):
    pass


def _interval_unique_violation() -> UniqueViolation:
    return UniqueViolation(
        'duplicate key value violates unique constraint "_chronos_b_interval_products_pkey"'
    )


def test_interval_autocommit_retries_logical_write_on_physical_unique_violation(
    sql_backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("exp", from_branch="main")
        session = ctx.checkout("exp")
        backend = ctx._backend  # type: ignore[attr-defined]
        original_execute = backend.execute
        calls = 0

        def flaky_execute(ref, sql, params):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise _interval_unique_violation()
            return original_execute(ref, sql, params)

        monkeypatch.setattr(backend, "execute", flaky_execute)

        result = session.execute(
            "UPDATE products SET price = :price WHERE sku = :sku",
            {"price": 44, "sku": "abc"},
        )

        assert result.rowcount == 1
        assert calls == 2
        assert _product(session, "abc")["price"] == 44
    finally:
        ctx.close()


def test_interval_explicit_transaction_does_not_retry_physical_unique_violation(
    sql_backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = _make_products_only_context(sql_backend, "interval")
    try:
        ctx.create_branch("exp", from_branch="main")
        session = ctx.checkout("exp")
        backend = ctx._backend  # type: ignore[attr-defined]
        original_execute = backend.execute
        calls = 0

        def flaky_execute(ref, sql, params):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise _interval_unique_violation()
            return original_execute(ref, sql, params)

        monkeypatch.setattr(backend, "execute", flaky_execute)

        with pytest.raises(UniqueViolation):
            with session.transaction():
                session.execute(
                    "UPDATE products SET price = :price WHERE sku = :sku",
                    {"price": 44, "sku": "abc"},
                )

        assert calls == 1
        assert _product(session, "abc")["price"] == 10
    finally:
        ctx.close()


def test_log_backend_branching_shares_log_prefix_and_appends_only_writes(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "log")
    assert _physical_change_count(ctx, "products") == 2
    ctx.create_branch("exp", from_branch="main")
    assert _physical_change_count(ctx, "products") == 2
    ctx.checkout("main").execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 99, "sku": "abc"},
    )
    ctx.checkout("exp").execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 15, "sku": "abc"},
    )

    assert _physical_change_count(ctx, "products") == 4
    assert _product(ctx.checkout("main"), "abc")["price"] == 99
    assert _product(ctx.checkout("exp"), "abc")["price"] == 15
    ctx.close()


def test_log_session_reuses_lineage_for_reads_and_refreshes_after_writes(
    sql_backend: str,
) -> None:
    ctx = _make_products_only_context(sql_backend, "log")
    ctx.create_branch("exp", from_branch="main")

    session = ctx.checkout("exp")
    branch_reads = 0
    original_execute = ctx.db.execute

    def counting_execute(sql, params=()):
        nonlocal branch_reads
        if "_chronos_branch_log_branches" in sql and sql.lstrip().upper().startswith("SELECT"):
            branch_reads += 1
        return original_execute(sql, params)

    ctx.db.execute = counting_execute  # type: ignore[method-assign]
    for _ in range(5):
        assert session.query("SELECT price FROM products WHERE sku = 'abc'") == [
            {"price": 10}
        ]
    assert branch_reads == 0

    session.execute(
        """
        INSERT INTO products (sku, name, price)
        VALUES (:sku, :name, :price)
        """,
        {"sku": "xyz", "name": "Xray", "price": 30},
    )
    assert session.query("SELECT price FROM products WHERE sku = 'xyz'") == [
        {"price": 30}
    ]
    assert branch_reads > 0
    ctx.close()
