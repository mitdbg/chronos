import sqlite3

import chronos_core._native_interval as native_interval


def _connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _create_blocks(conn):
    conn.execute(
        """
        CREATE TABLE blocks (
            inode_id INTEGER NOT NULL,
            block_index INTEGER NOT NULL,
            data BLOB,
            valid_length INTEGER,
            live_lo INTEGER NOT NULL,
            live_hi INTEGER NOT NULL,
            writer_segment_id INTEGER NOT NULL,
            deleted INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        "CREATE INDEX blocks_visible_idx ON blocks (inode_id, block_index, live_lo, live_hi)"
    )


def _rows(conn):
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT inode_id, block_index, data, valid_length, live_lo, live_hi, writer_segment_id, deleted
            FROM blocks
            ORDER BY inode_id, block_index, live_lo, live_hi
            """
        )
    ]


def test_native_connectors_smoke(tmp_path):
    db_path = tmp_path / "connector.sqlite"

    connector = native_interval.SQLiteConnector(str(db_path))
    connector.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
    connector.execute("INSERT INTO t VALUES (1, 'ok')")

    assert connector.query("SELECT id, value FROM t") == [{"id": 1, "value": "ok"}]
    assert native_interval.sqlite3_libversion()
    assert native_interval.libpq_version() > 0
    assert native_interval.recommended_sql_parser()["name"] == "libpg_query"
    assert native_interval.supports_connection_dialect("sqlite") is True
    assert native_interval.supports_connection_dialect("postgres") is False


def test_sqlite_interval_bulk_upsert_splits_overlapping_rows(tmp_path):
    db_path = tmp_path / "blocks.sqlite"
    with _connect(db_path) as conn:
        _create_blocks(conn)
        conn.execute(
            """
            INSERT INTO blocks
            VALUES (1, 0, ?, 3, 0, 100, 7, 0)
            """,
            (b"old",),
        )

    with _connect(db_path) as conn:
        stats = native_interval.interval_bulk_upsert_connection(
            "sqlite",
            conn,
            "blocks",
            ["inode_id", "block_index", "data", "valid_length"],
            ["inode_id", "block_index"],
            [{"inode_id": 1, "block_index": 0, "data": b"new", "valid_length": 3}],
            40,
            60,
            8,
            True,
        )

    with _connect(db_path) as conn:
        rows = _rows(conn)

    assert stats["strategy"] == "native_interval_bulk_upsert"
    assert stats["input_rows"] == 1
    assert stats["selected_rows"] == 1
    assert stats["deleted_rows"] == 1
    assert stats["inserted_rows"] == 3
    assert rows == [
        {
            "inode_id": 1,
            "block_index": 0,
            "data": b"old",
            "valid_length": 3,
            "live_lo": 0,
            "live_hi": 40,
            "writer_segment_id": 7,
            "deleted": 0,
        },
        {
            "inode_id": 1,
            "block_index": 0,
            "data": b"new",
            "valid_length": 3,
            "live_lo": 40,
            "live_hi": 60,
            "writer_segment_id": 8,
            "deleted": 0,
        },
        {
            "inode_id": 1,
            "block_index": 0,
            "data": b"old",
            "valid_length": 3,
            "live_lo": 60,
            "live_hi": 100,
            "writer_segment_id": 7,
            "deleted": 0,
        },
    ]


def test_sqlite_interval_bulk_upsert_batches_direct_and_overlapping_rows(tmp_path):
    db_path = tmp_path / "batch.sqlite"
    with _connect(db_path) as conn:
        _create_blocks(conn)
        conn.executemany(
            """
            INSERT INTO blocks
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            """,
            [
                (1, 0, b"parent-0", 8, 0, 100, 1),
                (1, 1, b"parent-1", 8, 0, 100, 1),
            ],
        )

    input_rows = [
        {"inode_id": 1, "block_index": 0, "data": b"child-0", "valid_length": 7},
        {"inode_id": 1, "block_index": 1, "data": b"child-1", "valid_length": 7},
    ]
    input_rows.extend(
        {
            "inode_id": 2,
            "block_index": idx,
            "data": f"new-{idx}".encode(),
            "valid_length": len(f"new-{idx}"),
        }
        for idx in range(64)
    )

    with _connect(db_path) as conn:
        stats = native_interval.interval_bulk_upsert_connection(
            "sqlite",
            conn,
            "blocks",
            ["inode_id", "block_index", "data", "valid_length"],
            ["inode_id", "block_index"],
            input_rows,
            25,
            75,
            2,
            True,
        )

    with _connect(db_path) as conn:
        row_count = conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
        visible_children = [
            dict(row)
            for row in conn.execute(
                """
                SELECT inode_id, block_index, data, live_lo, live_hi, writer_segment_id
                FROM blocks
                WHERE writer_segment_id = 2
                ORDER BY inode_id, block_index
                """
            )
        ]

    assert stats["input_rows"] == 66
    assert stats["selected_rows"] == 2
    assert stats["deleted_rows"] == 2
    assert stats["strategy"] == "native_interval_bulk_upsert"
    assert stats["inserted_rows"] == 70
    assert row_count == 70
    assert visible_children[:2] == [
        {
            "inode_id": 1,
            "block_index": 0,
            "data": b"child-0",
            "live_lo": 25,
            "live_hi": 75,
            "writer_segment_id": 2,
        },
        {
            "inode_id": 1,
            "block_index": 1,
            "data": b"child-1",
            "live_lo": 25,
            "live_hi": 75,
            "writer_segment_id": 2,
        },
    ]
    assert len(visible_children) == 66
