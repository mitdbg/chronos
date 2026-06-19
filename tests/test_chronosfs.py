from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
import time
import asyncio
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from chronos_core.branching import ChronosBranchContext
from chronos_core.workspace.chronosfs import (
    ChronosFSError,
    ChronosFSStore,
    ChronosFuseOperations,
)


@pytest.fixture
def chronosfs() -> ChronosFSStore:
    ctx = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
    store = ChronosFSStore(ctx, block_size=8)
    store.ensure()
    try:
        yield store
    finally:
        store.close()


def test_direct_file_operations_are_branch_isolated(chronosfs: ChronosFSStore) -> None:
    chronosfs.mkdir("main", "/notes")
    chronosfs.write_file("main", "/notes/plan.txt", "main plan\n")
    chronosfs.create_branch("agent", from_branch="main")

    chronosfs.write_at("agent", "/notes/plan.txt", len("main "), "branch")
    chronosfs.write_file("agent", "/notes/agent.txt", b"private\n")

    assert chronosfs.read_text("main", "/notes/plan.txt") == "main plan\n"
    assert not chronosfs.exists("main", "/notes/agent.txt")
    assert chronosfs.read_text("agent", "/notes/plan.txt") == "main branch"
    assert chronosfs.read_text("agent", "/notes/agent.txt") == "private\n"

    main = chronosfs.manifest("main")
    agent = chronosfs.manifest("agent")
    assert "notes/plan.txt" in main
    assert "notes/agent.txt" not in main
    assert "notes/agent.txt" in agent


def test_fixed_blocks_use_chronos_interval_cow(chronosfs: ChronosFSStore) -> None:
    chronosfs.write_file("main", "/blob.bin", b"aaaaaaaabbbbbbbbcccccccc")
    chronosfs.create_branch("child", from_branch="main")

    chronosfs.write_at("child", "/blob.bin", 10, b"XX")

    assert chronosfs.read_file("main", "/blob.bin") == b"aaaaaaaabbbbbbbbcccccccc"
    assert chronosfs.read_file("child", "/blob.bin") == b"aaaaaaaabbXXbbbbcccccccc"

    rows = chronosfs.context.db.execute(
        """
        SELECT block_index, count(*) AS c
        FROM _chronos_b_interval_chronosfs_file_blocks
        GROUP BY block_index
        ORDER BY block_index
        """
    ).fetchall()
    counts = {int(row["block_index"]): int(row["c"]) for row in rows}
    assert counts[0] == 1
    assert counts[1] > counts[0]
    assert counts[2] == 1


def test_write_at_reads_touched_blocks_in_one_range_query(
    chronosfs: ChronosFSStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chronosfs.write_file("main", "/blob.bin", b"aaaaaaaabbbbbbbbcccccccc")

    def fail_single_block_read(*args: object) -> None:
        raise AssertionError("write_at should use the batched block reader")

    monkeypatch.setattr(chronosfs, "_read_block", fail_single_block_read)
    monkeypatch.setattr(chronosfs, "_visible_block_length", fail_single_block_read)

    chronosfs.write_at("main", "/blob.bin", 6, b"XXYYZZQQ")

    assert chronosfs.read_file("main", "/blob.bin") == b"aaaaaaXXYYZZQQbbcccccccc"


def test_inode_allocation_rolls_back_with_failed_write(
    chronosfs: ChronosFSStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def next_inode_id() -> int:
        row = chronosfs.context.db.execute(
            "SELECT next_inode_id FROM _chronosfs_inode_allocator WHERE id = 1"
        ).fetchone()
        return int(row["next_inode_id"])

    before = next_inode_id()

    def fail_replace_blocks(*args: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(chronosfs, "_replace_blocks", fail_replace_blocks)

    with pytest.raises(RuntimeError, match="boom"):
        chronosfs.write_file("main", "/failed.bin", b"not committed")

    assert next_inode_id() == before
    assert not chronosfs.exists("main", "/failed.bin")


def test_truncate_and_sparse_reads(chronosfs: ChronosFSStore) -> None:
    chronosfs.write_file("main", "/sparse.bin", b"abc")
    chronosfs.truncate("main", "/sparse.bin", 20)
    assert chronosfs.read_file("main", "/sparse.bin") == b"abc" + (b"\x00" * 17)

    chronosfs.write_at("main", "/sparse.bin", 12, b"XYZ")
    data = chronosfs.read_file("main", "/sparse.bin")
    assert len(data) == 20
    assert data[:3] == b"abc"
    assert data[3:12] == b"\x00" * 9
    assert data[12:15] == b"XYZ"

    chronosfs.truncate("main", "/sparse.bin", 2)
    assert chronosfs.read_file("main", "/sparse.bin") == b"ab"


def test_inode_range_read_only_queries_overlapping_blocks(
    chronosfs: ChronosFSStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chronosfs.write_file("main", "/range.bin", b"aaaaaaaabbbbbbbbcccccccc")
    inode_id = chronosfs.stat("main", "/range.bin").inode_id
    real_session = chronosfs._read_session("main")
    queries: list[tuple[str, dict[str, object] | None]] = []

    class RecordingSession:
        def query(self, sql: str, params: dict[str, object] | None = None):
            queries.append((sql, params))
            return real_session.query(sql, params)

        def __getattr__(self, name: str):
            return getattr(real_session, name)

    monkeypatch.setattr(chronosfs, "_read_session", lambda branch_id: RecordingSession())

    assert chronosfs.read_inode_range("main", inode_id, 9, 5) == b"bbbbb"

    block_queries = [
        params
        for sql, params in queries
        if "FROM chronosfs_file_blocks" in sql
    ]
    assert block_queries == [{"inode": inode_id, "first": 1, "last": 1}]


def test_inode_range_read_handles_sparse_and_cross_block_reads(
    chronosfs: ChronosFSStore,
) -> None:
    chronosfs.write_file("main", "/range-sparse.bin", b"abc")
    chronosfs.truncate("main", "/range-sparse.bin", 20)
    chronosfs.write_at("main", "/range-sparse.bin", 12, b"XYZ")
    inode_id = chronosfs.stat("main", "/range-sparse.bin").inode_id

    assert chronosfs.read_inode_range("main", inode_id, 2, 14) == (
        b"c" + (b"\x00" * 9) + b"XYZ" + b"\x00"
    )
    assert chronosfs.read_inode_range("main", inode_id, 20, 10) == b""


def test_fuse_read_dispatches_to_inode_range(
    chronosfs: ChronosFSStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyfuse3")
    chronosfs.write_file("main", "/file.txt", b"0123456789")
    inode_id = chronosfs.stat("main", "/file.txt").inode_id
    ops = ChronosFuseOperations(chronosfs, branch_id="main")
    fh = ops._new_handle(inode_id)
    calls: list[tuple[int, int, int]] = []
    original_read_range = chronosfs._read_file_range_by_inode

    def read_range(session, inode: int, start: int, end: int) -> bytes:
        calls.append((inode, start, end))
        return original_read_range(session, inode, start, end)

    monkeypatch.setattr(chronosfs, "_read_file_range_by_inode", read_range)

    assert asyncio.run(ops.read(fh, 3, 3)) == b"345"
    assert calls == [(inode_id, 3, 6)]


def test_fuse_readdir_batches_child_metadata(
    chronosfs: ChronosFSStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyfuse3")
    chronosfs.mkdir("main", "/dir")
    chronosfs.write_file("main", "/dir/a.txt", b"a")
    chronosfs.write_file("main", "/dir/b.txt", b"b")
    inode_id = chronosfs.stat("main", "/dir").inode_id
    ops = ChronosFuseOperations(chronosfs, branch_id="main")

    def fail_lookup_child(*args: object) -> None:
        raise AssertionError("readdir should use batched child metadata")

    monkeypatch.setattr(ops, "_lookup_child", fail_lookup_child)

    entries = ops._regular_readdir(inode_id)
    assert [name for name, _attr in entries] == [".", "..", "a.txt", "b.txt"]


def test_checkpoint_and_restore_are_stable(chronosfs: ChronosFSStore) -> None:
    chronosfs.write_file("main", "/plan.txt", "base\n")
    chronosfs.create_branch("work", from_branch="main")
    chronosfs.write_file("work", "/plan.txt", "checkpoint\n")
    chronosfs.write_file("work", "/only-at-checkpoint.txt", "yes\n")
    chronosfs.create_checkpoint("snap", branch="work")

    chronosfs.write_file("work", "/plan.txt", "later\n")
    chronosfs.unlink("work", "/only-at-checkpoint.txt")
    chronosfs.create_branch_from_checkpoint("restored", "snap")

    assert chronosfs.read_text("restored", "/plan.txt") == "checkpoint\n"
    assert chronosfs.read_text("restored", "/only-at-checkpoint.txt") == "yes\n"
    assert chronosfs.read_text("work", "/plan.txt") == "later\n"
    assert not chronosfs.exists("work", "/only-at-checkpoint.txt")


def test_rename_symlink_and_merge_apply(chronosfs: ChronosFSStore) -> None:
    chronosfs.write_file("main", "/target.txt", "main target\n")
    chronosfs.mkdir("main", "/dir")
    chronosfs.write_file("main", "/dir/nested.txt", "nested\n")
    chronosfs.create_branch("source", from_branch="main")

    chronosfs.rename("source", "/target.txt", "/renamed.txt")
    chronosfs.symlink("source", "dir/nested.txt", "/link.txt")
    chronosfs.write_file("source", "/dir/nested.txt", "changed\n")

    diff = chronosfs.diff("main", "source")
    assert {"target.txt", "renamed.txt", "link.txt", "dir/nested.txt"} <= {
        change.path for change in diff.changes
    }

    result = chronosfs.merge_apply("source", "main")
    assert result.applied >= 4
    assert not chronosfs.exists("main", "/target.txt")
    assert chronosfs.read_text("main", "/renamed.txt") == "main target\n"
    assert chronosfs.readlink("main", "/link.txt") == "dir/nested.txt"
    assert chronosfs.read_text("main", "/link.txt") == "changed\n"


def test_errors_for_reserved_and_invalid_paths(chronosfs: ChronosFSStore) -> None:
    with pytest.raises(ChronosFSError):
        chronosfs.write_file("main", ".chronos/current", "bad")
    with pytest.raises(ChronosFSError):
        chronosfs.write_file("main", "../escape", "bad")


def test_postgres_direct_file_blocks_are_branch_isolated() -> None:
    dsn = _reset_and_get_postgres_dsn()
    store = ChronosFSStore.connect(dsn, backend="interval", block_size=8)
    store.ensure()
    try:
        store.write_file("main", "/blob.bin", b"aaaaaaaabbbbbbbbcccccccc")
        store.create_branch("agent", from_branch="main")
        store.write_at("agent", "/blob.bin", 10, b"XX")

        assert store.read_file("main", "/blob.bin") == b"aaaaaaaabbbbbbbbcccccccc"
        assert store.read_file("agent", "/blob.bin") == b"aaaaaaaabbXXbbbbcccccccc"

        rows = store.context.db.execute(
            """
            SELECT block_index, count(*) AS c
            FROM _chronos_b_interval_chronosfs_file_blocks
            GROUP BY block_index
            ORDER BY block_index
            """
        ).fetchall()
        counts = {int(row["block_index"]): int(row["c"]) for row in rows}
        assert counts[0] == 1
        assert counts[1] > counts[0]
        assert counts[2] == 1
    finally:
        store.close()


def test_postgres_fuse_mount_supports_normal_bash_tools(tmp_path: Path) -> None:
    _require_fuse_tools()
    dsn = _reset_and_get_postgres_dsn()
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs_database(dsn, mountpoint, script_dir=tmp_path):
        result = _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "mkdir -p pg",
                    "printf 'postgres fuse\\n' > pg/a.txt",
                    "cp pg/a.txt pg/b.txt",
                    "truncate -s 12288 pg/blob.bin",
                    "printf CHRONOS | dd of=pg/blob.bin bs=1 seek=4097 conv=notrunc status=none",
                    "cat pg/b.txt",
                ]
            ),
        )
        assert result.stdout == "postgres fuse\n"
        assert (mountpoint / "pg" / "blob.bin").read_bytes()[4097:4104] == b"CHRONOS"

    store = ChronosFSStore.connect(dsn, backend="interval")
    store.ensure()
    try:
        assert store.read_text("main", "/pg/b.txt") == "postgres fuse\n"
        data = store.read_file("main", "/pg/blob.bin")
        assert len(data) == 12288
        assert data[4097:4104] == b"CHRONOS"
    finally:
        store.close()


def test_fuse_mount_supports_normal_bash_tools(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        result = _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "mkdir -p notes",
                    "printf 'hello from bash\\n' > notes/a.txt",
                    "cp notes/a.txt notes/b.txt",
                    "mv notes/b.txt notes/c.txt",
                    "rm notes/a.txt",
                    "truncate -s 20 sparse.bin",
                    "printf XYZ | dd of=sparse.bin bs=1 seek=5 conv=notrunc status=none",
                    "ln -s notes/c.txt link.txt",
                    "cat notes/c.txt",
                ]
            ),
        )
        assert result.stdout == "hello from bash\n"
        assert (mountpoint / "link.txt").read_text() == "hello from bash\n"

    store = _open_store(db_path)
    try:
        assert not store.exists("main", "/notes/a.txt")
        assert store.read_text("main", "/notes/c.txt") == "hello from bash\n"
        sparse = store.read_file("main", "/sparse.bin")
        assert len(sparse) == 20
        assert sparse[5:8] == b"XYZ"
        assert store.readlink("main", "/link.txt") == "notes/c.txt"
    finally:
        store.close()


def test_fuse_control_paths_create_and_checkout_branch(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "mkdir .chronos/branches/agent",
                    "printf 'agent\\n' > .chronos/current",
                    "test \"$(cat .chronos/current)\" = agent",
                    "printf 'branch local\\n' > branch.txt",
                    "cat branch.txt",
                ]
            ),
        )

    store = _open_store(db_path)
    try:
        assert "agent" in store.branches
        assert store.read_text("agent", "/branch.txt") == "branch local\n"
        assert not store.exists("main", "/branch.txt")
    finally:
        store.close()


def test_fuse_large_scale_blocks_and_unix_tools(tmp_path: Path) -> None:
    _require_fuse_tools()
    db_path = tmp_path / "chronosfs.sqlite"
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    with _mounted_chronosfs(db_path, mountpoint):
        result = _run_bash(
            mountpoint,
            "\n".join(
                [
                    "set -euo pipefail",
                    "mkdir -p tree",
                    "for i in $(seq 1 200); do",
                    "  mkdir -p tree/dir_$((i % 10))",
                    "  printf 'file-%03d:%090d\\n' \"$i\" \"$i\" > tree/dir_$((i % 10))/file_$i.txt",
                    "done",
                    "dd if=/dev/urandom of=big.bin bs=1M count=2 status=none",
                    "sha256sum big.bin | awk '{print $1}'",
                    "find tree -type f | wc -l",
                ]
            ),
            timeout=60,
        )
        lines = result.stdout.splitlines()
        digest = lines[0]
        assert lines[1] == "200"
        result = _run_bash(
            mountpoint,
            "set -euo pipefail\nsha256sum big.bin | awk '{print $1}'\nwc -c < big.bin",
            timeout=60,
        )
        assert result.stdout.splitlines() == [digest, str(2 * 1024 * 1024)]

    store = _open_store(db_path)
    try:
        assert len(store.read_file("main", "/big.bin")) == 2 * 1024 * 1024
        assert len([path for path in store.manifest("main") if path.startswith("tree/")]) >= 210
    finally:
        store.close()


def _require_fuse_tools() -> None:
    missing = [
        name
        for name in ("bash", "mountpoint", "fusermount3")
        if shutil.which(name) is None
    ]
    if missing:
        pytest.skip(f"missing FUSE test tools: {', '.join(missing)}")
    if not Path("/dev/fuse").exists():
        pytest.skip("/dev/fuse is not available")
    pytest.importorskip("pyfuse3")
    pytest.importorskip("trio")


@contextmanager
def _mounted_chronosfs(db_path: Path, mountpoint: Path) -> Iterator[None]:
    database_url = "sqlite:///" + str(db_path)
    with _mounted_chronosfs_database(database_url, mountpoint, script_dir=db_path.parent):
        yield


@contextmanager
def _mounted_chronosfs_database(
    database_url: str,
    mountpoint: Path,
    *,
    script_dir: Path,
) -> Iterator[None]:
    script = script_dir / "mount_chronosfs.py"
    script.write_text(
        textwrap.dedent(
            f"""
            from chronos_core.workspace.chronosfs import ChronosFSStore, mount_chronosfs

            store = ChronosFSStore.connect({database_url!r}, backend="interval")
            store.ensure()
            try:
                mount_chronosfs(store, {str(mountpoint)!r})
            finally:
                store.close()
            """
        )
    )
    env = os.environ.copy()
    src = Path(__file__).resolve().parents[1] / "packages" / "chronos-core" / "src"
    tests = Path(__file__).resolve().parents[0]
    env["PYTHONPATH"] = os.pathsep.join(
        [str(src), str(tests), env.get("PYTHONPATH", "")]
    )
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                stdout, stderr = proc.communicate()
                raise AssertionError(
                    f"ChronosFS mount exited early with {proc.returncode}\n"
                    f"stdout:\n{stdout}\nstderr:\n{stderr}"
                )
            probe = subprocess.run(
                ["mountpoint", "-q", str(mountpoint)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if probe.returncode == 0:
                break
            time.sleep(0.05)
        else:
            proc.terminate()
            stdout, stderr = proc.communicate(timeout=5)
            raise AssertionError(
                "timed out waiting for ChronosFS mount\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
            )
        yield
    finally:
        subprocess.run(
            ["fusermount3", "-u", str(mountpoint)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        stdout = proc.stdout.read() if proc.stdout is not None else ""
        stderr = proc.stderr.read() if proc.stderr is not None else ""
        assert proc.returncode == 0, f"mount process failed\nstdout:\n{stdout}\nstderr:\n{stderr}"


def _run_bash(mountpoint: Path, script: str, *, timeout: float = 20) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["bash", "-lc", script],
        cwd=mountpoint,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    assert result.returncode == 0, result.stderr
    return result


def _open_store(db_path: Path) -> ChronosFSStore:
    store = ChronosFSStore.connect(f"sqlite:///{db_path}", backend="interval")
    store.ensure()
    return store


def _reset_and_get_postgres_dsn() -> str:
    pytest.importorskip("psycopg")
    from test_branching import _postgres_dsn, _reset_postgres_schema

    _reset_postgres_schema()
    return _postgres_dsn()
