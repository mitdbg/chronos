"""Live transactional integration scenarios (no mocks).

These tests validate real Chronos transactional guarantees used by coding/debugging
flows: speculative execution and concurrency isolation.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from chronos_code.config import Config
from chronos_code.chronos_integration.session_manager import SessionManager


def _require_tar(chronos_context):
    if chronos_context is None:
        pytest.skip("ChronosContext unavailable")


class TestLiveSpeculativeExecution:
    def test_speculative_debug_attempt_abort_then_commit(
        self,
        chronos_context,
        tmp_workspace: Path,
    ) -> None:
        """First speculative attempt is aborted, second is committed."""
        _require_tar(chronos_context)

        config = Config()
        session = SessionManager(
            project_path=tmp_workspace,
            config=config,
            chronos_context=chronos_context,
            db_path=str(tmp_workspace / ".chronos-code" / "speculative.db"),
        )
        session.start_session()

        original = (tmp_workspace / "src" / "main.py").read_text()

        txn = session.begin_txn()

        # Hypothesis A (bad fix) in child savepoint -> abort.
        attempt_a = txn.begin_subtxn("hypothesis_a")
        a_workdir = chronos_context.working_dir
        (a_workdir / "src" / "main.py").write_text("BROKEN_SPECULATIVE_FIX\n")
        attempt_a.abort()

        # After abort, parent overlay should still show original contents.
        parent_after_abort = chronos_context.working_dir
        assert (parent_after_abort / "src" / "main.py").read_text() == original

        # Hypothesis B (good fix) in child savepoint -> commit.
        attempt_b = txn.begin_subtxn("hypothesis_b")
        b_workdir = chronos_context.working_dir
        good_content = original + "\n# speculative-fix-applied\n"
        (b_workdir / "src" / "main.py").write_text(good_content)
        attempt_b.commit()

        session.commit_txn(txn)

        final = (tmp_workspace / "src" / "main.py").read_text()
        assert "BROKEN_SPECULATIVE_FIX" not in final
        assert "# speculative-fix-applied" in final


class TestLiveConcurrencyIsolation:
    def test_two_concurrent_transactions_have_isolated_views(
        self,
        tmp_workspace: Path,
    ) -> None:
        """Uncommitted writes in one transaction are invisible to another."""
        langchain_chronos = pytest.importorskip("langchain_chronos")
        ChronosContext = langchain_chronos.ChronosContext

        state_dir = tmp_workspace / ".chronos-code"
        state_dir.mkdir(exist_ok=True)
        shared_db = str(state_dir / "concurrency.db")

        ctx_a = ChronosContext(
            str(tmp_workspace),
            db_path=shared_db,
            enable_sqlite=True,
            enable_vectorstore=False,
        )
        ctx_b = ChronosContext(
            str(tmp_workspace),
            db_path=shared_db,
            enable_sqlite=True,
            enable_vectorstore=False,
        )

        try:
            original = (tmp_workspace / "src" / "main.py").read_text()

            ctx_a.begin()
            ctx_b.begin()

            # Write only in txn A's overlay.
            (ctx_a.working_dir / "src" / "main.py").write_text(
                "def main():\n    print('txn-a')\n"
            )

            # Txn B should not observe A's uncommitted write.
            b_seen = (ctx_b.working_dir / "src" / "main.py").read_text()
            assert b_seen == original

            # Commit A; B remains on its own snapshot and should still see original.
            ctx_a.commit()
            b_seen_after_commit = (ctx_b.working_dir / "src" / "main.py").read_text()
            assert b_seen_after_commit == original

            # New transaction sees committed value from A.
            ctx_b.abort()
            ctx_c = ChronosContext(
                str(tmp_workspace),
                db_path=shared_db,
                enable_sqlite=True,
                enable_vectorstore=False,
            )
            try:
                ctx_c.begin()
                c_seen = (ctx_c.working_dir / "src" / "main.py").read_text()
                assert "txn-a" in c_seen
            finally:
                if ctx_c.is_active:
                    ctx_c.abort()
        finally:
            if ctx_a.is_active:
                ctx_a.abort()
            if ctx_b.is_active:
                ctx_b.abort()

    def test_weak_snapshot_keeps_snapshot_reads(
        self,
        tmp_workspace: Path,
    ) -> None:
        """Weak snapshot still preserves stable reads within a transaction."""
        langchain_chronos = pytest.importorskip("langchain_chronos")
        ChronosContext = langchain_chronos.ChronosContext

        state_dir = tmp_workspace / ".chronos-code"
        state_dir.mkdir(exist_ok=True)
        shared_db = str(state_dir / "weak_snapshot_reads.db")

        target = tmp_workspace / "src" / "main.py"
        original = target.read_text()

        reader = ChronosContext(
            str(tmp_workspace),
            db_path=shared_db,
            enable_sqlite=True,
            enable_vectorstore=False,
            weak_snapshot=True,
        )
        writer = ChronosContext(
            str(tmp_workspace),
            db_path=shared_db,
            enable_sqlite=True,
            enable_vectorstore=False,
            weak_snapshot=True,
        )
        try:
            reader.begin()
            writer.begin()

            seen_before = (reader.working_dir / "src" / "main.py").read_text()
            (writer.working_dir / "src" / "main.py").write_text(
                "def main():\n    print('weak-writer')\n"
            )
            writer.commit()

            # Reader should stay on its original snapshot.
            seen_after = (reader.working_dir / "src" / "main.py").read_text()
            assert seen_before == original
            assert seen_after == original
        finally:
            if reader.is_active:
                reader.abort()
            if writer.is_active:
                writer.abort()

    def test_weak_snapshot_allows_lost_update_on_same_file(
        self,
        tmp_workspace: Path,
    ) -> None:
        """Weak snapshot disables conflict detection, allowing last-writer-wins."""
        langchain_chronos = pytest.importorskip("langchain_chronos")
        ChronosContext = langchain_chronos.ChronosContext

        state_dir = tmp_workspace / ".chronos-code"
        state_dir.mkdir(exist_ok=True)
        shared_db = str(state_dir / "weak_snapshot_lost_update.db")

        target = tmp_workspace / "counter.txt"
        target.write_text("10\n")

        a = ChronosContext(
            str(tmp_workspace),
            db_path=shared_db,
            enable_sqlite=True,
            enable_vectorstore=False,
            weak_snapshot=True,
        )
        b = ChronosContext(
            str(tmp_workspace),
            db_path=shared_db,
            enable_sqlite=True,
            enable_vectorstore=False,
            weak_snapshot=True,
        )
        try:
            a.begin()
            b.begin()

            # Both read from their own starting snapshot (10).
            xa = int((a.working_dir / "counter.txt").read_text().strip())
            xb = int((b.working_dir / "counter.txt").read_text().strip())
            assert xa == 10 and xb == 10

            (a.working_dir / "counter.txt").write_text(f"{xa + 1}\n")
            (b.working_dir / "counter.txt").write_text(f"{xb + 3}\n")

            # With strong conflict detection, one commit would abort.
            # Weak snapshot mode should allow both commits.
            a.commit()
            b.commit()

            final_val = int(target.read_text().strip())
            # Last-writer-wins anomaly: final value is not additive (11 or 13).
            assert final_val in {11, 13}
        finally:
            if a.is_active:
                a.abort()
            if b.is_active:
                b.abort()

    def test_weak_snapshot_toggles_shim_conflict_controls(
        self,
        tmp_workspace: Path,
    ) -> None:
        """Weak mode should disable shim conflict checks while strong keeps them."""
        langchain_chronos = pytest.importorskip("langchain_chronos")
        ChronosContext = langchain_chronos.ChronosContext

        state_dir = tmp_workspace / ".chronos-code"
        state_dir.mkdir(exist_ok=True)
        strong_db = str(state_dir / "strong_controls.db")
        weak_db = str(state_dir / "weak_controls.db")

        strong_ctx = ChronosContext(
            str(tmp_workspace),
            db_path=strong_db,
            enable_sqlite=True,
            enable_vectorstore=False,
        )
        weak_ctx = ChronosContext(
            str(tmp_workspace),
            db_path=weak_db,
            enable_sqlite=True,
            enable_vectorstore=False,
            weak_snapshot=True,
        )
        assert strong_ctx.weak_snapshot is False
        assert weak_ctx.weak_snapshot is True
        assert strong_ctx.sqlite_shim is not None and weak_ctx.sqlite_shim is not None
        assert strong_ctx.sqlite_shim._enforce_write_locks is True
        assert weak_ctx.sqlite_shim._enforce_write_locks is False
        assert strong_ctx.shim._enable_conflict_detection is True
        assert weak_ctx.shim._enable_conflict_detection is False

    def test_weak_snapshot_allows_sqlite_lost_update(
        self,
        tmp_workspace: Path,
    ) -> None:
        """Weak mode allows same-key commits from concurrent SQLite txns."""
        langchain_chronos = pytest.importorskip("langchain_chronos")
        ChronosContext = langchain_chronos.ChronosContext

        state_dir = tmp_workspace / ".chronos-code"
        state_dir.mkdir(exist_ok=True)
        shared_db = str(state_dir / "weak_sqlite_lost_update.db")

        a = ChronosContext(
            str(tmp_workspace),
            db_path=shared_db,
            enable_sqlite=True,
            enable_vectorstore=False,
            weak_snapshot=True,
        )
        b = ChronosContext(
            str(tmp_workspace),
            db_path=shared_db,
            enable_sqlite=True,
            enable_vectorstore=False,
            weak_snapshot=True,
        )
        try:
            assert a.sqlite_shim is not None and b.sqlite_shim is not None
            for shim in (a.sqlite_shim, b.sqlite_shim):
                shim.register_table("counters", ["id TEXT", "value INTEGER"], pk_column="id")
            a.sqlite_shim.seed_data("counters", [{"id": "main", "value": 10}])

            a.begin()
            b.begin()
            assert a.txn is not None and b.txn is not None

            row_a = a.sqlite_shim.get(a.txn, "counters", "main")
            row_b = b.sqlite_shim.get(b.txn, "counters", "main")
            assert row_a is not None and row_b is not None
            assert int(row_a["value"]) == 10
            assert int(row_b["value"]) == 10

            a.sqlite_shim.put(a.txn, "counters", {"id": "main", "value": 11})
            b.sqlite_shim.put(b.txn, "counters", {"id": "main", "value": 13})

            a.commit()
            b.commit()

            c = ChronosContext(
                str(tmp_workspace),
                db_path=shared_db,
                enable_sqlite=True,
                enable_vectorstore=False,
                weak_snapshot=True,
            )
            try:
                assert c.sqlite_shim is not None
                c.sqlite_shim.register_table(
                    "counters", ["id TEXT", "value INTEGER"], pk_column="id"
                )
                c.begin()
                assert c.txn is not None
                final_row = c.sqlite_shim.get(c.txn, "counters", "main")
                assert final_row is not None
                final_val = int(final_row["value"])
                # Non-additive outcome under weak mode (last-writer-wins semantics).
                assert final_val in {11, 13}
            finally:
                if c.is_active:
                    c.abort()
        finally:
            if a.is_active:
                a.abort()
            if b.is_active:
                b.abort()
