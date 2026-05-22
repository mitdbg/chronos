"""Unit tests for SessionManager."""

import re
import pytest
from pathlib import Path

from janus_code.config import Config
from janus_code.janus_integration.session_manager import SessionManager, TxnContext


@pytest.fixture
def session(tmp_path: Path) -> SessionManager:
    config = Config()
    sm = SessionManager(project_path=tmp_path, config=config)
    sm.start_session()
    return sm


class TestTxnContext:
    def test_create(self):
        ctx = TxnContext(txn_id="test-1")
        assert ctx.is_active
        assert ctx.txn_id == "test-1"

    def test_commit(self):
        ctx = TxnContext(txn_id="test-1")
        ctx.commit()
        assert not ctx.is_active

    def test_abort(self):
        ctx = TxnContext(txn_id="test-1")
        ctx.abort()
        assert not ctx.is_active

    def test_double_commit(self):
        ctx = TxnContext(txn_id="test-1")
        ctx.commit()
        ctx.commit()  # should be no-op
        assert not ctx.is_active

    def test_begin_subtxn(self):
        parent = TxnContext(txn_id="parent")
        child = parent.begin_subtxn("explore")
        assert child.is_active
        assert child.txn_id == "parent_1"

    def test_subtxn_commit(self):
        parent = TxnContext(txn_id="parent")
        child = parent.begin_subtxn("explore")
        child.commit()
        assert not child.is_active
        assert parent.is_active

    def test_subtxn_abort(self):
        parent = TxnContext(txn_id="parent")
        child = parent.begin_subtxn("explore")
        child.abort()
        assert not child.is_active
        assert parent.is_active

    def test_parent_commit_aborts_active_children(self):
        parent = TxnContext(txn_id="parent")
        child = parent.begin_subtxn("explore")
        parent.commit()
        assert not child.is_active
        assert not parent.is_active


class TestSessionManager:
    def test_start_session(self, session: SessionManager):
        assert session.session_id != ""
        assert session.turn_number == 0

    def test_begin_txn(self, session: SessionManager):
        txn = session.begin_txn()
        assert txn.is_active
        assert session.turn_number == 1
        assert session.current_txn is txn
        assert re.fullmatch(r"txn\d+", txn.txn_id)

    def test_commit_txn(self, session: SessionManager):
        txn = session.begin_txn()
        session.commit_txn(txn)
        assert not txn.is_active
        assert session.current_txn is None

    def test_abort_txn(self, session: SessionManager):
        txn = session.begin_txn()
        session.abort_txn(txn)
        assert not txn.is_active
        assert session.current_txn is None

    def test_multiple_txns(self, session: SessionManager):
        txn1 = session.begin_txn()
        session.commit_txn(txn1)
        txn2 = session.begin_txn()
        session.commit_txn(txn2)
        assert session.turn_number == 2
        n1 = int(txn1.txn_id.removeprefix("txn"))
        n2 = int(txn2.txn_id.removeprefix("txn"))
        assert n2 == n1 + 1

    def test_begin_parallel_txns(self, session: SessionManager):
        txns = session.begin_parallel_txns(3)
        assert len(txns) == 3
        for txn in txns:
            assert txn.is_active
            assert re.fullmatch(r"txn\d+", txn.txn_id)

    def test_subtxn_ids_are_parent_prefixed_and_sequential(self):
        parent = TxnContext(txn_id="txn99")
        child1 = parent.begin_subtxn("first")
        child2 = parent.begin_subtxn("second")
        assert child1.txn_id == "txn99_1"
        assert child2.txn_id == "txn99_2"

    def test_begin_aggregator_txn(self, session: SessionManager):
        txn = session.begin_aggregator_txn()
        assert txn.is_active
        assert txn.txn_id.startswith("txn")

    def test_get_status(self, session: SessionManager):
        status = session.get_status()
        assert status["session_id"] == session.session_id
        assert status["has_active_txn"] is False

        session.begin_txn()
        status = session.get_status()
        assert status["has_active_txn"] is True

    def test_persist_and_load_messages(self, session: SessionManager):
        from janus_code.context.conversation import Message

        msg = Message(role="user", content="hello", turn=1)
        session.persist_message(msg)

        loaded = session.load_messages()
        assert len(loaded) == 1
        assert loaded[0].content == "hello"

    def test_resume_session(self, session: SessionManager):
        from janus_code.context.conversation import Message

        msg = Message(role="user", content="test", turn=1)
        session.persist_message(msg)

        sid = session.session_id
        session2 = SessionManager(
            project_path=session.project_path,
            config=session.config,
            db_path=":memory:",
        )
        # Can't resume from memory db, but test the path
        session2.message_store = session.message_store
        resumed = session2.resume_session(sid)
        assert len(resumed) == 1
        assert session2.turn_number == 2
