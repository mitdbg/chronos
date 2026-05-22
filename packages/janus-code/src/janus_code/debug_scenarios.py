"""Deterministic debug scenarios for CLI integration testing.

These scenarios exercise Janus transaction semantics without relying on
nondeterministic LLM behavior. They are exposed through `/debug ...`.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import time

from janus_code.agent.dag import normalize_planner_dag
from janus_code.janus_integration.session_manager import SessionManager


class DebugScenarioRunner:
    """Run deterministic Janus scenarios for CLI verification."""

    def __init__(
        self,
        working_dir: Path,
        session: SessionManager,
    ) -> None:
        self.working_dir = working_dir
        self.session = session

    def run(self, args: list[str]) -> str:
        """Execute a debug scenario by name."""
        if not args or args[0].lower() in {"help", "list"}:
            return self._help_text()

        name = args[0].lower()
        if name == "subagent-speculative":
            return self._subagent_speculative()
        if name == "parallel-snapshot":
            return self._parallel_snapshot()
        if name == "parallel-db-conflict-retry":
            return self._parallel_db_conflict_retry()
        if name == "nested-dag-wave":
            return self._nested_dag_wave()

        return (
            f"Unknown debug scenario: {name}\n\n"
            f"{self._help_text()}"
        )

    def _help_text(self) -> str:
        return (
            "Debug scenarios:\n"
            "  /debug subagent-speculative      "
            "Subtxn rollback then successful retry\n"
            "  /debug parallel-snapshot         "
            "Consistent snapshot read isolation\n"
            "  /debug parallel-db-conflict-retry "
            "DB row write conflict + retry commit\n"
            "  /debug nested-dag-wave           "
            "Nested DAG executes in topological parallel waves"
        )

    def _subagent_speculative(self) -> str:
        """Exercise speculative subtransactions in a coding/debugging flow."""
        target = self.working_dir / "debug_calc.py"
        buggy = (
            "def add(a, b):\n"
            "    return a - b\n"
        )
        wrong_fix = (
            "def add(a, b):\n"
            "    return a * b\n"
        )
        correct_fix = (
            "def add(a, b):\n"
            "    return a + b\n"
        )
        target.write_text(buggy)

        txn = self.session.begin_txn()
        try:
            attempt_a = txn.begin_subtxn("attempt_a_wrong_fix")
            (self.session.janus_context.working_dir / "debug_calc.py").write_text(wrong_fix)
            attempt_a.abort()

            parent_after_abort = (
                self.session.janus_context.working_dir / "debug_calc.py"
            ).read_text()
            rollback_ok = parent_after_abort == buggy

            attempt_b = txn.begin_subtxn("attempt_b_correct_fix")
            (self.session.janus_context.working_dir / "debug_calc.py").write_text(correct_fix)
            attempt_b.commit()

            self.session.commit_txn(txn)
        except Exception:
            self.session.abort_txn(txn)
            raise

        final_text = target.read_text()
        final_ok = final_text == correct_fix
        return (
            "SPECULATIVE_OK "
            f"rollback_ok={rollback_ok} "
            f"final_ok={final_ok} "
            "file=debug_calc.py"
        )

    def _parallel_snapshot(self) -> str:
        """Show snapshot isolation across concurrent Janus transactions."""
        from langchain_janus import JanusContext

        target = self.working_dir / "debug_snapshot.txt"
        initial = "version=1\n"
        updated = "version=2\n"
        target.write_text(initial)

        janus_dir = self.working_dir / ".janus-code"
        janus_dir.mkdir(exist_ok=True)
        db_path = str(janus_dir / "debug_snapshot.db")

        reader = JanusContext(
            str(self.working_dir),
            db_path=db_path,
            enable_sqlite=True,
            enable_vectorstore=False,
        )
        writer = JanusContext(
            str(self.working_dir),
            db_path=db_path,
            enable_sqlite=True,
            enable_vectorstore=False,
        )
        fresh_reader = None
        try:
            reader.begin()
            writer.begin()

            seen_before = (reader.working_dir / "debug_snapshot.txt").read_text()

            (writer.working_dir / "debug_snapshot.txt").write_text(updated)
            writer.commit()

            seen_after_writer_commit = (
                reader.working_dir / "debug_snapshot.txt"
            ).read_text()
            reader.abort()

            fresh_reader = JanusContext(
                str(self.working_dir),
                db_path=db_path,
                enable_sqlite=True,
                enable_vectorstore=False,
            )
            fresh_reader.begin()
            seen_fresh = (fresh_reader.working_dir / "debug_snapshot.txt").read_text()
            fresh_reader.abort()

            ok = (
                seen_before == initial
                and seen_after_writer_commit == initial
                and seen_fresh == updated
            )
            return (
                "SNAPSHOT_ISOLATION_OK "
                f"consistent={ok} "
                f"before={seen_before.strip()} "
                f"after_same_txn={seen_after_writer_commit.strip()} "
                f"fresh={seen_fresh.strip()}"
            )
        finally:
            for ctx in (reader, writer, fresh_reader):
                if ctx is not None and ctx.is_active:
                    try:
                        ctx.abort()
                    except Exception:
                        pass

    def _parallel_db_conflict_retry(self) -> str:
        """Trigger a real SQLiteShim write conflict and retry in a new txn."""
        from janus_core.transaction.coordinator import TransactionCoordinator
        from janus_core.transaction.shim_sqlite import SQLiteShim, WriteConflictError

        janus_dir = self.working_dir / ".janus-code"
        janus_dir.mkdir(exist_ok=True)
        db_path = str(janus_dir / "debug_parallel_conflict.db")

        coordinator = TransactionCoordinator()
        shim = SQLiteShim(db_path)
        coordinator.register_shim(shim)
        shim.register_table(
            "agent_state",
            ["row_id TEXT", "value TEXT"],
            pk_column="row_id",
        )

        t1 = coordinator.begin()
        t2 = coordinator.begin()
        retries = 0
        conflict_detected = False

        shim.put(t1, "agent_state", {"row_id": "shared", "value": "agent-a"})
        try:
            shim.put(t2, "agent_state", {"row_id": "shared", "value": "agent-b"})
        except WriteConflictError:
            conflict_detected = True
            retries += 1
            coordinator.rollback(t2.id)
        else:
            coordinator.rollback(t2.id)
            raise RuntimeError("Expected SQLite write conflict was not raised.")

        coordinator.commit(t1.id)

        retry_txn = coordinator.begin()
        prior = shim.get(retry_txn, "agent_state", "shared")
        shim.put(
            retry_txn,
            "agent_state",
            {"row_id": "shared", "value": "agent-b-retry"},
        )
        coordinator.commit(retry_txn.id)

        verify_txn = coordinator.begin()
        final = shim.get(verify_txn, "agent_state", "shared")
        coordinator.rollback(verify_txn.id)

        return (
            "DB_CONFLICT_RETRY_OK "
            f"conflict_detected={conflict_detected} "
            f"retries={retries} "
            f"prior={prior.get('value')} "
            f"final={final.get('value')}"
        )

    def _nested_dag_wave(self) -> str:
        """Execute a nested DAG and assert topological wave behavior."""
        from langchain_janus import JanusContext

        plan = normalize_planner_dag(
            {
                "dag": {
                    "mode": "parallel",
                    "response_node_id": "final",
                    "nodes": [
                        {
                            "id": "prepare",
                            "agent_type": "explore",
                            "prompt": "prepare context",
                            "children_mode": "parallel",
                            "children": [
                                {
                                    "id": "left",
                                    "agent_type": "implement",
                                    "prompt": "left branch",
                                },
                                {
                                    "id": "right",
                                    "agent_type": "implement",
                                    "prompt": "right branch",
                                },
                            ],
                        },
                        {
                            "id": "final",
                            "agent_type": "test",
                            "prompt": "finalize summary",
                            "depends_on": ["left", "right"],
                        },
                    ],
                }
            },
            user_input="debug nested dag wave",
        )

        debug_dir = self.working_dir / "debug_nested_dag"
        debug_dir.mkdir(parents=True, exist_ok=True)

        node_starts: dict[str, float] = {}
        node_ends: dict[str, float] = {}
        wave_sizes: list[int] = []
        wave_parallel_ok = True

        def _run_node(node_id: str) -> str:
            db_path = self.working_dir / ".janus-code" / f"debug_node_{node_id}.db"
            ctx = JanusContext(
                str(self.working_dir),
                db_path=str(db_path),
                enable_sqlite=True,
                enable_vectorstore=False,
            )
            worker_session = SessionManager(
                project_path=self.working_dir,
                config=self.session.config,
                janus_context=ctx,
                db_path=":memory:",
            )
            worker_session.start_session()
            txn = worker_session.begin_txn()
            try:
                node_starts[node_id] = time.perf_counter()
                workdir = ctx.working_dir
                (workdir / "debug_nested_dag").mkdir(parents=True, exist_ok=True)
                (workdir / "debug_nested_dag" / f"{node_id}.txt").write_text(
                    f"{node_id}\n"
                )
                time.sleep(0.15)
                worker_session.commit_txn(txn)
                node_ends[node_id] = time.perf_counter()
                return node_id
            except Exception:
                if txn.is_active:
                    worker_session.abort_txn(txn)
                raise

        for level in plan.topo_levels:
            wave_sizes.append(len(level))
            if len(level) == 1:
                _run_node(level[0])
                continue
            with ThreadPoolExecutor(max_workers=len(level)) as pool:
                futures = [pool.submit(_run_node, node_id) for node_id in level]
                for f in futures:
                    f.result()
            starts = [node_starts[node_id] for node_id in level]
            if (max(starts) - min(starts)) > 0.12:
                wave_parallel_ok = False

        topo_ok = True
        for node_id, node in plan.nodes_by_id.items():
            for dep in node.depends_on:
                if node_starts.get(node_id, 0.0) >= node_ends.get(dep, 0.0):
                    continue
                topo_ok = False

        expected_files_ok = all(
            (debug_dir / f"{node_id}.txt").exists()
            for node_id in plan.nodes_by_id
        )
        final = (debug_dir / "final.txt").read_text().strip() if (debug_dir / "final.txt").exists() else ""
        ok = (
            wave_sizes == [1, 2, 1]
            and wave_parallel_ok
            and topo_ok
            and expected_files_ok
            and final == "final"
        )
        return (
            "NESTED_DAG_WAVE_OK "
            f"ok={ok} "
            f"waves={wave_sizes} "
            f"parallel={wave_parallel_ok} "
            f"topo={topo_ok} "
            f"final={final}"
        )
