"""Unit tests for DAG-first orchestrator behavior."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest

from janus_code.agent.orchestrator import Orchestrator
from janus_code.config import Config
from janus_code.janus_integration.session_manager import SessionManager


class MockTool:
    def __init__(self, name: str, response: str = "ok"):
        self.name = name
        self._response = response
        self.calls = []

    def run(self, **kwargs):
        self.calls.append(kwargs)
        return self._response


def make_llm(responses):
    idx = [0]

    def fn(msgs, schemas):
        if idx[0] >= len(responses):
            return {"content": "[end]", "tool_calls": None}
        r = responses[idx[0]]
        idx[0] += 1
        return r

    return fn


def _extract_contract(messages: list[dict[str, str]]) -> dict:
    user_messages = [m for m in messages if m.get("role") == "user"]
    assert user_messages, "worker messages must contain a user contract"
    payload = user_messages[-1]["content"]
    start = payload.find("{")
    end = payload.rfind("}")
    assert start >= 0 and end > start
    return json.loads(payload[start : end + 1])


@pytest.fixture
def ws(tmp_path: Path) -> Path:
    (tmp_path / "CLAUDE.md").write_text("# Test Project\n")
    return tmp_path


class TestOrchestrator:
    @pytest.mark.asyncio
    async def test_simple_process_without_session(self, ws: Path):
        llm = make_llm([{"content": "Hello!", "tool_calls": None}])
        orch = Orchestrator(config=Config(), working_dir=ws, tools={}, llm_fn=llm)
        result = await orch.process("Hi")
        assert result == "Hello!"

    @pytest.mark.asyncio
    async def test_process_with_tool_without_session(self, ws: Path):
        read_tool = MockTool("read_file", "file content")
        llm = make_llm(
            [
                {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "c1",
                            "function": {
                                "name": "read_file",
                                "arguments": '{"path":"x.py"}',
                            },
                        }
                    ],
                },
                {"content": "Read the file.", "tool_calls": None},
            ]
        )
        orch = Orchestrator(
            config=Config(),
            working_dir=ws,
            tools={"read_file": read_tool},
            llm_fn=llm,
        )
        result = await orch.process("Read x.py")
        assert result == "Read the file."
        assert len(read_tool.calls) == 1

    @pytest.mark.asyncio
    async def test_planner_prompt_is_dag_first(self, ws: Path):
        planner_payloads: list[str] = []

        def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                planner_payloads.append(last)
                return {"content": "{}", "tool_calls": None}
            return {"content": "done", "tool_calls": None}

        orch = Orchestrator(config=Config(), working_dir=ws, tools={}, llm_fn=llm_fn)
        session = SessionManager(project_path=ws, config=Config())
        session.start_session()

        await orch.process("do something", session=session)
        assert len(planner_payloads) == 1
        payload = planner_payloads[0]
        assert "DAG structure only" in payload
        assert '"depends_on"' in payload
        assert '"children"' in payload
        assert '"response_node_id"' in payload
        assert '"executable"' in payload

    @pytest.mark.asyncio
    async def test_two_shot_planner_applies_detail_prompts(self, ws: Path):
        calls = {"shot1": 0, "shot2": 0}
        worker_objectives: list[str] = []

        def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                calls["shot1"] += 1
                return {
                    "content": json.dumps(
                        {
                            "assessment": "structure",
                            "dag": {
                                "mode": "parallel",
                                "response_node_id": "final",
                                "nodes": [
                                    {"id": "a", "agent_type": "implement", "depends_on": []},
                                    {"id": "b", "agent_type": "implement", "depends_on": []},
                                    {
                                        "id": "final",
                                        "agent_type": "test",
                                        "depends_on": ["a", "b"],
                                    },
                                ],
                            },
                        }
                    ),
                    "tool_calls": None,
                }
            if "Planning refinement:" in str(last):
                calls["shot2"] += 1
                return {
                    "content": json.dumps(
                        {
                            "assessment": "detailed",
                            "node_details": [
                                {"id": "a", "prompt": "worker a detail"},
                                {"id": "b", "prompt": "worker b detail"},
                                {"id": "final", "prompt": "worker final detail"},
                            ],
                        }
                    ),
                    "tool_calls": None,
                }
            if "Execute this task contract exactly." in str(last):
                contract = _extract_contract(messages)
                worker_objectives.append(str(contract.get("objective", "")))
                return {"content": contract["objective"], "tool_calls": None}
            if "Semantically merge these results" in str(last):
                return {"content": "merged", "tool_calls": None}
            return {"content": "fallback", "tool_calls": None}

        config = Config()
        orch = Orchestrator(config=config, working_dir=ws, tools={}, llm_fn=llm_fn)
        session = SessionManager(project_path=ws, config=config)
        session.start_session()

        await orch.process("run two-shot planning", session=session)
        assert calls["shot1"] == 1
        assert calls["shot2"] == 1
        assert "worker final detail" in worker_objectives

    @pytest.mark.asyncio
    async def test_parallel_dag_uses_worker_contract_only_no_prompt_generation(self, ws: Path):
        seen = {"planner": 0, "worker": 0, "aggregator": 0, "prompt_gen": 0}
        worker_contracts: list[dict] = []

        def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                seen["planner"] += 1
                return {
                    "content": json.dumps(
                        {
                            "assessment": "independent",
                            "dag": {
                                "mode": "parallel",
                                "nodes": [
                                    {
                                        "id": "a",
                                        "agent_type": "implement",
                                        "prompt": "create foo.md",
                                        "allowed_tools": ["write_file", "read_file"],
                                    },
                                    {
                                        "id": "b",
                                        "agent_type": "implement",
                                        "prompt": "create bar.md",
                                        "allowed_tools": ["write_file", "read_file"],
                                    },
                                ],
                            },
                        }
                    ),
                    "tool_calls": None,
                }
            if messages and "You generate concise high-quality system prompts" in messages[0]["content"]:
                seen["prompt_gen"] += 1
                return {"content": "should not happen", "tool_calls": None}
            if "Semantically merge these results" in str(last):
                seen["aggregator"] += 1
                return {"content": "merged", "tool_calls": None}
            if "Execute this task contract exactly." in str(last):
                seen["worker"] += 1
                worker_contracts.append(_extract_contract(messages))
                return {"content": "worker complete", "tool_calls": None}
            return {"content": "fallback", "tool_calls": None}

        config = Config()
        orch = Orchestrator(config=config, working_dir=ws, tools={}, llm_fn=llm_fn)
        session = SessionManager(project_path=ws, config=config)
        session.start_session()
        result = await orch.process("create foo and bar", session=session)

        assert result == "merged"
        assert seen["planner"] == 1
        assert seen["worker"] == 2
        assert seen["aggregator"] == 1
        assert seen["prompt_gen"] == 0
        assert all("upstream_results" in c for c in worker_contracts)
        assert all("node_id" in c for c in worker_contracts)
        assert all("allowed_tools" in c for c in worker_contracts)

    @pytest.mark.asyncio
    async def test_nested_dag_executes_topologically_and_parallel(self, ws: Path):
        started_at: dict[str, float] = {}
        finished_at: dict[str, float] = {}
        order: list[str] = []
        events: list[dict] = []

        async def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                return {
                    "content": json.dumps(
                        {
                            "assessment": "nested",
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
                                        "prompt": "finalize response",
                                        "depends_on": ["left", "right"],
                                    },
                                ],
                            },
                        }
                    ),
                    "tool_calls": None,
                }
            if "Execute this task contract exactly." in str(last):
                contract = _extract_contract(messages)
                node_id = contract["node_id"]
                started_at[node_id] = time.perf_counter()
                await asyncio.sleep(0.06)
                order.append(node_id)
                finished_at[node_id] = time.perf_counter()
                return {"content": f"{node_id} done", "tool_calls": None}
            return {"content": "fallback", "tool_calls": None}

        config = Config()
        orch = Orchestrator(
            config=config,
            working_dir=ws,
            tools={},
            llm_fn=llm_fn,
            event_sink=lambda event: events.append(dict(event)),
        )
        session = SessionManager(project_path=ws, config=config)
        session.start_session()

        result = await orch.process("run nested dag", session=session)
        assert result == "final done"

        assert "prepare" in order
        assert "left" in order
        assert "right" in order
        assert "final" in order
        assert finished_at["prepare"] <= started_at["left"]
        assert finished_at["prepare"] <= started_at["right"]
        assert finished_at["left"] <= started_at["final"]
        assert finished_at["right"] <= started_at["final"]
        wave_events = [
            e
            for e in events
            if e.get("type") == "parallel_start" and int(e.get("task_count", 0)) == 2
        ]
        assert wave_events, "expected at least one DAG wave with two concurrent nodes"

    @pytest.mark.asyncio
    async def test_cycle_in_planner_dag_falls_back_to_single_node(self, ws: Path):
        calls = {"planner": 0, "worker": 0}

        def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                calls["planner"] += 1
                return {
                    "content": json.dumps(
                        {
                            "assessment": "bad cycle",
                            "dag": {
                                "nodes": [
                                    {
                                        "id": "n1",
                                        "agent_type": "implement",
                                        "prompt": "a",
                                        "depends_on": ["n2"],
                                    },
                                    {
                                        "id": "n2",
                                        "agent_type": "implement",
                                        "prompt": "b",
                                        "depends_on": ["n1"],
                                    },
                                ]
                            },
                        }
                    ),
                    "tool_calls": None,
                }
            calls["worker"] += 1
            return {"content": "fallback worker node", "tool_calls": None}

        config = Config()
        orch = Orchestrator(config=config, working_dir=ws, tools={}, llm_fn=llm_fn)
        session = SessionManager(project_path=ws, config=config)
        session.start_session()

        result = await orch.process("handle request safely", session=session)
        assert result == "fallback worker node"
        assert calls["planner"] == 1
        assert calls["worker"] >= 1

    @pytest.mark.asyncio
    async def test_planner_receives_prior_history(self, ws: Path):
        planner_calls: list[list[dict[str, str]]] = []
        reply_idx = {"value": 0}

        def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                planner_calls.append(messages)
                return {"content": "{}", "tool_calls": None}
            reply_idx["value"] += 1
            return {"content": f"assistant reply {reply_idx['value']}", "tool_calls": None}

        config = Config()
        orch = Orchestrator(config=config, working_dir=ws, tools={}, llm_fn=llm_fn)
        session = SessionManager(project_path=ws, config=config)
        session.start_session()

        await orch.process("first request", session=session)
        await orch.process("second request", session=session)

        assert len(planner_calls) == 2
        second_planner_messages_json = json.dumps(planner_calls[1])
        assert "first request" in second_planner_messages_json
        assert "assistant reply" in second_planner_messages_json

    @pytest.mark.asyncio
    async def test_turn_usage_accumulates_tokens(self, ws: Path):
        def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                return {
                    "content": "{}",
                    "tool_calls": None,
                    "usage": {
                        "prompt_tokens": 100,
                        "completion_tokens": 10,
                        "total_tokens": 110,
                        "cached_prompt_tokens": 60,
                        "uncached_prompt_tokens": 40,
                    },
                }
            return {
                "content": "done",
                "tool_calls": None,
                "usage": {
                    "prompt_tokens": 70,
                    "completion_tokens": 20,
                    "total_tokens": 90,
                    "cached_prompt_tokens": 50,
                    "uncached_prompt_tokens": 20,
                },
            }

        config = Config()
        orch = Orchestrator(config=config, working_dir=ws, tools={}, llm_fn=llm_fn)
        session = SessionManager(project_path=ws, config=config)
        session.start_session()

        result = await orch.process("quick task", session=session)
        assert result == "done"
        usage = orch.get_last_turn_usage()
        assert usage["prompt_tokens"] == 240
        assert usage["completion_tokens"] == 50
        assert usage["total_tokens"] == 290
        assert usage["cached_prompt_tokens"] == 160
        assert usage["uncached_prompt_tokens"] == 80

    @pytest.mark.asyncio
    async def test_planner_json_repair_handles_unescaped_quotes(
        self, ws: Path
    ):
        seen_nodes: list[str] = []

        def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                # Invalid JSON due to unescaped inner quotes around v2.
                return {
                    "content": (
                        '{"assessment":"Bad JSON because of "v2" quotes",'
                        '"dag":{"mode":"parallel","response_node_id":"final",'
                        '"nodes":['
                        '{"id":"a","agent_type":"implement","prompt":"read snapshot.txt"},'
                        '{"id":"b","agent_type":"implement","prompt":"write "v2" to snapshot.txt"},'
                        '{"id":"final","agent_type":"test","prompt":"summarize","depends_on":["a","b"]}'
                        ']}}'
                    ),
                    "tool_calls": None,
                }
            if "Execute this task contract exactly." in str(last):
                contract = _extract_contract(messages)
                node_id = str(contract.get("node_id", ""))
                seen_nodes.append(node_id)
                return {"content": f"{node_id} done", "tool_calls": None}
            return {"content": "fallback", "tool_calls": None}

        config = Config()
        orch = Orchestrator(
            config=config,
            working_dir=ws,
            tools={},
            llm_fn=llm_fn,
        )
        session = SessionManager(project_path=ws, config=config)
        session.start_session()

        result = await orch.process("parallel task", session=session)
        assert result == "final done"
        assert set(seen_nodes) == {"a", "b", "final"}

    @pytest.mark.asyncio
    async def test_unrecoverable_planner_json_uses_generic_fallback_and_emits_debug(
        self, ws: Path
    ):
        events: list[dict] = []
        seen_nodes: list[str] = []

        def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                return {"content": "not json at all", "tool_calls": None}
            if "Execute this task contract exactly." in str(last):
                contract = _extract_contract(messages)
                node_id = str(contract.get("node_id", ""))
                seen_nodes.append(node_id)
                return {"content": f"{node_id} done", "tool_calls": None}
            return {"content": "fallback", "tool_calls": None}

        config = Config()
        orch = Orchestrator(
            config=config,
            working_dir=ws,
            tools={},
            llm_fn=llm_fn,
            event_sink=lambda event: events.append(dict(event)),
        )
        session = SessionManager(project_path=ws, config=config)
        session.start_session()

        result = await orch.process("do task", session=session)
        assert result == "n1 done"
        assert seen_nodes == ["n1"]
        invalid_events = [e for e in events if e.get("type") == "llm_plan_invalid"]
        assert invalid_events
        assert any(
            "candidate" in str(e.get("error", "")).lower()
            or "json" in str(e.get("error", "")).lower()
            for e in invalid_events
        )

    @pytest.mark.asyncio
    async def test_invalid_response_node_id_is_repaired_not_full_fallback(self, ws: Path):
        events: list[dict] = []
        seen_nodes: list[str] = []

        def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                return {
                    "content": json.dumps(
                        {
                            "assessment": "bad response node id",
                            "dag": {
                                "mode": "parallel",
                                "response_node_id": "report",
                                "nodes": [
                                    {"id": "a", "agent_type": "implement", "prompt": "task a"},
                                    {"id": "b", "agent_type": "implement", "prompt": "task b"},
                                ],
                            },
                        }
                    ),
                    "tool_calls": None,
                }
            if "Semantically merge these results" in str(last):
                return {"content": "merged", "tool_calls": None}
            if "Execute this task contract exactly." in str(last):
                node_id = str(_extract_contract(messages).get("node_id"))
                seen_nodes.append(node_id)
                return {"content": f"{node_id} done", "tool_calls": None}
            return {"content": "fallback", "tool_calls": None}

        config = Config()
        orch = Orchestrator(
            config=config,
            working_dir=ws,
            tools={},
            llm_fn=llm_fn,
            event_sink=lambda event: events.append(dict(event)),
        )
        session = SessionManager(project_path=ws, config=config)
        session.start_session()

        result = await orch.process("parallel tasks", session=session)
        assert result == "merged"
        assert set(seen_nodes) == {"a", "b"}
        assert any(e.get("type") == "llm_plan_repaired" for e in events)
        assert any(
            e.get("type") == "plan_dag"
            and "DAG Graph:" in str(e.get("preview", ""))
            and "sources:" in str(e.get("preview", ""))
            and "a" in str(e.get("preview", ""))
            and "b" in str(e.get("preview", ""))
            for e in events
        )

    @pytest.mark.asyncio
    async def test_plan_dag_event_preview_uses_non_markup_format(self, ws: Path):
        events: list[dict] = []

        def llm_fn(messages, _schemas):
            last = messages[-1]["content"] if messages else ""
            if "Planning task:" in str(last):
                return {
                    "content": json.dumps(
                        {
                            "assessment": "preview",
                            "dag": {
                                "mode": "parallel",
                                "response_node_id": "final",
                                "nodes": [
                                    {"id": "a", "agent_type": "implement", "prompt": "a"},
                                    {"id": "b", "agent_type": "implement", "prompt": "b"},
                                    {
                                        "id": "final",
                                        "agent_type": "test",
                                        "prompt": "final",
                                        "depends_on": ["a", "b"],
                                    },
                                ],
                            },
                        }
                    ),
                    "tool_calls": None,
                }
            if "Execute this task contract exactly." in str(last):
                node_id = str(_extract_contract(messages).get("node_id"))
                return {"content": f"{node_id} done", "tool_calls": None}
            return {"content": "fallback", "tool_calls": None}

        config = Config()
        orch = Orchestrator(
            config=config,
            working_dir=ws,
            tools={},
            llm_fn=llm_fn,
            event_sink=lambda event: events.append(dict(event)),
        )
        session = SessionManager(project_path=ws, config=config)
        session.start_session()

        await orch.process("preview dag", session=session)
        previews = [str(e.get("preview", "")) for e in events if e.get("type") == "plan_dag"]
        assert previews
        assert any(
            "DAG Graph:" in p
            and "a ---\\" in p
            and "b ---/" in p
            and "+---> final" in p
            and "response: final" in p
            for p in previews
        )
        assert all("[" not in p and "]" not in p for p in previews)
