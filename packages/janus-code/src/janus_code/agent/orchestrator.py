"""Orchestrator — top-level agent that processes user messages.

Decides between inline (single txn) and parallel (N txns + aggregator)
execution. Manages the full turn lifecycle.
"""

from __future__ import annotations

import json
import logging
import copy
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from janus_code.agent.agent_loop import AgentLoop
from janus_code.agent.external_workers import ExternalWorkerAdapter
from janus_code.agent.dag import (
    DagPlan as NormalizedDagPlan,
    DagValidationError,
    FlatDagNode,
    normalize_planner_dag,
)
from janus_code.agent.prompts import (
    ORCHESTRATOR_SYSTEM_PROMPT,
    PLANNER_DETAILS_SYSTEM_PROMPT,
    PLANNER_STRUCTURE_SYSTEM_PROMPT,
)
from janus_code.agent.sub_agents import (
    SubAgentConfig,
    filter_tools,
    get_sub_agent_config,
)
from janus_code.config import Config
from janus_code.context.context_manager import ContextManager
from janus_code.context.conversation import Conversation, Message, ToolCall
from janus_code.context.memory_loader import MemoryLoader
from janus_code.mcp_server.embedded import EmbeddedJanusMcpServer
from janus_code.janus_integration.aggregator import Aggregator
from janus_code.janus_integration.parallel_executor import (
    AgentResult,
    AgentTask,
    ParallelExecutor,
)
from janus_code.janus_integration.session_manager import SessionManager, TxnContext
from janus_code.janus_integration.sub_agent_txn import SubAgentTxn, SubAgentResult
from janus_code.tools.registry import build_tool_schemas

logger = logging.getLogger(__name__)


@dataclass
class TaskPlan:
    """Plan for how to execute a user request."""

    parallelize: bool = False
    subtasks: list[dict[str, Any]] = field(default_factory=list)
    inline_mode: str = "direct"  # "direct" | "subtransactions"
    inline_subtasks: list[dict[str, str]] = field(default_factory=list)
    assessment: str = ""
    dag: NormalizedDagPlan | None = None


class Orchestrator:
    """Top-level agent that processes each user message.

    For each message, decides whether to execute inline (single txn)
    or decompose into parallel transactions + aggregation.

    In Phase 2, this runs without Janus transaction integration — just
    the agent loop with tools. Phase 3 adds transaction boundaries.
    """

    def __init__(
        self,
        config: Config,
        working_dir: Path,
        tools: dict[str, Any],
        llm_fn: Callable,
        tool_schemas: list[dict[str, Any]] | None = None,
        refresh_tools_for_txn: (
            Callable[[TxnContext], tuple[dict[str, Any], list[dict[str, Any]]]]
            | None
        ) = None,
        parallel_tools_for_context: (
            Callable[[Any], tuple[dict[str, Any], list[dict[str, Any]]]]
            | None
        ) = None,
        event_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.config = config
        self.working_dir = working_dir
        self.tools = tools
        self.llm_fn = llm_fn
        self.tool_schemas = tool_schemas or []
        self.last_result: str = ""
        self._refresh_tools_for_txn = refresh_tools_for_txn
        self._parallel_tools_for_context = parallel_tools_for_context
        self._event_sink = event_sink
        self._active_session: SessionManager | None = None
        self._active_subtxn_runner: SubAgentTxn | None = None
        self._external_worker_adapter = ExternalWorkerAdapter(
            config=config,
            root_dir=working_dir,
            event_sink=self._event_sink,
        )
        self._embedded_mcp_server: EmbeddedJanusMcpServer | None = None
        if (
            self._external_worker_adapter.runtime in ("codex", "claude")
            and bool(config.external_worker_use_embedded_mcp)
        ):
            self._embedded_mcp_server = EmbeddedJanusMcpServer(
                server_name=config.external_worker_mcp_server_name,
                host=config.embedded_mcp_host,
                port=int(config.embedded_mcp_port),
                path=config.embedded_mcp_path,
                event_sink=self._event_sink,
            )
        self._last_turn_usage: dict[str, int] = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "cached_prompt_tokens": 0,
            "uncached_prompt_tokens": 0,
        }

        # Build memory loader
        self.memory_loader = MemoryLoader(
            working_dir=working_dir, memory_dir=config.memory_dir
        )

        # Conversation and context management
        self.conversation = Conversation()
        self.context_manager = ContextManager(
            config=config,
            memory_loader=self.memory_loader,
            conversation=self.conversation,
        )

        # Initialize system prompt
        self._init_system_prompt()

    def _emit_event(self, event_type: str, message: str, **details: Any) -> None:
        """Emit a structured progress event for optional CLI display."""
        if self._event_sink is None:
            return
        payload: dict[str, Any] = {"type": event_type, "message": message}
        if details:
            payload.update(details)
        try:
            self._event_sink(payload)
        except Exception:
            logger.debug("Progress event sink failed", exc_info=True)

    @staticmethod
    def _clip(text: str, max_chars: int = 140) -> str:
        value = (text or "").strip().replace("\n", " ")
        if len(value) <= max_chars:
            return value
        return value[: max_chars - 3] + "..."

    def _resolve_txn_working_dir(
        self,
        txn: TxnContext | None = None,
        session: SessionManager | None = None,
    ) -> Path:
        """Resolve working directory for the current transaction branch."""
        if session is not None:
            janus_ctx = getattr(session, "janus_context", None)
            workdir = getattr(janus_ctx, "working_dir", None)
            if workdir is not None:
                return Path(workdir)
        if txn is not None:
            janus_ctx = getattr(txn, "_janus_context", None)
            workdir = getattr(janus_ctx, "working_dir", None)
            if workdir is not None:
                return Path(workdir)
        return self.working_dir

    async def _run_external_worker(
        self,
        *,
        agent_type: str,
        objective: str,
        task_contract: dict[str, Any],
        txn: TxnContext,
        working_dir: Path,
        session: SessionManager | None = None,
    ) -> str:
        runtime = self._external_worker_adapter.runtime
        if runtime not in ("codex", "claude"):
            raise RuntimeError("External worker runtime is not enabled.")
        session_id = self._external_worker_adapter.build_branch_session_id(
            txn_id=txn.txn_id,
            agent_type=agent_type,
        )
        mcp_url: str | None = None
        if self._embedded_mcp_server is not None:
            janus_ctx = (
                getattr(session, "janus_context", None)
                if session is not None
                else getattr(txn, "_janus_context", None)
            )
            if janus_ctx is None:
                raise RuntimeError(
                    "Embedded MCP mode requires an active JanusContext from orchestrator."
                )
            self._embedded_mcp_server.ensure_started()
            self._embedded_mcp_server.register_session(session_id, janus_ctx)
            mcp_url = self._embedded_mcp_server.url
        try:
            return await self._external_worker_adapter.run_worker(
                runtime=runtime,
                agent_type=agent_type,
                objective=objective,
                task_contract=task_contract,
                branch_session_id=session_id,
                working_dir=working_dir,
                txn_id=txn.txn_id,
                mcp_url=mcp_url,
            )
        finally:
            if self._embedded_mcp_server is not None:
                self._embedded_mcp_server.unregister_session(session_id)

    def _reset_turn_usage(self) -> None:
        self._last_turn_usage = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "cached_prompt_tokens": 0,
            "uncached_prompt_tokens": 0,
        }

    def _record_usage(self, usage: dict[str, Any] | None) -> None:
        if not usage:
            return
        for key in (
            "prompt_tokens",
            "completion_tokens",
            "total_tokens",
            "cached_prompt_tokens",
            "uncached_prompt_tokens",
        ):
            try:
                value = int(usage.get(key, 0))
            except Exception:
                value = 0
            self._last_turn_usage[key] = self._last_turn_usage.get(key, 0) + value

    def _record_usage_from_response(self, response: dict[str, Any] | None) -> None:
        if isinstance(response, dict):
            self._record_usage(response.get("usage"))

    def get_last_turn_usage(self) -> dict[str, int]:
        return dict(self._last_turn_usage)

    def _init_system_prompt(self) -> None:
        """Build and set the system prompt with memory."""
        system_parts = [ORCHESTRATOR_SYSTEM_PROMPT]

        memory_text = self.memory_loader.load_all()
        if memory_text:
            system_parts.append(f"\n\n{memory_text}")

        self.conversation.append_system("\n".join(system_parts))

    async def process(
        self,
        user_input: str,
        session: SessionManager | None = None,
    ) -> str:
        """Process a user message. Returns the assistant's response.

        - If ``session`` is None, runs in simple inline mode (Phase 2 style).
        - If ``session`` is provided, decides inline vs parallel and manages
          transaction boundaries (Phase 3/4 flow).
        """
        if session is None:
            return await self._process_simple(user_input)

        self._active_session = session
        self._reset_turn_usage()
        self._emit_event(
            "turn_start",
            "Processing user message.",
            preview=self._clip(user_input, max_chars=100),
        )
        plan, planning_txn = await self._assess_task(user_input, session)
        dag = plan.dag
        self._emit_event(
            "plan_result",
            "Planner produced DAG execution plan.",
            txn_id=planning_txn.txn_id,
            node_count=len(dag.nodes_by_id) if dag is not None else 0,
            level_count=len(dag.topo_levels) if dag is not None else 0,
            assessment=plan.assessment,
        )
        if dag is not None:
            self._emit_event(
                "plan_dag",
                "DAG visualization.",
                txn_id=planning_txn.txn_id,
                preview=self._dag_preview(dag),
            )

        try:
            self._emit_event(
                "txn_commit",
                "Committed planning transaction.",
                txn_id=planning_txn.txn_id,
                stage="planning",
            )
            session.commit_txn(planning_txn)
            if dag is None:
                raise RuntimeError("Planner did not produce a DAG plan.")
            return await self._execute_dag(
                plan=dag,
                session=session,
                planning_txn_id=planning_txn.txn_id,
            )
        except Exception:
            if planning_txn.is_active:
                self._emit_event(
                    "txn_abort",
                    "Aborted planning transaction.",
                    txn_id=planning_txn.txn_id,
                    stage="planning",
                )
                session.abort_txn(planning_txn)
            raise

    async def _process_simple(self, user_input: str) -> str:
        """Legacy inline processing without explicit transaction manager."""
        self._reset_turn_usage()
        self.conversation.advance_turn()
        self.conversation.append_user(user_input)

        loop = AgentLoop(
            conversation=self.conversation,
            context_manager=self.context_manager,
            tools=self.tools,
            llm_fn=self.llm_fn,
            tool_schemas=self.tool_schemas,
            on_llm_usage=self._record_usage,
        )
        final_message = await loop.run()
        self.last_result = final_message.content
        return final_message.content

    async def _assess_task(
        self,
        user_input: str,
        session: SessionManager,
    ) -> tuple[TaskPlan, TxnContext]:
        """Start the planning transaction and produce a validated execution DAG."""
        fallback_dag = self._build_fallback_dag(user_input)
        default = TaskPlan(
            parallelize=False,
            subtasks=[],
            inline_mode="direct",
            inline_subtasks=[],
            assessment="fallback-dag",
            dag=fallback_dag,
        )
        self.conversation.advance_turn()
        planning_txn = session.begin_txn()
        self._emit_event(
            "txn_begin",
            "Started planning transaction.",
            txn_id=planning_txn.txn_id,
            stage="planning",
        )
        self._refresh_tools_for_txn_if_needed(planning_txn)
        self._persist_user_message(session, user_input)

        text = user_input.strip()
        if not text:
            return default, planning_txn

        try:
            # Shot 1: structure-first DAG plan.
            planner_messages = self._build_planner_structure_messages(text)
            self._emit_event(
                "llm_plan",
                "Calling planner LLM (shot 1/2: DAG structure).",
                txn_id=planning_txn.txn_id,
                shot=1,
                message_count=len(planner_messages),
            )
            response = self.llm_fn(planner_messages, [])
            if hasattr(response, "__await__"):
                response = await response
            self._record_usage_from_response(response)
            content = str(response.get("content", "")).strip()
            if not content:
                self._emit_event(
                    "llm_plan_empty",
                    "Planner returned empty output; using fallback DAG.",
                    txn_id=planning_txn.txn_id,
                    shot=1,
                )
                return default, planning_txn

            parsed, parse_debug = self._extract_json_obj_with_debug(content)
            if parsed is None:
                self._emit_event(
                    "llm_plan_invalid",
                    "Planner output was not valid JSON; using fallback DAG.",
                    txn_id=planning_txn.txn_id,
                    shot=1,
                    error=self._clip(parse_debug, max_chars=220),
                    preview=self._clip(content, max_chars=220),
                )
                return default, planning_txn

            structure_payload = self._rewrite_group_dependencies(parsed)
            structure_payload = self._ensure_executable_prompts(
                structure_payload,
                text,
            )
            try:
                structure_dag = normalize_planner_dag(structure_payload, text)
                logger.debug("Planner produced valid DAG structure: %s", json.dumps(parsed))
            except DagValidationError as e:
                repaired = self._repair_planner_dag(structure_payload, str(e))
                if repaired is not None:
                    try:
                        structure_dag = normalize_planner_dag(repaired, text)
                        structure_payload = repaired
                        self._emit_event(
                            "llm_plan_repaired",
                            "Planner DAG structure repaired and accepted.",
                            txn_id=planning_txn.txn_id,
                            shot=1,
                            error=self._clip(str(e), max_chars=200),
                        )
                    except Exception as inner:
                        self._emit_event(
                            "llm_plan_invalid",
                            "Planner DAG invalid after repair; using fallback DAG.",
                            txn_id=planning_txn.txn_id,
                            shot=1,
                            error=self._clip(str(inner), max_chars=220),
                        )
                        return default, planning_txn
                else:
                    self._emit_event(
                        "llm_plan_invalid",
                        "Planner DAG invalid; using fallback DAG.",
                        txn_id=planning_txn.txn_id,
                        shot=1,
                        error=self._clip(str(e), max_chars=200),
                    )
                    return default, planning_txn
            except Exception as e:
                self._emit_event(
                    "llm_plan_invalid",
                    "Planner DAG parse failed; using fallback DAG.",
                    txn_id=planning_txn.txn_id,
                    shot=1,
                    error=self._clip(str(e), max_chars=200),
                )
                return default, planning_txn

            # Shot 2: fill prompts/constraints/tools while preserving structure.
            detail_messages = self._build_planner_detail_messages(
                text=text,
                structure_dag=structure_dag,
            )
            self._emit_event(
                "llm_plan",
                "Calling planner LLM (shot 2/2: node details).",
                txn_id=planning_txn.txn_id,
                shot=2,
                message_count=len(detail_messages),
            )
            response_2 = self.llm_fn(detail_messages, [])
            if hasattr(response_2, "__await__"):
                response_2 = await response_2
            self._record_usage_from_response(response_2)
            content_2 = str(response_2.get("content", "")).strip()
            assessment = structure_dag.assessment or str(parsed.get("assessment", ""))
            dag = structure_dag

            if not content_2:
                self._emit_event(
                    "llm_plan_empty",
                    "Planner detail shot returned empty output; using structure DAG.",
                    txn_id=planning_txn.txn_id,
                    shot=2,
                )
            else:
                parsed_2, parse_debug_2 = self._extract_json_obj_with_debug(content_2)
                if parsed_2 is None:
                    self._emit_event(
                        "llm_plan_invalid",
                        "Planner detail shot was not valid JSON; using structure DAG.",
                        txn_id=planning_txn.txn_id,
                        shot=2,
                        error=self._clip(parse_debug_2, max_chars=220),
                        preview=self._clip(content_2, max_chars=220),
                    )
                else:
                    detailed_payload = self._apply_node_details(structure_payload, parsed_2)
                    detailed_payload = self._rewrite_group_dependencies(detailed_payload)
                    detailed_payload = self._ensure_executable_prompts(
                        detailed_payload,
                        text,
                    )
                    try:
                        dag = normalize_planner_dag(detailed_payload, text)
                        self._emit_event(
                            "llm_plan_repaired",
                            "Planner node details accepted.",
                            txn_id=planning_txn.txn_id,
                            shot=2,
                        )
                        assessment = (
                            dag.assessment
                            or str(parsed_2.get("assessment", "")).strip()
                            or assessment
                        )
                    except Exception as e:
                        self._emit_event(
                            "llm_plan_invalid",
                            "Planner detail shot invalid DAG; using structure DAG.",
                            txn_id=planning_txn.txn_id,
                            shot=2,
                            error=self._clip(str(e), max_chars=220),
                        )

            has_parallel_wave = any(len(level) > 1 for level in dag.topo_levels)

            return (
                TaskPlan(
                    parallelize=has_parallel_wave,
                    subtasks=[],
                    inline_mode="dag",
                    inline_subtasks=[],
                    assessment=assessment,
                    dag=dag,
                ),
                planning_txn,
            )
        except Exception as e:
            logger.warning("Planner LLM failed; falling back to default DAG (%s)", e)
            self._emit_event(
                "llm_plan_error",
                "Planner LLM failed; using fallback DAG.",
                txn_id=planning_txn.txn_id,
                error=self._clip(str(e), max_chars=180),
            )
            return default, planning_txn

    def _build_planner_structure_messages(
        self,
        user_text: str,
    ) -> list[dict[str, str]]:
        """Build shot-1 planner messages for DAG structure only."""
        history = self._build_parent_history_snapshot(
            max_messages=40,
            max_chars=9000,
        )
        return [
            {"role": "system", "content": PLANNER_STRUCTURE_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Planning task: produce a DAG structure only.\n\n"
                    "Return ONLY valid JSON.\n"
                    "Do not include prose outside JSON.\n"
                    "Every depends_on id must exist.\n"
                    "DAG must be acyclic.\n\n"
                    "JSON schema:\n"
                    "{\n"
                    '  "assessment": string,\n'
                    '  "dag": {\n'
                    '    "mode": "parallel" | "sequential",\n'
                    '    "response_node_id": string | null,\n'
                    '    "nodes": [\n'
                    "      {\n"
                    '        "id": string,\n'
                    '        "agent_type": string,\n'
                    '        "depends_on": [string],\n'
                    '        "executable": boolean,\n'
                    '        "children_mode": "parallel" | "sequential",\n'
                    '        "children": [ ... same node schema ... ]\n'
                    "      }\n"
                    "    ]\n"
                    "  }\n"
                    "}\n\n"
                    f"Recent context:\n{history}\n\n"
                    f"Current user request:\n{user_text}"
                ),
            },
        ]

    def _build_planner_detail_messages(
        self,
        *,
        text: str,
        structure_dag: NormalizedDagPlan,
    ) -> list[dict[str, str]]:
        """Build shot-2 planner messages for node prompts and constraints."""
        node_outline = [
            {
                "id": node.id,
                "agent_type": node.agent_type,
                "depends_on": list(node.depends_on),
            }
            for node in structure_dag.nodes_by_id.values()
        ]
        return [
            {"role": "system", "content": PLANNER_DETAILS_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Planning refinement: fill node details for this fixed DAG.\n\n"
                    "Return ONLY valid JSON.\n"
                    "Do not change ids or depends_on.\n\n"
                    "Fixed executable nodes:\n"
                    f"{json.dumps(node_outline, ensure_ascii=True, indent=2)}\n\n"
                    "JSON schema:\n"
                    "{\n"
                    '  "assessment": string,\n'
                    '  "node_details": [\n'
                    "    {\n"
                    '      "id": string,\n'
                    '      "prompt": string,\n'
                    '      "constraints": [string],\n'
                    '      "forbidden_paths": [string],\n'
                    '      "success_criteria": [string],\n'
                    '      "allowed_tools": [string]\n'
                    "    }\n"
                    "  ]\n"
                    "}\n\n"
                    f"Current user request:\n{text}"
                ),
            },
        ]

    def _ensure_executable_prompts(
        self,
        parsed_plan: dict[str, Any],
        user_text: str,
    ) -> dict[str, Any]:
        """Ensure each executable leaf has a deterministic prompt."""
        payload = copy.deepcopy(parsed_plan)
        dag_payload = payload.get("dag") if isinstance(payload.get("dag"), dict) else payload
        nodes = dag_payload.get("nodes")
        if not isinstance(nodes, list):
            return payload

        def _walk(node: Any, idx: int = 0) -> None:
            if not isinstance(node, dict):
                return
            node_id = str(
                node.get("id")
                or node.get("node_id")
                or node.get("name")
                or f"n{idx + 1}"
            ).strip() or f"n{idx + 1}"
            if not node.get("agent_type"):
                node["agent_type"] = "implement"

            children = node.get("children")
            child_list = children if isinstance(children, list) else []
            prompt = str(
                node.get("prompt")
                or node.get("objective")
                or node.get("task")
                or node.get("description")
                or node.get("instructions")
                or ""
            ).strip()

            if child_list and not prompt and "executable" not in node:
                node["executable"] = False

            if not child_list:
                executable = bool(node.get("executable", True))
                if not prompt:
                    node["prompt"] = (
                        f"Execute DAG node '{node_id}' for user request: {user_text}"
                    )
                    node["executable"] = True
                elif "executable" not in node:
                    node["executable"] = executable

            for child_idx, child in enumerate(child_list):
                _walk(child, child_idx)

        for idx, node in enumerate(nodes):
            _walk(node, idx)
        return payload

    def _rewrite_group_dependencies(
        self,
        parsed_plan: dict[str, Any],
    ) -> dict[str, Any]:
        """Rewrite dependencies on non-executable group nodes to leaf terminals."""
        payload = copy.deepcopy(parsed_plan)
        dag_payload = payload.get("dag") if isinstance(payload.get("dag"), dict) else payload
        nodes = dag_payload.get("nodes")
        if not isinstance(nodes, list):
            return payload

        terminals_by_id: dict[str, list[str]] = {}
        executable_by_id: dict[str, bool] = {}

        def _dedupe(items: list[str]) -> list[str]:
            seen: set[str] = set()
            out: list[str] = []
            for item in items:
                key = str(item).strip()
                if not key or key in seen:
                    continue
                seen.add(key)
                out.append(key)
            return out

        def _collect(node: Any, idx: int = 0) -> list[str]:
            if not isinstance(node, dict):
                return []
            node_id = str(
                node.get("id")
                or node.get("node_id")
                or node.get("name")
                or f"n{idx + 1}"
            ).strip() or f"n{idx + 1}"
            children = node.get("children")
            child_list = children if isinstance(children, list) else []
            prompt = str(
                node.get("prompt")
                or node.get("objective")
                or node.get("task")
                or node.get("description")
                or node.get("instructions")
                or ""
            ).strip()
            executable = bool(node.get("executable", bool(prompt)))
            executable_by_id[node_id] = executable

            child_terminals: list[str] = []
            for child_idx, child in enumerate(child_list):
                child_terminals.extend(_collect(child, child_idx))
            child_terminals = _dedupe(child_terminals)

            terminals = [node_id] if executable else child_terminals
            terminals_by_id[node_id] = _dedupe(terminals)
            return terminals_by_id[node_id]

        def _rewrite(node: Any) -> None:
            if not isinstance(node, dict):
                return
            deps = self._normalize_string_list(node.get("depends_on"))
            rewritten: list[str] = []
            for dep in deps:
                if dep in executable_by_id and not executable_by_id[dep]:
                    rewritten.extend(terminals_by_id.get(dep, []))
                else:
                    rewritten.append(dep)
            if deps or "depends_on" in node:
                node["depends_on"] = _dedupe(rewritten)
            children = node.get("children")
            if isinstance(children, list):
                for child in children:
                    _rewrite(child)

        for idx, node in enumerate(nodes):
            _collect(node, idx)
        for node in nodes:
            _rewrite(node)

        response_node_id = dag_payload.get("response_node_id")
        if isinstance(response_node_id, str):
            response_node_id = response_node_id.strip()
            if response_node_id in executable_by_id and not executable_by_id[response_node_id]:
                terminals = terminals_by_id.get(response_node_id, [])
                dag_payload["response_node_id"] = terminals[0] if len(terminals) == 1 else None
        return payload

    def _apply_node_details(
        self,
        base_payload: dict[str, Any],
        detail_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Merge shot-2 node details into shot-1 structure while preserving edges."""
        merged = copy.deepcopy(base_payload)
        details = self._extract_node_detail_map(detail_payload)
        if not details:
            return merged

        assessment = str(detail_payload.get("assessment", "")).strip()
        if assessment:
            merged["assessment"] = assessment

        dag_payload = merged.get("dag") if isinstance(merged.get("dag"), dict) else merged
        nodes = dag_payload.get("nodes")
        if not isinstance(nodes, list):
            return merged

        def _walk(node: Any) -> None:
            if not isinstance(node, dict):
                return
            node_id = str(
                node.get("id")
                or node.get("node_id")
                or node.get("name")
                or ""
            ).strip()
            if node_id and node_id in details:
                spec = details[node_id]
                for key in (
                    "prompt",
                    "constraints",
                    "forbidden_paths",
                    "success_criteria",
                    "allowed_tools",
                    "agent_type",
                ):
                    if key in spec:
                        node[key] = spec[key]
            children = node.get("children")
            if isinstance(children, list):
                for child in children:
                    _walk(child)

        for node in nodes:
            _walk(node)
        return merged

    def _extract_node_detail_map(
        self,
        payload: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        """Extract node details from shot-2 payload in flexible formats."""
        detail_map: dict[str, dict[str, Any]] = {}

        def _capture(node: Any) -> None:
            if not isinstance(node, dict):
                return
            node_id = str(
                node.get("id")
                or node.get("node_id")
                or node.get("name")
                or ""
            ).strip()
            if not node_id:
                return
            entry: dict[str, Any] = {}
            prompt = str(
                node.get("prompt")
                or node.get("objective")
                or node.get("task")
                or node.get("description")
                or node.get("instructions")
                or ""
            ).strip()
            if prompt:
                entry["prompt"] = prompt
            for key in (
                "constraints",
                "forbidden_paths",
                "success_criteria",
                "allowed_tools",
            ):
                if key in node:
                    entry[key] = self._normalize_string_list(node.get(key))
            agent_type = str(node.get("agent_type") or node.get("type") or "").strip().lower()
            if agent_type:
                entry["agent_type"] = agent_type
            if entry:
                detail_map[node_id] = entry

        node_details = payload.get("node_details")
        if isinstance(node_details, list):
            for item in node_details:
                _capture(item)

        dag_payload = payload.get("dag") if isinstance(payload.get("dag"), dict) else payload
        raw_nodes = dag_payload.get("nodes")
        if isinstance(raw_nodes, list):
            stack: list[Any] = list(raw_nodes)
            while stack:
                node = stack.pop(0)
                _capture(node)
                if isinstance(node, dict):
                    children = node.get("children")
                    if isinstance(children, list):
                        stack.extend(children)
        return detail_map

    def _build_fallback_dag(self, user_input: str) -> NormalizedDagPlan:
        """Build a conservative fallback DAG with no input-shape assumptions."""
        return normalize_planner_dag({}, user_input)

    @staticmethod
    def _repair_planner_dag(
        parsed: dict[str, Any],
        error: str,
    ) -> dict[str, Any] | None:
        """Attempt safe planner DAG repairs for known non-structural issues."""
        # Recover from non-existent response_node_id without discarding the DAG.
        if "response_node_id" in error and "not in DAG nodes" in error:
            repaired = copy.deepcopy(parsed)
            dag_payload = repaired.get("dag")
            if isinstance(dag_payload, dict):
                dag_payload.pop("response_node_id", None)
                return repaired
        return None

    @staticmethod
    def _dag_preview(dag: NormalizedDagPlan, max_levels: int = 6) -> str:
        """Create graph-only ASCII DAG visualization with arrows."""
        max_targets = 40
        max_sources = 20

        lines: list[str] = ["DAG Graph:"]

        # Track outgoing edges so disconnected nodes can still be shown.
        outgoing: dict[str, list[str]] = {nid: [] for nid in dag.nodes_by_id}
        for target, node in dag.nodes_by_id.items():
            for dep in node.depends_on:
                outgoing.setdefault(dep, []).append(target)

        targets = [
            node_id
            for node_id in dag.nodes_by_id
            if dag.nodes_by_id[node_id].depends_on
        ]
        shown_targets = sorted(targets)[:max_targets]

        for target in shown_targets:
            deps = sorted(dag.nodes_by_id[target].depends_on)
            if len(deps) == 1:
                lines.append(f"  {deps[0]} ----> {target}")
                continue
            if len(deps) == 2:
                width = max(len(dep) for dep in deps)
                lines.append(f"  {deps[0]:<{width}} ---\\")
                lines.append(f"  {'':<{width}}    +---> {target}")
                lines.append(f"  {deps[1]:<{width}} ---/")
                continue

            width = max(len(dep) for dep in deps)
            for i, dep in enumerate(deps):
                if i == 0:
                    arm = "---\\"
                elif i == len(deps) - 1:
                    arm = "---/"
                else:
                    arm = "---+"
                lines.append(f"  {dep:<{width}} {arm}")
            lines.append(f"  {'':<{width + 1}} +---> {target}")

        if len(targets) > max_targets:
            lines.append(f"  ... (+{len(targets) - max_targets} more targets)")

        # Show source/disconnected nodes not represented in arrow rows.
        sources = sorted(
            node_id
            for node_id, node in dag.nodes_by_id.items()
            if not node.depends_on
        )
        shown_sources = sources[:max_sources]
        if shown_sources:
            lines.append("  sources:")
            for src in shown_sources:
                if outgoing.get(src):
                    lines.append(f"  {src}")
                else:
                    lines.append(f"  {src} (isolated)")
        if len(sources) > max_sources:
            lines.append(f"  ... (+{len(sources) - max_sources} more sources)")

        if not targets and not sources:
            lines.append("  (empty)")
        if dag.response_node_id:
            lines.append(f"  response: {dag.response_node_id}")
        if len(dag.topo_levels) > max_levels:
            lines.append(f"  ... (+{len(dag.topo_levels) - max_levels} more levels)")

        return "\n".join(lines)

    @staticmethod
    def _extract_json_obj_with_debug(
        text: str,
    ) -> tuple[dict[str, Any] | None, str]:
        """Extract a JSON object and return parse diagnostics."""
        if not text.strip():
            return None, "empty planner output"

        candidates = Orchestrator._json_candidates(text)
        errors: list[str] = []
        for idx, candidate in enumerate(candidates, start=1):
            parsed, err = Orchestrator._try_parse_json_obj(candidate)
            if parsed is not None:
                return parsed, f"parsed candidate {idx} without repair"
            errors.append(f"cand{idx}: {err}")

            repaired = Orchestrator._repair_json_candidate(candidate)
            if repaired != candidate:
                parsed, err = Orchestrator._try_parse_json_obj(repaired)
                if parsed is not None:
                    return parsed, f"parsed candidate {idx} after repair"
                errors.append(f"cand{idx}-repaired: {err}")

        if not candidates:
            return None, "no JSON object candidate found"
        return None, "; ".join(errors[:4])

    @staticmethod
    def _try_parse_json_obj(candidate: str) -> tuple[dict[str, Any] | None, str]:
        try:
            parsed = json.loads(candidate)
        except Exception as e:
            return None, f"{type(e).__name__}: {e}"
        if not isinstance(parsed, dict):
            return None, f"expected object, got {type(parsed).__name__}"
        return parsed, ""

    @staticmethod
    def _json_candidates(text: str) -> list[str]:
        """Generate parse candidates from raw/fenced/balanced JSON blocks."""
        candidates: list[str] = []
        stripped = text.strip()
        if stripped:
            candidates.append(stripped)

        for match in re.finditer(
            r"```(?:json)?\s*(\{.*?\})\s*```",
            text,
            flags=re.DOTALL | re.IGNORECASE,
        ):
            blob = str(match.group(1)).strip()
            if blob:
                candidates.append(blob)

        # Capture balanced object slices to avoid taking whole noisy text.
        depth = 0
        start_idx: int | None = None
        for idx, ch in enumerate(text):
            if ch == "{":
                if depth == 0:
                    start_idx = idx
                depth += 1
            elif ch == "}":
                if depth > 0:
                    depth -= 1
                    if depth == 0 and start_idx is not None:
                        blob = text[start_idx : idx + 1].strip()
                        if blob:
                            candidates.append(blob)
                        start_idx = None

        # de-duplicate while preserving order
        seen: set[str] = set()
        out: list[str] = []
        for cand in candidates:
            if cand in seen:
                continue
            seen.add(cand)
            out.append(cand)
        return out

    @staticmethod
    def _repair_json_candidate(candidate: str) -> str:
        """Repair common planner JSON issues (quotes, trailing commas)."""
        repaired = candidate
        repaired = repaired.replace("\u201c", '"').replace("\u201d", '"')
        repaired = repaired.replace("\u2018", "'").replace("\u2019", "'")
        repaired = re.sub(r",\s*([}\]])", r"\1", repaired)
        repaired = Orchestrator._escape_unescaped_quotes_in_json_strings(repaired)
        return repaired

    @staticmethod
    def _escape_unescaped_quotes_in_json_strings(text: str) -> str:
        """Escape likely interior quotes in JSON strings.

        This repairs payloads such as: {"k":"value with "v2" quotes"}.
        """
        out: list[str] = []
        in_string = False
        escape = False
        n = len(text)
        i = 0
        while i < n:
            ch = text[i]
            if not in_string:
                out.append(ch)
                if ch == '"':
                    in_string = True
                i += 1
                continue

            if escape:
                out.append(ch)
                escape = False
                i += 1
                continue

            if ch == "\\":
                out.append(ch)
                escape = True
                i += 1
                continue

            if ch == '"':
                j = i + 1
                while j < n and text[j].isspace():
                    j += 1
                # Valid JSON string terminators:
                # - key strings: followed by colon
                # - value strings: followed by comma/]/}
                if j >= n or text[j] in {":", ",", "}", "]"}:
                    out.append(ch)
                    in_string = False
                else:
                    out.append('\\"')
                i += 1
                continue

            out.append(ch)
            i += 1
        return "".join(out)

    @staticmethod
    def _parse_inline_subtasks(
        parsed_plan: dict[str, Any],
        user_text: str,
    ) -> list[dict[str, str]]:
        """Parse planner-provided inline subtransactions."""
        raw = parsed_plan.get("inline_subtasks")
        if raw is None:
            # Legacy compatibility: planners returning only "steps".
            raw = parsed_plan.get("steps", [])

        results: list[dict[str, str]] = []
        for entry in raw if isinstance(raw, list) else []:
            if isinstance(entry, str):
                agent_type = entry.strip().lower()
                if not agent_type:
                    continue
                results.append({
                    "agent_type": agent_type,
                    "task": f"{agent_type.title()} for request: {user_text}",
                })
                continue
            if isinstance(entry, dict):
                agent_type = str(
                    entry.get("agent_type")
                    or entry.get("type")
                    or entry.get("name")
                    or "implement"
                ).strip().lower()
                task = str(
                    entry.get("task")
                    or entry.get("description")
                    or entry.get("instructions")
                    or user_text
                ).strip()
                if not agent_type or not task:
                    continue
                results.append({
                    "agent_type": agent_type,
                    "task": task,
                })
        return results

    @staticmethod
    def _parse_parallel_subtasks(
        parsed_plan: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Parse planner-provided parallel subtasks and worker contracts."""
        raw = parsed_plan.get("parallel_subtasks")
        if raw is None:
            raw = parsed_plan.get("subtasks", [])

        results: list[dict[str, Any]] = []
        for entry in raw if isinstance(raw, list) else []:
            if isinstance(entry, str):
                objective = entry.strip()
                if not objective:
                    continue
                results.append({
                    "objective": objective,
                    "description": objective,
                    "agent_type": "implement",
                    "constraints": [],
                    "forbidden_paths": [],
                    "success_criteria": [],
                    "allowed_tools": [],
                })
                continue

            if isinstance(entry, dict):
                objective = str(
                    entry.get("objective")
                    or entry.get("description")
                    or entry.get("task")
                    or entry.get("instructions")
                    or ""
                ).strip()
                if not objective:
                    continue
                agent_type = str(
                    entry.get("agent_type")
                    or entry.get("type")
                    or "implement"
                ).strip().lower()
                if not agent_type:
                    agent_type = "implement"
                constraints = Orchestrator._normalize_string_list(
                    entry.get("constraints") or entry.get("constraint")
                )
                forbidden_paths = Orchestrator._normalize_string_list(
                    entry.get("forbidden_paths")
                )
                success_criteria = Orchestrator._normalize_string_list(
                    entry.get("success_criteria")
                )
                allowed_tools = Orchestrator._normalize_string_list(
                    entry.get("allowed_tools")
                )
                results.append({
                    "objective": objective,
                    "description": objective,
                    "agent_type": agent_type,
                    "constraints": constraints,
                    "forbidden_paths": forbidden_paths,
                    "success_criteria": success_criteria,
                    "allowed_tools": allowed_tools,
                })
        return results

    @staticmethod
    def _normalize_string_list(value: Any) -> list[str]:
        """Normalize planner-provided scalar/list values into a string list."""
        if value is None:
            return []
        if isinstance(value, str):
            text = value.strip()
            return [text] if text else []
        if not isinstance(value, (list, tuple)):
            return []
        out: list[str] = []
        for item in value:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                out.append(text)
        return out

    @staticmethod
    def _parallel_task_objective(task_spec: dict[str, Any]) -> str:
        """Resolve canonical objective text for a parallel task spec."""
        return str(
            task_spec.get("objective")
            or task_spec.get("description")
            or task_spec.get("task")
            or ""
        ).strip()

    @staticmethod
    def _infer_paths_from_parallel_task(task_spec: dict[str, Any]) -> list[str]:
        """Resolve allowed paths heuristically from the objective text."""
        return Orchestrator._infer_paths_from_objective(
            Orchestrator._parallel_task_objective(task_spec)
        )

    @staticmethod
    def _parallel_task_forbidden_paths(
        task_spec: dict[str, Any],
        all_known_paths: set[str],
        allowed_paths: list[str],
    ) -> list[str]:
        """Resolve forbidden paths or derive from sibling allowed paths."""
        explicit = Orchestrator._normalize_string_list(task_spec.get("forbidden_paths"))
        if explicit:
            return explicit
        if not all_known_paths:
            return []
        return sorted(all_known_paths.difference(set(allowed_paths)))

    @staticmethod
    def _parallel_task_task_id(idx: int) -> str:
        """Generate deterministic internal task ids for parallel workers."""
        return f"parallel_{idx + 1}"

    def _build_parent_history_snapshot(
        self,
        max_messages: int = 80,
        max_chars: int = 14000,
    ) -> str:
        """Serialize recent parent history as compact read-only text."""
        messages = self.conversation.messages[-max_messages:]
        lines: list[str] = []
        for msg in messages:
            body = (msg.content or "").strip().replace("\n", " ")
            if len(body) > 400:
                body = body[:400] + "..."
            lines.append(f"[turn {msg.turn}] {msg.role}: {body}")
        text = "\n".join(lines) if lines else "No prior history."
        if len(text) > max_chars:
            text = text[-max_chars:]
        return text

    @staticmethod
    def _build_prior_results_summary(
        prior_results: list[SubAgentResult] | None,
        max_chars: int = 1000,
    ) -> str:
        """Compact summary of prior sequential sub-step outcomes."""
        if not prior_results:
            return "(none)"
        lines = []
        for result in prior_results:
            status = "ok" if result.success else "failed"
            summary = (result.summary or result.error or "").strip().replace("\n", " ")
            if len(summary) > 180:
                summary = summary[:180] + "..."
            lines.append(f"- {result.agent_type} [{status}]: {summary}")
        text = "\n".join(lines)
        if len(text) > max_chars:
            text = text[:max_chars] + "..."
        return text

    @staticmethod
    def _infer_paths_from_objective(objective: str) -> list[str]:
        """Heuristic extraction of path-like tokens from a task objective."""
        # Prefer explicit file-like paths from objective text.
        matches = re.findall(r"(?:^|[\s'\"`(])([A-Za-z0-9_./-]+\.[A-Za-z0-9_+-]+)", objective)
        cleaned: list[str] = []
        for raw in matches:
            candidate = raw.strip().strip(".,;:)]}>")
            if not candidate:
                continue
            # Ignore obvious URLs.
            if "://" in candidate:
                continue
            cleaned.append(candidate)

        # Preserve order while de-duplicating.
        seen: set[str] = set()
        ordered: list[str] = []
        for item in cleaned:
            if item in seen:
                continue
            seen.add(item)
            ordered.append(item)
        return ordered

    @staticmethod
    def _build_task_contract(
        *,
        node_id: str,
        objective: str,
        allowed_tools: list[str],
        forbidden_paths: list[str],
        constraints: list[str] | None = None,
        success_criteria: list[str] | None = None,
        upstream_results: list[dict[str, Any]] | None = None,
        prior_step_summaries: str = "(none)",
    ) -> dict[str, Any]:
        """Build a strict worker contract passed as structured JSON."""
        default_success_criteria = [
            "Complete only the objective for this node.",
            "Do not modify forbidden_paths.",
            "Return a concise summary of concrete outcomes.",
        ]
        return {
            "node_id": node_id,
            "objective": objective,
            "constraints": constraints or [],
            "forbidden_paths": forbidden_paths,
            "success_criteria": success_criteria or default_success_criteria,
            "allowed_tools": allowed_tools,
            "upstream_results": upstream_results or [],
            "prior_step_summaries": prior_step_summaries,
        }

    @staticmethod
    def _format_task_contract(contract: dict[str, Any]) -> str:
        """Render task contract as stable pretty JSON."""
        return json.dumps(contract, ensure_ascii=True, indent=2, sort_keys=True)

    @staticmethod
    def _build_worker_system_prompt(
        *,
        mode: str,
        base_prompt: str,
        task_contract: dict[str, Any],
    ) -> str:
        """Build deterministic worker system prompt from base prompt + contract."""
        contract_text = Orchestrator._format_task_contract(task_contract)
        mode_rule = (
            "DAG mode: execute exactly this node and rely only on upstream_results."
            if mode == "dag"
            else "Sequential mode: rely only on prior_step_summaries."
        )
        return (
            f"{base_prompt}\n\n"
            "STRICT EXECUTION CONTRACT:\n"
            f"- {mode_rule}\n"
            "- You must not perform work outside the contract.\n"
            "- Do not call launch_task to create hidden extra workers.\n"
            "- If constraints conflict, explain briefly and stop.\n\n"
            "Task contract (JSON):\n"
            f"{contract_text}"
        )

    @staticmethod
    def _brief_text(text: str, max_chars: int = 300) -> str:
        value = (text or "").strip().replace("\n", " ")
        if len(value) > max_chars:
            return value[:max_chars] + "..."
        return value

    def _emit_assistant_event(
        self,
        msg: Message,
        scope: str,
        txn_id: str | None = None,
    ) -> None:
        """Emit concise intermediate assistant output event."""
        text = self._brief_text(msg.content, max_chars=220)
        if not text:
            return
        self._emit_event(
            "llm_output",
            "LLM output received.",
            txn_id=txn_id,
            scope=scope,
            tool_calls=len(msg.tool_calls or []),
            preview=text,
        )

    def _emit_tool_call_event(
        self,
        tool_call: ToolCall,
        scope: str,
        txn_id: str | None = None,
    ) -> None:
        args_preview = self._clip(
            json.dumps(tool_call.arguments, ensure_ascii=True, default=str),
            160,
        )
        self._emit_event(
            "tool_call",
            f"Calling tool `{tool_call.name}`.",
            txn_id=txn_id,
            scope=scope,
            tool=tool_call.name,
            args=args_preview,
        )

    def _emit_tool_result_event(
        self,
        tool_call: ToolCall,
        scope: str,
        txn_id: str | None = None,
    ) -> None:
        result_preview = self._clip(str(tool_call.result or ""), 200)
        self._emit_event(
            "tool_result",
            f"Tool `{tool_call.name}` completed.",
            txn_id=txn_id,
            scope=scope,
            tool=tool_call.name,
            duration_ms=tool_call.duration_ms,
            result=result_preview,
        )

    def _on_direct_assistant_message(
        self,
        session: SessionManager,
        msg: Message,
        txn_id: str | None = None,
    ) -> None:
        self._persist_assistant_message(session, msg)
        self._emit_assistant_event(msg, scope="inline", txn_id=txn_id)

    def _on_direct_tool_call(
        self,
        tc: ToolCall,
        txn_id: str | None = None,
    ) -> None:
        self._emit_tool_call_event(tc, scope="inline", txn_id=txn_id)

    def _on_direct_tool_result(
        self,
        session: SessionManager,
        tc: ToolCall,
        txn_id: str | None = None,
    ) -> None:
        self._persist_tool_result(session, tc)
        self._emit_tool_result_event(tc, scope="inline", txn_id=txn_id)

    def _persist_user_message(self, session: SessionManager, user_input: str) -> None:
        msg = self.conversation.append_user(user_input)
        if session.message_store._shim is not None and session.message_store._txn is None:
            return
        session.persist_message(msg)

    def _persist_assistant_message(self, session: SessionManager, msg: Message) -> None:
        if session.message_store._shim is not None and session.message_store._txn is None:
            return
        session.persist_message(msg)

    def _persist_tool_result(
        self,
        session: SessionManager,
        tool_call: ToolCall,
    ) -> None:
        if session.message_store._shim is not None and session.message_store._txn is None:
            return
        session.message_store.store_tool_call(
            session.session_id,
            self.conversation.turn,
            tool_call,
        )
        # AgentLoop appends tool-result message before this callback runs.
        if self.conversation.messages:
            last = self.conversation.messages[-1]
            if last.role == "tool":
                session.persist_message(last)

    def _refresh_tools_for_txn_if_needed(self, txn: TxnContext) -> None:
        tools, schemas = self._get_tools_for_txn(txn, mutate=True)
        self.tools = tools
        self.tool_schemas = schemas

    def _get_tools_for_txn(
        self,
        txn: TxnContext,
        mutate: bool = False,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Get tool registry for a transaction.

        In parallel paths, callers should use ``mutate=False`` to avoid shared
        mutable tool state races across concurrently running workers.
        """
        if self._refresh_tools_for_txn is None:
            return self.tools, self.tool_schemas
        tools, schemas = self._refresh_tools_for_txn(txn)
        if mutate:
            self.tools = tools
            self.tool_schemas = schemas
        return tools, schemas

    async def _run_sub_agent(
        self,
        config: SubAgentConfig,
        tools: dict[str, Any],
        task: str,
        prior_results: list[SubAgentResult] | None,
        txn: TxnContext,
    ) -> str:
        """Run one sub-agent loop inside a subtransaction."""
        self._emit_event(
            "subtxn_agent_start",
            "Starting sub-agent transaction.",
            txn_id=txn.txn_id,
            agent_type=config.type,
            task=self._clip(task, max_chars=110),
        )
        prior_summary = self._build_prior_results_summary(prior_results)
        contract = self._build_task_contract(
            node_id=txn.txn_id,
            objective=task,
            allowed_tools=sorted(tools.keys()),
            forbidden_paths=[],
            prior_step_summaries=prior_summary,
        )
        generated_prompt = self._build_worker_system_prompt(
            mode="sequential",
            base_prompt=config.system_prompt,
            task_contract=contract,
        )
        runtime = self._external_worker_adapter.runtime
        if runtime in ("codex", "claude"):
            summary = await self._run_external_worker(
                agent_type=config.type,
                objective=task,
                task_contract=contract,
                txn=txn,
                working_dir=self._resolve_txn_working_dir(txn=txn),
                session=self._active_session,
            )
            self._emit_event(
                "subtxn_agent_done",
                "Sub-agent transaction completed.",
                txn_id=txn.txn_id,
                agent_type=config.type,
                runtime=runtime,
                summary=self._clip(summary, max_chars=140),
            )
            return summary

        sub_conv = Conversation()
        sub_conv.append_system(generated_prompt)
        sub_conv.advance_turn()
        sub_conv.append_user(
            "Execute this task contract exactly.\n\n"
            "Task contract (JSON):\n"
            f"{self._format_task_contract(contract)}"
        )

        sub_ctx = ContextManager(
            config=self.config,
            memory_loader=self.memory_loader,
            conversation=sub_conv,
        )
        sub_schemas = build_tool_schemas(tools)

        loop = AgentLoop(
            conversation=sub_conv,
            context_manager=sub_ctx,
            tools=tools,
            llm_fn=self.llm_fn,
            tool_schemas=sub_schemas,
            on_llm_usage=self._record_usage,
            on_assistant_message=lambda msg: self._emit_assistant_event(
                msg, scope=f"subtxn:{config.type}", txn_id=txn.txn_id
            ),
            on_tool_call=lambda tc: self._emit_tool_call_event(
                tc, scope=f"subtxn:{config.type}", txn_id=txn.txn_id
            ),
            on_tool_result=lambda tc: self._emit_tool_result_event(
                tc, scope=f"subtxn:{config.type}", txn_id=txn.txn_id
            ),
        )
        result = await loop.run()
        self._emit_event(
            "subtxn_agent_done",
            "Sub-agent transaction completed.",
            txn_id=txn.txn_id,
            agent_type=config.type,
            summary=self._clip(result.content, max_chars=140),
        )
        return result.content

    async def _execute_inline(
        self,
        user_input: str,
        plan: TaskPlan,
        session: SessionManager,
        txn: TxnContext | None = None,
        user_already_persisted: bool = False,
        turn_already_advanced: bool = False,
    ) -> str:
        """Inline path: one transaction with sequential subtransactions."""
        if txn is None:
            txn = session.begin_txn()
            self._emit_event(
                "txn_begin",
                "Started inline transaction.",
                txn_id=txn.txn_id,
                stage="inline",
            )
            self._refresh_tools_for_txn_if_needed(txn)
        self._active_subtxn_runner = None

        try:
            if not turn_already_advanced:
                self.conversation.advance_turn()
            if not user_already_persisted:
                self._persist_user_message(session, user_input)

            if plan.inline_mode != "subtransactions" or not plan.inline_subtasks:
                self._emit_event(
                    "inline_mode",
                    "Executing inline in direct mode.",
                    txn_id=txn.txn_id,
                )
                loop = AgentLoop(
                    conversation=self.conversation,
                    context_manager=self.context_manager,
                    tools=self.tools,
                    llm_fn=self.llm_fn,
                    tool_schemas=self.tool_schemas,
                    on_llm_usage=self._record_usage,
                    on_assistant_message=lambda msg: self._on_direct_assistant_message(
                        session, msg, txn_id=txn.txn_id
                    ),
                    on_tool_call=lambda tc: self._on_direct_tool_call(
                        tc, txn_id=txn.txn_id
                    ),
                    on_tool_result=lambda tc: self._on_direct_tool_result(
                        session, tc, txn_id=txn.txn_id
                    ),
                )
                final_msg = await loop.run()
                session.commit_txn(txn)
                self._emit_event(
                    "txn_commit",
                    "Committed inline transaction.",
                    txn_id=txn.txn_id,
                    stage="inline",
                )
                self.last_result = final_msg.content
                return final_msg.content

            self._emit_event(
                "inline_mode",
                "Executing inline via sequential subtransactions.",
                txn_id=txn.txn_id,
                steps=len(plan.inline_subtasks),
            )
            runner = SubAgentTxn(
                parent_txn=txn,
                all_tools=self.tools,
                run_agent_fn=self._run_sub_agent,
                event_sink=self._emit_event,
            )
            self._active_subtxn_runner = runner

            steps = [
                (step["agent_type"], step["task"])
                for step in plan.inline_subtasks
            ]
            results = await runner.run_sequential(
                steps=steps,
                stop_on_failure=False,
            )

            summary_lines: list[str] = []
            for r in results:
                if r.success:
                    summary_lines.append(
                        f"[{r.agent_type}] {self._brief_text(r.summary)}"
                    )
                else:
                    summary_lines.append(
                        f"[{r.agent_type}] FAILED: {self._brief_text(r.error or r.summary)}"
                    )
            summary = "\n\n".join(summary_lines) if summary_lines else "No work performed."

            final_msg = self.conversation.append_assistant(summary)
            self._persist_assistant_message(session, final_msg)
            session.commit_txn(txn)
            self._emit_event(
                "txn_commit",
                "Committed inline transaction.",
                txn_id=txn.txn_id,
                stage="inline",
            )

            self.last_result = summary
            return summary
        except Exception:
            session.abort_txn(txn)
            self._emit_event(
                "txn_abort",
                "Aborted inline transaction.",
                txn_id=txn.txn_id,
                stage="inline",
            )
            raise
        finally:
            self._active_subtxn_runner = None

    def _create_parallel_worker_session(
        self,
        base_session: SessionManager,
        task: AgentTask,
        attempt: int,
    ) -> SessionManager:
        """Create an isolated Janus session for one parallel worker attempt."""
        try:
            from langchain_janus import JanusContext
        except Exception:
            self._emit_event(
                "parallel_session_fallback",
                "Parallel worker session fallback to shared context (JanusContext unavailable).",
                task=self._clip(task.description, max_chars=100),
            )
            return base_session

        try:
            parallel_dir = self.working_dir / ".janus-code" / "parallel"
            parallel_dir.mkdir(parents=True, exist_ok=True)
            worker_db = parallel_dir / (
                f"worker_{base_session.session_id}_{attempt}_{uuid.uuid4().hex[:8]}.db"
            )
            worker_ctx = JanusContext(
                str(self.working_dir),
                db_path=str(worker_db),
                enable_sqlite=True,
                enable_vectorstore=False,
                weak_snapshot=bool(getattr(base_session.config, "weak_snapshot", False)),
            )
            worker_session = SessionManager(
                project_path=self.working_dir,
                config=base_session.config,
                janus_context=worker_ctx,
                db_path=":memory:",
            )
            worker_session.start_session()
            self._emit_event(
                "parallel_session_created",
                "Created isolated worker Janus session.",
                task=self._clip(task.description, max_chars=100),
                attempt=attempt,
            )
            return worker_session
        except Exception as e:
            self._emit_event(
                "parallel_session_fallback",
                "Failed to create isolated worker Janus session; using shared session.",
                task=self._clip(task.description, max_chars=100),
                attempt=attempt,
                error=self._clip(str(e), max_chars=180),
            )
            return base_session

    def _get_parallel_tools_for_session(
        self,
        session: SessionManager,
        txn: TxnContext,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Build per-worker tools bound to the worker Janus context."""
        if self._parallel_tools_for_context is not None:
            try:
                return self._parallel_tools_for_context(session.janus_context)
            except Exception as e:
                self._emit_event(
                    "parallel_tools_fallback",
                    "Parallel worker tool builder failed; falling back to shared tools.",
                    txn_id=txn.txn_id,
                    error=self._clip(str(e), max_chars=180),
                )
        # Shared fallback (may serialize behavior if using shared JanusContext).
        return self._get_tools_for_txn(txn, mutate=False)

    async def _execute_dag(
        self,
        plan: NormalizedDagPlan,
        session: SessionManager,
        planning_txn_id: str | None = None,
    ) -> str:
        """Execute a validated DAG in topological waves.

        Every node runs as its own transaction. Nodes in the same topological
        level run concurrently as parallel transactions.
        """
        executor = ParallelExecutor(
            session=session,
            retry_policy=session.config.retry_policy,
            run_agent_fn=self._run_parallel_agent,
            event_sink=self._emit_event,
            session_factory=lambda task, attempt: self._create_parallel_worker_session(
                session, task, attempt
            ),
        )
        results_by_node: dict[str, AgentResult] = {}
        all_results: list[AgentResult] = []

        for level_index, level_ids in enumerate(plan.topo_levels, start=1):
            runnable: list[AgentTask] = []

            for node_id in level_ids:
                node = plan.nodes_by_id[node_id]
                dep_results = [results_by_node[dep] for dep in node.depends_on]
                failed_deps = [res.task.metadata.get("node_id") for res in dep_results if not res.success]
                if failed_deps:
                    skipped = AgentResult(
                        task=AgentTask(
                            description=node.prompt,
                            agent_type=node.agent_type,
                            metadata={"node_id": node_id},
                        ),
                        success=False,
                        summary="",
                        error=(
                            "Skipped due to failed dependencies: "
                            + ", ".join(str(dep) for dep in failed_deps)
                        ),
                        txn_id="",
                        attempts=0,
                    )
                    results_by_node[node_id] = skipped
                    all_results.append(skipped)
                    self._emit_event(
                        "dag_node_skip",
                        "Skipped DAG node due to failed dependencies.",
                        txn_id=planning_txn_id,
                        node_id=node_id,
                        level=level_index,
                        error=skipped.error,
                    )
                    continue

                upstream_results = [
                    {
                        "node_id": dep,
                        "summary": results_by_node[dep].summary,
                        "success": results_by_node[dep].success,
                    }
                    for dep in node.depends_on
                    if dep in results_by_node
                ]
                runnable.append(
                    AgentTask(
                        description=node.prompt,
                        agent_type=node.agent_type,
                        metadata={
                            "node_id": node.id,
                            "objective": node.prompt,
                            "constraints": node.constraints,
                            "forbidden_paths": node.forbidden_paths,
                            "success_criteria": node.success_criteria,
                            "allowed_tools": node.allowed_tools,
                            "upstream_results": upstream_results,
                        },
                    )
                )

            if not runnable:
                continue

            self._emit_event(
                "parallel_start",
                "Starting DAG wave transactions.",
                txn_id=planning_txn_id,
                level=level_index,
                task_count=len(runnable),
            )
            wave_results = await executor.execute(runnable)
            self._emit_event(
                "parallel_done",
                "DAG wave transactions finished.",
                txn_id=planning_txn_id,
                level=level_index,
                success_count=sum(1 for r in wave_results if r.success),
                failure_count=sum(1 for r in wave_results if not r.success),
            )
            for result in wave_results:
                node_id = str((result.task.metadata or {}).get("node_id") or "")
                if node_id:
                    results_by_node[node_id] = result
                all_results.append(result)

        final_summary = await self._resolve_dag_final_summary(
            plan=plan,
            results_by_node=results_by_node,
            all_results=all_results,
            session=session,
            planning_txn_id=planning_txn_id,
        )
        self.last_result = final_summary
        return final_summary

    async def _resolve_dag_final_summary(
        self,
        plan: NormalizedDagPlan,
        results_by_node: dict[str, AgentResult],
        all_results: list[AgentResult],
        session: SessionManager,
        planning_txn_id: str | None = None,
    ) -> str:
        """Resolve final response from response node or LLM aggregation."""
        if plan.response_node_id:
            picked = results_by_node.get(plan.response_node_id)
            if picked is not None and picked.success and (picked.summary or "").strip():
                summary = picked.summary
                await self._persist_final_summary(summary, session)
                return summary

        terminal_ids = self._terminal_node_ids(plan.nodes_by_id)
        terminal_success = [
            results_by_node[node_id]
            for node_id in terminal_ids
            if node_id in results_by_node and results_by_node[node_id].success
        ]
        if len(terminal_success) == 1 and (terminal_success[0].summary or "").strip():
            summary = terminal_success[0].summary
            await self._persist_final_summary(summary, session)
            return summary

        aggregator = Aggregator(
            session=session,
            llm_fn=self.llm_fn,
            on_llm_usage=self._record_usage,
        )
        self._emit_event(
            "aggregator_start",
            "Starting aggregator transaction.",
            txn_id=planning_txn_id,
        )
        agg_result = await aggregator.aggregate(
            all_results,
            on_summary=self._persist_aggregated_summary,
        )
        self._emit_event(
            "aggregator_done",
            "Aggregator transaction finished.",
            txn_id=planning_txn_id,
            success=agg_result.success,
            merged_count=agg_result.merged_count,
        )
        if agg_result.success:
            return agg_result.summary
        await self._persist_final_summary(agg_result.summary, session)
        return agg_result.summary

    async def _persist_final_summary(
        self,
        summary: str,
        session: SessionManager,
    ) -> None:
        """Persist final assistant summary in its own transaction."""
        txn = session.begin_txn()
        self._refresh_tools_for_txn_if_needed(txn)
        try:
            msg = self.conversation.append_assistant(summary)
            self._persist_assistant_message(session, msg)
            session.commit_txn(txn)
            self._emit_event(
                "txn_commit",
                "Committed final summary transaction.",
                txn_id=txn.txn_id,
                stage="final-summary",
            )
        except Exception:
            if txn.is_active:
                session.abort_txn(txn)
                self._emit_event(
                    "txn_abort",
                    "Aborted final summary transaction.",
                    txn_id=txn.txn_id,
                    stage="final-summary",
                )
            raise

    @staticmethod
    def _terminal_node_ids(nodes_by_id: dict[str, FlatDagNode]) -> list[str]:
        """Return node IDs with no outgoing edges."""
        parents: set[str] = set()
        for node in nodes_by_id.values():
            parents.update(node.depends_on)
        return [node_id for node_id in nodes_by_id if node_id not in parents]

    async def _run_parallel_agent(
        self,
        task: AgentTask,
        txn: TxnContext,
        session: SessionManager | None = None,
    ) -> str:
        """Run one parallel worker in a clean context with a strict task contract."""
        worker_session = session or self._active_session
        if worker_session is None:
            raise RuntimeError("No active session for parallel worker.")
        self._emit_event(
            "parallel_worker_start",
            "Starting parallel worker.",
            txn_id=txn.txn_id,
            agent_type=task.agent_type,
            task=self._clip(task.description, max_chars=110),
        )
        config = get_sub_agent_config(task.agent_type)
        metadata = task.metadata or {}
        requested_tools = [
            name
            for name in self._normalize_string_list(metadata.get("allowed_tools"))
            if name in config.allowed_tools
        ]
        objective = str(metadata.get("objective") or task.description).strip()
        if not objective:
            objective = task.description
        constraints = self._normalize_string_list(metadata.get("constraints"))
        forbidden_paths = list(metadata.get("forbidden_paths") or [])
        success_criteria = self._normalize_string_list(metadata.get("success_criteria"))
        upstream_results = metadata.get("upstream_results")
        if not isinstance(upstream_results, list):
            upstream_results = []
        runtime = self._external_worker_adapter.runtime
        if runtime in ("codex", "claude"):
            allowed_tools = requested_tools or [
                "janus_txn",
                "janus_file_editor",
                "janus_bash",
                "janus_sqlite",
            ]
            contract = self._build_task_contract(
                node_id=str(metadata.get("node_id") or txn.txn_id),
                objective=objective,
                allowed_tools=allowed_tools,
                forbidden_paths=forbidden_paths,
                constraints=constraints,
                success_criteria=success_criteria,
                upstream_results=upstream_results,
            )
            summary = await self._run_external_worker(
                agent_type=task.agent_type,
                objective=objective,
                task_contract=contract,
                txn=txn,
                working_dir=self._resolve_txn_working_dir(
                    txn=txn,
                    session=worker_session,
                ),
                session=worker_session,
            )
            self._emit_event(
                "parallel_worker_done",
                "Parallel worker finished.",
                txn_id=txn.txn_id,
                agent_type=task.agent_type,
                runtime=runtime,
                summary=self._clip(summary, max_chars=140),
            )
            return summary

        tools, _ = self._get_parallel_tools_for_session(worker_session, txn)
        effective_allowed_tools = requested_tools or config.allowed_tools
        scoped_tools = filter_tools(tools, effective_allowed_tools)
        scoped_schemas = build_tool_schemas(scoped_tools)
        contract = self._build_task_contract(
            node_id=str(metadata.get("node_id") or txn.txn_id),
            objective=objective,
            allowed_tools=sorted(scoped_tools.keys()),
            forbidden_paths=forbidden_paths,
            constraints=constraints,
            success_criteria=success_criteria,
            upstream_results=upstream_results,
        )

        generated_prompt = self._build_worker_system_prompt(
            mode="dag",
            base_prompt=config.system_prompt,
            task_contract=contract,
        )

        sub_conv = Conversation()
        sub_conv.append_system(generated_prompt)
        sub_conv.advance_turn()
        sub_conv.append_user(
            "Execute this task contract exactly.\n\n"
            "Task contract (JSON):\n"
            f"{self._format_task_contract(contract)}\n\n"
            "Produce a concise summary of completed work and outcomes."
        )

        sub_ctx = ContextManager(
            config=self.config,
            memory_loader=self.memory_loader,
            conversation=sub_conv,
        )
        loop = AgentLoop(
            conversation=sub_conv,
            context_manager=sub_ctx,
            tools=scoped_tools,
            llm_fn=self.llm_fn,
            tool_schemas=scoped_schemas,
            on_llm_usage=self._record_usage,
            on_assistant_message=lambda msg: self._emit_assistant_event(
                msg, scope=f"parallel:{task.agent_type}", txn_id=txn.txn_id
            ),
            on_tool_call=lambda tc: self._emit_tool_call_event(
                tc, scope=f"parallel:{task.agent_type}", txn_id=txn.txn_id
            ),
            on_tool_result=lambda tc: self._emit_tool_result_event(
                tc, scope=f"parallel:{task.agent_type}", txn_id=txn.txn_id
            ),
        )
        result = await loop.run()
        self._emit_event(
            "parallel_worker_done",
            "Parallel worker finished.",
            txn_id=txn.txn_id,
            agent_type=task.agent_type,
            summary=self._clip(result.content, max_chars=140),
        )
        return result.content

    async def _persist_aggregated_summary(self, summary: str, _txn: TxnContext) -> None:
        """Persist the final aggregated assistant message inside aggregator txn."""
        if self._active_session is None:
            return
        msg = self.conversation.append_assistant(summary)
        self._persist_assistant_message(self._active_session, msg)

    async def launch_task(
        self,
        description: str,
        type: str,
        parallel: bool = False,
    ) -> str:
        """Runtime launcher used by TaskTool."""
        if parallel:
            return (
                "Parallel launch acknowledged, but async task queue is not "
                "enabled in this build. Run with parallel=false."
            )
        if self._active_subtxn_runner is None:
            return "No active parent transaction for launch_task."

        result = await self._active_subtxn_runner.run_step(
            agent_type=type,
            task_description=description,
            prior_results=self._active_subtxn_runner.results,
        )
        if result.success:
            return result.summary
        return f"Task failed: {result.error or result.summary}"

    def format_history(self) -> str:
        """Return current in-memory conversation as readable text."""
        if not self.conversation.messages:
            return "No conversation history."
        lines: list[str] = []
        for m in self.conversation.messages:
            body = (m.content or "").strip().replace("\n", "\n    ")
            lines.append(f"[turn {m.turn}] {m.role}: {body}")
        return "\n".join(lines)

    def restore_from_messages(self, messages: list[Message]) -> None:
        """Replace in-memory conversation with persisted messages."""
        if not messages:
            return
        self.conversation = Conversation(messages=list(messages))
        max_turn = max(m.turn for m in messages)
        while self.conversation.turn < max_turn:
            self.conversation.advance_turn()
        self.context_manager = ContextManager(
            config=self.config,
            memory_loader=self.memory_loader,
            conversation=self.conversation,
        )

    async def process_with_sub_agents(
        self,
        user_input: str,
        steps: list[str] | None = None,
    ) -> str:
        """Process using sequential sub-agent delegation.

        This is used when the orchestrator determines that the task
        benefits from structured sub-agent execution (explore → implement → test).

        Each sub-agent runs in sequence with its own conversation context
        but shares the same working directory (and in Phase 3, subtransaction).
        """
        self.conversation.advance_turn()
        self.conversation.append_user(user_input)

        effective_steps = steps or ["explore", "implement", "test"]
        results: list[str] = []

        for step_type in effective_steps:
            agent_config = get_sub_agent_config(step_type)
            scoped_tools = filter_tools(self.tools, agent_config.allowed_tools)

            # Create sub-agent conversation
            sub_conv = Conversation()
            sub_conv.append_system(agent_config.system_prompt)
            sub_conv.append_user(
                f"Task: {user_input}\n\n"
                + (f"Previous results:\n{chr(10).join(results)}" if results else "")
            )

            sub_ctx = ContextManager(
                config=self.config,
                memory_loader=self.memory_loader,
                conversation=sub_conv,
            )

            loop = AgentLoop(
                conversation=sub_conv,
                context_manager=sub_ctx,
                tools=scoped_tools,
                llm_fn=self.llm_fn,
                tool_schemas=self.tool_schemas,
                on_llm_usage=self._record_usage,
            )

            result = await loop.run()
            results.append(f"[{step_type}] {result.content}")

        # Summarize results in main conversation
        summary = "\n\n".join(results)
        self.conversation.append_assistant(summary)
        self.last_result = summary
        return summary
