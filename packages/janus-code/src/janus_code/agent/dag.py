"""DAG planning and validation helpers for orchestrator execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class DagValidationError(ValueError):
    """Raised when a planner-emitted DAG is invalid."""


@dataclass
class DagNode:
    """Planner node model (supports nested children)."""

    id: str
    agent_type: str
    prompt: str
    depends_on: list[str] = field(default_factory=list)
    constraints: list[str] = field(default_factory=list)
    forbidden_paths: list[str] = field(default_factory=list)
    success_criteria: list[str] = field(default_factory=list)
    allowed_tools: list[str] = field(default_factory=list)
    children: list["DagNode"] = field(default_factory=list)
    children_mode: str = "parallel"  # "parallel" | "sequential"
    executable: bool = True


@dataclass
class FlatDagNode:
    """Normalized executable node with explicit dependencies."""

    id: str
    agent_type: str
    prompt: str
    depends_on: list[str]
    constraints: list[str]
    forbidden_paths: list[str]
    success_criteria: list[str]
    allowed_tools: list[str]


@dataclass
class DagPlan:
    """Normalized DAG plan used by the orchestrator executor."""

    assessment: str
    nodes_by_id: dict[str, FlatDagNode]
    topo_levels: list[list[str]]
    response_node_id: str | None = None


def normalize_planner_dag(parsed: dict[str, Any], user_input: str) -> DagPlan:
    """Build a validated, normalized DAG plan from planner JSON."""
    assessment = str(parsed.get("assessment", "")).strip()
    dag_payload = parsed.get("dag") if isinstance(parsed.get("dag"), dict) else parsed

    root_nodes_raw = dag_payload.get("nodes")
    root_mode = str(dag_payload.get("mode", "parallel")).strip().lower()
    response_node_id = dag_payload.get("response_node_id")
    if not isinstance(root_nodes_raw, list) or not root_nodes_raw:
        root_nodes_raw, root_mode = _legacy_to_nodes(parsed, user_input)
        if response_node_id is None:
            response_node_id = parsed.get("response_node_id")

    root_nodes = [
        _parse_node(raw, fallback_id=f"n{idx + 1}")
        for idx, raw in enumerate(root_nodes_raw)
    ]

    nodes_by_id: dict[str, FlatDagNode] = {}
    if root_mode not in {"parallel", "sequential"}:
        root_mode = "parallel"

    if root_mode == "parallel":
        for node in root_nodes:
            _flatten_node(node, inherited_deps=[], out=nodes_by_id)
    else:
        chain_deps: list[str] = []
        for node in root_nodes:
            chain_deps = _flatten_node(node, inherited_deps=chain_deps, out=nodes_by_id)

    if not nodes_by_id:
        raise DagValidationError("Planner DAG produced no executable nodes.")

    for node_id, node in nodes_by_id.items():
        for dep in node.depends_on:
            if dep not in nodes_by_id:
                raise DagValidationError(
                    f"Node '{node_id}' depends on unknown node '{dep}'."
                )
            if dep == node_id:
                raise DagValidationError(
                    f"Node '{node_id}' cannot depend on itself."
                )

    topo_levels = _topological_levels(nodes_by_id)
    if response_node_id is not None:
        response_node_id = str(response_node_id).strip()
    if response_node_id and response_node_id not in nodes_by_id:
        raise DagValidationError(
            f"response_node_id '{response_node_id}' is not in DAG nodes."
        )

    return DagPlan(
        assessment=assessment,
        nodes_by_id=nodes_by_id,
        topo_levels=topo_levels,
        response_node_id=response_node_id or None,
    )


def _legacy_to_nodes(
    parsed: dict[str, Any],
    user_input: str,
) -> tuple[list[Any], str]:
    """Backward-compatible conversion from pre-DAG planner outputs."""
    inline_subtasks = parsed.get("inline_subtasks")
    if isinstance(inline_subtasks, list) and inline_subtasks:
        nodes: list[dict[str, Any]] = []
        for idx, entry in enumerate(inline_subtasks):
            if isinstance(entry, dict):
                prompt = str(
                    entry.get("task")
                    or entry.get("description")
                    or entry.get("instructions")
                    or user_input
                ).strip()
                agent_type = str(
                    entry.get("agent_type")
                    or entry.get("type")
                    or "implement"
                ).strip().lower() or "implement"
            else:
                prompt = str(entry).strip() or user_input
                agent_type = "implement"
            nodes.append(
                {
                    "id": f"n{idx + 1}",
                    "agent_type": agent_type,
                    "prompt": prompt,
                }
            )
        return nodes, "sequential"

    parallel_subtasks = parsed.get("parallel_subtasks", parsed.get("subtasks"))
    if isinstance(parallel_subtasks, list) and parallel_subtasks:
        nodes = []
        for idx, entry in enumerate(parallel_subtasks):
            if isinstance(entry, dict):
                prompt = str(
                    entry.get("prompt")
                    or entry.get("objective")
                    or entry.get("description")
                    or entry.get("task")
                    or user_input
                ).strip()
                agent_type = str(
                    entry.get("agent_type")
                    or entry.get("type")
                    or "implement"
                ).strip().lower() or "implement"
                constraints = _as_str_list(entry.get("constraints"))
                forbidden_paths = _as_str_list(entry.get("forbidden_paths"))
                success_criteria = _as_str_list(entry.get("success_criteria"))
                allowed_tools = _as_str_list(entry.get("allowed_tools"))
            else:
                prompt = str(entry).strip() or user_input
                agent_type = "implement"
                constraints = []
                forbidden_paths = []
                success_criteria = []
                allowed_tools = []
            nodes.append(
                {
                    "id": f"n{idx + 1}",
                    "agent_type": agent_type,
                    "prompt": prompt,
                    "constraints": constraints,
                    "forbidden_paths": forbidden_paths,
                    "success_criteria": success_criteria,
                    "allowed_tools": allowed_tools,
                }
            )
        return nodes, "parallel"

    return (
        [
            {
                "id": "n1",
                "agent_type": "implement",
                "prompt": user_input.strip() or "Handle the user request.",
            }
        ],
        "parallel",
    )


def _parse_node(raw: Any, fallback_id: str) -> DagNode:
    """Parse one node entry, recursively parsing nested children."""
    if isinstance(raw, str):
        prompt = raw.strip()
        return DagNode(
            id=fallback_id,
            agent_type="implement",
            prompt=prompt,
            executable=bool(prompt),
        )

    if not isinstance(raw, dict):
        raise DagValidationError(f"Node must be object or string; got {type(raw)!r}.")

    node_id = str(
        raw.get("id")
        or raw.get("node_id")
        or raw.get("name")
        or fallback_id
    ).strip()
    if not node_id:
        node_id = fallback_id

    prompt = str(
        raw.get("prompt")
        or raw.get("objective")
        or raw.get("task")
        or raw.get("description")
        or raw.get("instructions")
        or ""
    ).strip()

    children_raw = raw.get("children")
    children: list[DagNode] = []
    if isinstance(children_raw, list):
        children = [
            _parse_node(child, fallback_id=f"{node_id}_{idx + 1}")
            for idx, child in enumerate(children_raw)
        ]

    executable = bool(raw.get("executable", bool(prompt)))
    if executable and not prompt:
        raise DagValidationError(f"Node '{node_id}' is executable but prompt is empty.")
    if not executable and not children:
        raise DagValidationError(
            f"Node '{node_id}' is non-executable and has no children."
        )

    children_mode = str(raw.get("children_mode", raw.get("mode", "parallel"))).strip().lower()
    if children_mode not in {"parallel", "sequential"}:
        children_mode = "parallel"

    agent_type = str(raw.get("agent_type") or raw.get("type") or "implement").strip().lower()
    if not agent_type:
        agent_type = "implement"

    return DagNode(
        id=node_id,
        agent_type=agent_type,
        prompt=prompt,
        depends_on=_as_str_list(raw.get("depends_on")),
        constraints=_as_str_list(raw.get("constraints")),
        forbidden_paths=_as_str_list(raw.get("forbidden_paths")),
        success_criteria=_as_str_list(raw.get("success_criteria")),
        allowed_tools=_as_str_list(raw.get("allowed_tools")),
        children=children,
        children_mode=children_mode,
        executable=executable,
    )


def _flatten_node(
    node: DagNode,
    inherited_deps: list[str],
    out: dict[str, FlatDagNode],
) -> list[str]:
    """Flatten nested node tree into executable nodes with explicit edges."""
    base_deps = _dedupe(inherited_deps + node.depends_on)

    current_terminals = list(base_deps)
    anchor_deps = list(base_deps)

    if node.executable:
        if node.id in out:
            raise DagValidationError(f"Duplicate node id '{node.id}'.")
        if node.id in base_deps:
            raise DagValidationError(f"Node '{node.id}' cannot depend on itself.")
        out[node.id] = FlatDagNode(
            id=node.id,
            agent_type=node.agent_type,
            prompt=node.prompt,
            depends_on=base_deps,
            constraints=node.constraints,
            forbidden_paths=node.forbidden_paths,
            success_criteria=node.success_criteria,
            allowed_tools=node.allowed_tools,
        )
        anchor_deps = [node.id]
        current_terminals = [node.id]

    if not node.children:
        return current_terminals

    child_start_deps = anchor_deps if anchor_deps else base_deps
    if node.children_mode == "parallel":
        child_terminals: list[str] = []
        for child in node.children:
            child_terminals.extend(_flatten_node(child, child_start_deps, out))
        return _dedupe(child_terminals) or current_terminals

    seq_deps = child_start_deps
    seq_terminals: list[str] = seq_deps
    for child in node.children:
        seq_terminals = _flatten_node(child, seq_deps, out)
        seq_deps = seq_terminals or seq_deps
    return _dedupe(seq_terminals) or current_terminals


def _topological_levels(nodes_by_id: dict[str, FlatDagNode]) -> list[list[str]]:
    """Return topological levels; raise if cycle exists."""
    indegree: dict[str, int] = {node_id: len(n.depends_on) for node_id, n in nodes_by_id.items()}
    edges: dict[str, list[str]] = {node_id: [] for node_id in nodes_by_id}
    for node_id, node in nodes_by_id.items():
        for dep in node.depends_on:
            edges.setdefault(dep, []).append(node_id)

    ready = sorted([node_id for node_id, deg in indegree.items() if deg == 0])
    levels: list[list[str]] = []
    visited = 0

    while ready:
        level = list(ready)
        levels.append(level)
        next_ready: list[str] = []
        for src in level:
            visited += 1
            for dst in sorted(edges.get(src, [])):
                indegree[dst] -= 1
                if indegree[dst] == 0:
                    next_ready.append(dst)
        ready = sorted(next_ready)

    if visited != len(nodes_by_id):
        raise DagValidationError("Planner DAG contains a cycle.")
    return levels


def _as_str_list(value: Any) -> list[str]:
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
