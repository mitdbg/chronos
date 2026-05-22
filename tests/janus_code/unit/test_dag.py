"""Unit tests for DAG normalization and validation."""

from __future__ import annotations

import pytest

from janus_code.agent.dag import DagValidationError, normalize_planner_dag


def test_normalize_parallel_nodes():
    plan = normalize_planner_dag(
        {
            "dag": {
                "mode": "parallel",
                "nodes": [
                    {"id": "a", "agent_type": "explore", "prompt": "read a"},
                    {"id": "b", "agent_type": "implement", "prompt": "write b"},
                ],
            }
        },
        user_input="x",
    )
    assert set(plan.nodes_by_id.keys()) == {"a", "b"}
    assert len(plan.topo_levels) == 1
    assert set(plan.topo_levels[0]) == {"a", "b"}


def test_normalize_nested_children_sequence():
    plan = normalize_planner_dag(
        {
            "dag": {
                "mode": "parallel",
                "nodes": [
                    {
                        "id": "root",
                        "agent_type": "implement",
                        "prompt": "root",
                        "children_mode": "sequential",
                        "children": [
                            {"id": "c1", "agent_type": "implement", "prompt": "c1"},
                            {"id": "c2", "agent_type": "implement", "prompt": "c2"},
                        ],
                    }
                ],
            }
        },
        user_input="x",
    )
    assert "root" in plan.nodes_by_id
    assert plan.nodes_by_id["c1"].depends_on == ["root"]
    assert plan.nodes_by_id["c2"].depends_on == ["c1"]


def test_cycle_detection():
    with pytest.raises(DagValidationError, match="cycle"):
        normalize_planner_dag(
            {
                "dag": {
                    "nodes": [
                        {"id": "a", "agent_type": "implement", "prompt": "a", "depends_on": ["b"]},
                        {"id": "b", "agent_type": "implement", "prompt": "b", "depends_on": ["a"]},
                    ]
                }
            },
            user_input="x",
        )


def test_legacy_inline_subtasks_to_sequential_dag():
    plan = normalize_planner_dag(
        {
            "inline_subtasks": [
                {"agent_type": "explore", "task": "step 1"},
                {"agent_type": "implement", "task": "step 2"},
            ]
        },
        user_input="x",
    )
    assert len(plan.nodes_by_id) == 2
    assert plan.nodes_by_id["n2"].depends_on == ["n1"]
