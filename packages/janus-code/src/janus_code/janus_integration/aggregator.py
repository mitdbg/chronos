"""Aggregator — merges results from parallel transactions.

After N parallel transactions commit independently, the aggregator
runs in a fresh transaction, reads all committed results, and
produces the final workspace state.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from janus_code.janus_integration.parallel_executor import AgentResult, AgentTask
from janus_code.janus_integration.session_manager import SessionManager, TxnContext

logger = logging.getLogger(__name__)


@dataclass
class AggregatorResult:
    """Result from the aggregator transaction."""

    success: bool
    summary: str
    merged_count: int = 0
    error: str | None = None


class Aggregator:
    """Runs after parallel transactions to merge their results.

    The aggregator transaction:
    1. Reads committed results from all parallel agents.
    2. Synthesizes/merges as needed (e.g., combining separate file edits).
    3. Produces the final workspace state.
    4. Commits the aggregator transaction.

    If aggregation fails, the aggregator transaction is aborted,
    but the parallel transactions' commits remain.
    """

    def __init__(
        self,
        session: SessionManager,
        merge_fn: Callable[[list[AgentResult], TxnContext], str] | None = None,
        llm_fn: Callable[[list[dict[str, Any]], list[dict[str, Any]]], dict[str, Any]]
        | None = None,
        on_llm_usage: Callable[[dict[str, Any] | None], None] | None = None,
    ) -> None:
        self.session = session
        self._merge_fn = merge_fn
        self._llm_fn = llm_fn
        self._on_llm_usage = on_llm_usage

    async def aggregate(
        self,
        results: list[AgentResult],
        on_summary: Callable[[str, TxnContext], None | Awaitable[None]] | None = None,
    ) -> AggregatorResult:
        """Run the aggregator transaction on parallel results.

        Args:
            results: Results from parallel agent executions
            on_summary: Optional callback invoked inside the aggregator
                transaction before commit.

        Returns:
            AggregatorResult with success/failure and summary
        """
        # Filter to successful results
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        if not successful:
            return AggregatorResult(
                success=False,
                summary="No successful parallel results to aggregate.",
                error="All parallel tasks failed.",
            )

        # Begin aggregator transaction
        txn = self.session.begin_aggregator_txn()

        try:
            if self._merge_fn:
                summary_or_awaitable = self._merge_fn(successful, txn)
                if hasattr(summary_or_awaitable, "__await__"):
                    summary = await summary_or_awaitable
                else:
                    summary = str(summary_or_awaitable)
            elif self._llm_fn:
                summary = await self._llm_merge(successful, failed)
            else:
                summary = self._default_merge(successful, failed)

            if on_summary is not None:
                cb_result = on_summary(summary, txn)
                if hasattr(cb_result, "__await__"):
                    await cb_result

            self.session.commit_txn(txn)

            return AggregatorResult(
                success=True,
                summary=summary,
                merged_count=len(successful),
            )

        except Exception as e:
            self.session.abort_txn(txn)
            return AggregatorResult(
                success=False,
                summary=f"Aggregation failed: {e}",
                error=str(e),
            )

    async def _llm_merge(
        self,
        successful: list[AgentResult],
        failed: list[AgentResult],
    ) -> str:
        """Use an LLM as the aggregator agent to synthesize final results.

        Falls back to deterministic concatenation if the LLM returns empty
        content or raises an error.
        """
        payload = {
            "successful": [
                {
                    "task": r.task.description,
                    "agent_type": r.task.agent_type,
                    "summary": r.summary,
                    "txn_id": r.txn_id,
                    "attempts": r.attempts,
                }
                for r in successful
            ],
            "failed": [
                {
                    "task": r.task.description,
                    "agent_type": r.task.agent_type,
                    "error": r.error,
                    "txn_id": r.txn_id,
                    "attempts": r.attempts,
                }
                for r in failed
            ],
        }
        messages = [
            {
                "role": "system",
                "content": (
                    "You are the Janus-Code aggregator agent. "
                    "Merge parallel agent outputs into one concise final "
                    "assistant response for the user. Synthesize, deduplicate, "
                    "and call out unresolved failures explicitly."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Semantically merge these results:\n\n"
                    + json.dumps(payload, ensure_ascii=True, indent=2)
                ),
            },
        ]

        try:
            response = self._llm_fn(messages, [])
            if hasattr(response, "__await__"):
                response = await response
            if self._on_llm_usage is not None and isinstance(response, dict):
                self._on_llm_usage(response.get("usage"))
            content = str(response.get("content", "")).strip()
            if content:
                return content
            logger.warning(
                "Aggregator LLM returned empty content; falling back to deterministic merge."
            )
        except Exception as e:
            logger.warning(
                "Aggregator LLM merge failed (%s); falling back to deterministic merge.",
                e,
            )

        return self._default_merge(successful, failed)

    def _default_merge(
        self,
        successful: list[AgentResult],
        failed: list[AgentResult],
    ) -> str:
        """Default merge: concatenate summaries."""
        parts: list[str] = []
        parts.append(f"## Aggregated Results ({len(successful)} successful)")

        for r in successful:
            parts.append(f"\n### {r.task.description}")
            parts.append(r.summary)

        if failed:
            parts.append(f"\n## Failed Tasks ({len(failed)})")
            for r in failed:
                parts.append(f"- {r.task.description}: {r.error}")

        return "\n".join(parts)
