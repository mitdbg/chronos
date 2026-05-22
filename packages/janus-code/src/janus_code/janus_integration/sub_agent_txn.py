"""SubAgentTxn — sequential subtransaction management for sub-agents.

Within a single parent transaction, sub-agents run SEQUENTIALLY as
subtransactions. Each subtxn sees the results of prior siblings
(because prior siblings' commits merged into the parent).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from janus_code.agent.sub_agents import SubAgentConfig, filter_tools, get_sub_agent_config
from janus_code.janus_integration.session_manager import TxnContext

logger = logging.getLogger(__name__)


@dataclass
class SubAgentResult:
    """Result from a sub-agent execution."""

    agent_type: str
    success: bool
    summary: str
    error: str | None = None
    txn_id: str = ""


class SubAgentTxn:
    """Manages sequential sub-agent execution within a parent transaction.

    For each sub-agent step:
    1. Begin a child subtransaction of the parent.
    2. Run the sub-agent with scoped tools and isolated conversation.
    3. On success: commit_child → changes visible to next step.
    4. On failure: abort_child → changes discarded, parent unaffected.

    The parent transaction accumulates all committed children's results.
    """

    def __init__(
        self,
        parent_txn: TxnContext,
        all_tools: dict[str, Any],
        run_agent_fn: Callable | None = None,
        event_sink: Callable[..., None] | None = None,
    ) -> None:
        self.parent_txn = parent_txn
        self.all_tools = all_tools
        self._run_agent_fn = run_agent_fn
        self._event_sink = event_sink
        self._results: list[SubAgentResult] = []

    @property
    def results(self) -> list[SubAgentResult]:
        return list(self._results)

    async def run_step(
        self,
        agent_type: str,
        task_description: str,
        prior_results: list[SubAgentResult] | None = None,
    ) -> SubAgentResult:
        """Run a single sub-agent step in a child subtransaction.

        Args:
            agent_type: "explore", "implement", or "test"
            task_description: What the sub-agent should do
            prior_results: Results from prior steps for context

        Returns:
            SubAgentResult with success/failure and summary
        """
        config = get_sub_agent_config(agent_type)
        scoped_tools = filter_tools(self.all_tools, config.allowed_tools)

        # Begin child subtransaction
        subtxn = self.parent_txn.begin_subtxn(agent_type)
        if self._event_sink is not None:
            self._event_sink(
                "subtxn_begin",
                f"Started subtransaction `{agent_type}`.",
                txn_id=subtxn.txn_id,
                agent_type=agent_type,
            )

        try:
            if self._run_agent_fn:
                summary = await self._run_agent_fn(
                    config=config,
                    tools=scoped_tools,
                    task=task_description,
                    prior_results=prior_results,
                    txn=subtxn,
                )
            else:
                summary = f"[{agent_type}] Completed: {task_description}"

            result = SubAgentResult(
                agent_type=agent_type,
                success=True,
                summary=summary,
                txn_id=subtxn.txn_id,
            )

            # Commit child → merges into parent
            subtxn.commit()
            logger.info("Sub-agent %s committed: %s", agent_type, summary[:100])
            if self._event_sink is not None:
                self._event_sink(
                    "subtxn_commit",
                    f"Committed subtransaction `{agent_type}`.",
                    txn_id=subtxn.txn_id,
                    agent_type=agent_type,
                )

        except Exception as e:
            # Abort child → parent unaffected
            subtxn.abort()
            result = SubAgentResult(
                agent_type=agent_type,
                success=False,
                summary=f"Failed: {e}",
                error=str(e),
                txn_id=subtxn.txn_id,
            )
            logger.warning("Sub-agent %s aborted: %s", agent_type, e)
            if self._event_sink is not None:
                self._event_sink(
                    "subtxn_abort",
                    f"Aborted subtransaction `{agent_type}`.",
                    txn_id=subtxn.txn_id,
                    agent_type=agent_type,
                    error=str(e),
                )

        self._results.append(result)
        return result

    async def run_sequential(
        self,
        steps: list[tuple[str, str]],
        stop_on_failure: bool = False,
    ) -> list[SubAgentResult]:
        """Run multiple sub-agent steps sequentially.

        Each step sees prior steps' committed results.

        Args:
            steps: List of (agent_type, task_description) tuples
            stop_on_failure: If True, stop after first failure

        Returns:
            List of results for all steps
        """
        results: list[SubAgentResult] = []

        for agent_type, task_desc in steps:
            result = await self.run_step(
                agent_type=agent_type,
                task_description=task_desc,
                prior_results=list(results),  # copy to avoid mutation
            )
            results.append(result)

            if stop_on_failure and not result.success:
                logger.info(
                    "Stopping sequential execution after %s failure",
                    agent_type,
                )
                break

        return results

    async def run_explore_implement_test(
        self,
        task_description: str,
        stop_on_failure: bool = True,
    ) -> list[SubAgentResult]:
        """Run the standard explore → implement → test pipeline.

        This is the most common sub-agent sequence.
        """
        return await self.run_sequential(
            steps=[
                ("explore", f"Explore codebase for: {task_description}"),
                ("implement", f"Implement: {task_description}"),
                ("test", f"Test changes for: {task_description}"),
            ],
            stop_on_failure=stop_on_failure,
        )
