"""AskUser tool — structured question-asking with options."""

from __future__ import annotations

from typing import Any, Callable

from pydantic import BaseModel, Field


class AskUserOption(BaseModel):
    """A single option for a question."""

    label: str = Field(description="Option label text.")
    description: str | None = Field(
        default=None, description="Optional description."
    )


class AskUserInput(BaseModel):
    """Input schema for the AskUser tool."""

    question: str = Field(description="The question to ask the user.")
    options: list[AskUserOption] | None = Field(
        default=None,
        description="Options for the user to choose from.",
    )
    allow_free_text: bool = Field(
        default=True,
        description="Whether to allow free-text input in addition to options.",
    )


class AskUserTool:
    """Ask the user structured questions during agent execution.

    - Can present options for the user to choose from.
    - Always allows free-text input (via 'Other').
    - Blocks the agent loop until the user responds.

    Requires a ``prompt_fn`` callback that handles the actual user
    interaction (CLI prompt, UI dialog, etc.).
    """

    name: str = "ask_user"
    description: str = (
        "Ask the user a question. Can provide options to choose from. "
        "Use to clarify requirements, get preferences, or offer choices."
    )
    args_schema = AskUserInput

    def __init__(
        self,
        prompt_fn: Callable[[str, list[dict[str, Any]] | None, bool], str] | None = None,
    ) -> None:
        self._prompt_fn = prompt_fn or self._default_prompt

    @staticmethod
    def _default_prompt(
        question: str,
        options: list[dict[str, Any]] | None,
        allow_free_text: bool,
    ) -> str:
        """Default prompt that returns a placeholder (for testing)."""
        return "(no user input available)"

    def run(
        self,
        question: str,
        options: list[dict[str, Any]] | list[AskUserOption] | None = None,
        allow_free_text: bool = True,
    ) -> str:
        """Execute the ask_user tool."""
        # Normalize options to dicts
        opts: list[dict[str, Any]] | None = None
        if options:
            opts = []
            for o in options:
                if isinstance(o, AskUserOption):
                    opts.append(o.model_dump())
                elif isinstance(o, dict):
                    opts.append(o)

        return self._prompt_fn(question, opts, allow_free_text)
