"""Unit tests for AskUser tool."""

import pytest

from chronos_code.tools.ask_user import AskUserTool


class TestAskUser:
    def test_basic_question(self):
        responses = []

        def mock_prompt(q, opts, free):
            responses.append(q)
            return "yes"

        tool = AskUserTool(prompt_fn=mock_prompt)
        result = tool.run("Continue?")
        assert result == "yes"
        assert responses == ["Continue?"]

    def test_with_options(self):
        def mock_prompt(q, opts, free):
            assert opts is not None
            assert len(opts) == 2
            return opts[0]["label"]

        tool = AskUserTool(prompt_fn=mock_prompt)
        result = tool.run(
            "Choose:",
            options=[
                {"label": "Option A", "description": "First option"},
                {"label": "Option B"},
            ],
        )
        assert result == "Option A"

    def test_free_text(self):
        def mock_prompt(q, opts, free):
            assert free is True
            return "custom input"

        tool = AskUserTool(prompt_fn=mock_prompt)
        result = tool.run("Describe:", allow_free_text=True)
        assert result == "custom input"

    def test_default_prompt(self):
        tool = AskUserTool()
        result = tool.run("test?")
        assert "no user input" in result

    def test_pydantic_option_objects(self):
        from chronos_code.tools.ask_user import AskUserOption

        def mock_prompt(q, opts, free):
            return opts[0]["label"]

        tool = AskUserTool(prompt_fn=mock_prompt)
        result = tool.run(
            "Pick:",
            options=[AskUserOption(label="A"), AskUserOption(label="B")],
        )
        assert result == "A"
