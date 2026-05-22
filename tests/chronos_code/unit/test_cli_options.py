"""CLI option coverage tests."""

from click.testing import CliRunner

from chronos_code.cli import _create_chronos_context, main


def test_help_includes_show_events_option() -> None:
    """CLI help should expose the intermediate progress-events toggle."""
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "--show-events / --no-show-events" in result.output
    assert "--log TEXT" in result.output
    assert "--fresh" in result.output
    assert "--weak-snapshot" in result.output
    assert "--worker-runtime" in result.output
    assert "--worker-timeout-sec INTEGER" in result.output
    assert "--embedded-mcp / --no-embedded-mcp" in result.output


def test_create_chronos_context_accepts_weak_snapshot_flag(tmp_path) -> None:
    """ChronosContext factory should propagate weak snapshot configuration."""
    strong_ctx = _create_chronos_context(tmp_path, weak_snapshot=False)
    weak_ctx = _create_chronos_context(tmp_path, weak_snapshot=True)

    assert getattr(strong_ctx, "weak_snapshot", False) is False
    assert getattr(weak_ctx, "weak_snapshot", False) is True
