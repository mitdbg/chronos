"""Unit tests for CLI event labeling and fresh-state preparation."""

from pathlib import Path

from chronos_code.cli import _build_txn_labeler, _prepare_chronos_state_dir


def test_build_txn_labeler_assigns_stable_sequential_labels() -> None:
    label = _build_txn_labeler()

    assert label(None) == "-"
    assert label("") == "-"
    assert label("txn42") == "txn42"
    assert label("txn42_3") == "txn42_3"
    assert label("txn_alpha") == "txn1"
    assert label("txn_beta") == "txn2"
    # Stable mapping for repeated IDs.
    assert label("txn_alpha") == "txn1"
    assert label("txn_beta") == "txn2"


def test_prepare_chronos_state_dir_creates_directory(tmp_path: Path) -> None:
    state_dir = _prepare_chronos_state_dir(tmp_path, fresh=False)

    assert state_dir == tmp_path / ".chronos-code"
    assert state_dir.exists()
    assert state_dir.is_dir()


def test_prepare_chronos_state_dir_fresh_cleans_existing_state(tmp_path: Path) -> None:
    existing = tmp_path / ".chronos-code"
    existing.mkdir(parents=True, exist_ok=True)
    marker = existing / "old.txt"
    marker.write_text("stale")

    state_dir = _prepare_chronos_state_dir(tmp_path, fresh=True)

    assert state_dir.exists()
    assert state_dir.is_dir()
    assert not marker.exists()
