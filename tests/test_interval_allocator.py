from __future__ import annotations

import pytest

from chronos_core.branching import ChronosBranchContext
from chronos_core.branching._common import interval_reserve_bits_for_coordinate_width


def _current_segment(ctx: ChronosBranchContext, branch_id: str) -> dict[str, int]:
    row = ctx.db.execute(
        """
        SELECT s.live_lo, s.live_hi, b.child_count
        FROM _chronos_branch_interval_branches AS b
        JOIN _chronos_branch_interval_segments AS s
          ON s.segment_id = b.current_segment_id
        WHERE b.branch_id = ?
        """,
        (branch_id,),
    ).fetchone()
    assert row is not None
    return {
        "live_lo": int(row["live_lo"]),
        "live_hi": int(row["live_hi"]),
        "child_count": int(row["child_count"]),
    }


def _initial_segment(ctx: ChronosBranchContext, branch_id: str) -> dict[str, int]:
    row = ctx.db.execute(
        """
        SELECT live_lo, live_hi
        FROM _chronos_branch_interval_segments
        WHERE owner_branch_id = ? AND segment_kind = 'mutable'
        ORDER BY segment_id
        LIMIT 1
        """,
        (branch_id,),
    ).fetchone()
    assert row is not None
    return {"live_lo": int(row["live_lo"]), "live_hi": int(row["live_hi"])}


def _segment(ctx: ChronosBranchContext, segment_id: int) -> dict[str, int]:
    row = ctx.db.execute(
        """
        SELECT live_lo, live_hi
        FROM _chronos_branch_interval_segments
        WHERE segment_id = ?
        """,
        (segment_id,),
    ).fetchone()
    assert row is not None
    return {"live_lo": int(row["live_lo"]), "live_hi": int(row["live_hi"])}


def _width(segment: dict[str, int]) -> int:
    return segment["live_hi"] - segment["live_lo"]


@pytest.mark.parametrize(
    ("width", "expected"),
    [
        (32, (10, 5, 3)),
        (64, (20, 10, 6)),
        (128, (40, 20, 12)),
        (256, (80, 40, 24)),
        (512, (160, 80, 48)),
        (1024, (320, 160, 96)),
    ],
)
def test_reserve_bits_scale_with_coordinate_width(
    width: int, expected: tuple[int, int, int]
) -> None:
    assert interval_reserve_bits_for_coordinate_width(width, "postgres") == expected


def test_default_allocator_uses_native_default_domain_width() -> None:
    assert interval_reserve_bits_for_coordinate_width(0, "postgres") == (33, 17, 10)
    assert interval_reserve_bits_for_coordinate_width(0, "sqlite") == (20, 10, 6)

    ctx = ChronosBranchContext.connect("sqlite:///:memory:", backend="interval")
    try:
        backend = ctx._backend  # type: ignore[attr-defined]
        assert backend.reserve_bits == interval_reserve_bits_for_coordinate_width(
            0, "sqlite"
        )
        assert backend.harmonic_reserve == 8
    finally:
        ctx.close()


def test_shallow_allocator_reserves_successive_depth_levels() -> None:
    ctx = ChronosBranchContext.connect(
        "sqlite:///:memory:",
        backend="interval",
        interval_reserve_bits=(4, 3, 2),
        interval_harmonic_reserve=2,
    )
    try:
        main_initial = _current_segment(ctx, "main")
        ctx.create_branch("depth_1", from_branch="main", fanout=64)
        depth_1_initial = _initial_segment(ctx, "depth_1")
        assert _width(depth_1_initial) == (_width(main_initial) - 1) >> 4

        ctx.create_branch("depth_2", from_branch="depth_1", fanout=64)
        depth_2_initial = _initial_segment(ctx, "depth_2")
        assert _width(depth_2_initial) == (_width(depth_1_initial) - 1) >> 3

        ctx.create_branch("depth_3", from_branch="depth_2", fanout=64)
        depth_3_initial = _initial_segment(ctx, "depth_3")
        assert _width(depth_3_initial) == (_width(depth_2_initial) - 1) >> 2
    finally:
        ctx.close()


def test_known_fanout_allocates_equal_slices_from_original_branch_interval() -> None:
    ctx = ChronosBranchContext.connect(
        "sqlite:///:memory:",
        backend="interval",
        interval_reserve_bits=(4, 3, 2),
        interval_harmonic_reserve=2,
    )
    try:
        ctx.create_branch("depth_1", from_branch="main", fanout=1)
        ctx.create_branch("depth_2", from_branch="depth_1", fanout=1)
        ctx.create_branch("depth_3", from_branch="depth_2", fanout=1)
        original_width = _width(_initial_segment(ctx, "depth_3")) - 1

        for index in range(4):
            ctx.create_branch(f"known_{index}", from_branch="depth_3", fanout=4)

        rows = ctx.db.execute(
            """
            SELECT live_lo, live_hi
            FROM _chronos_branch_interval_segments
            WHERE owner_branch_id LIKE 'known_%' AND segment_kind = 'mutable'
            ORDER BY owner_branch_id
            """
        ).fetchall()
        widths = [int(row["live_hi"]) - int(row["live_lo"]) for row in rows]
        assert widths == [original_width // 5] * 4
    finally:
        ctx.close()


def test_unknown_deep_fanout_uses_harmonic_reserve_and_child_count() -> None:
    ctx = ChronosBranchContext.connect(
        "sqlite:///:memory:",
        backend="interval",
        interval_reserve_bits=(4, 3, 2),
        interval_harmonic_reserve=2,
    )
    try:
        ctx.create_branch("depth_1", from_branch="main", fanout=1)
        ctx.create_branch("depth_2", from_branch="depth_1", fanout=1)
        ctx.create_branch("depth_3", from_branch="depth_2", fanout=1)

        before_first = _current_segment(ctx, "depth_3")
        ctx.create_branch("unknown_0", from_branch="depth_3")
        first = _initial_segment(ctx, "unknown_0")
        expected_first = (before_first["live_hi"] - before_first["live_lo"] - 1) // 3
        assert _width(first) == expected_first

        before_second = _current_segment(ctx, "depth_3")
        assert before_second["child_count"] == 1
        ctx.create_branch("unknown_1", from_branch="depth_3")
        second = _initial_segment(ctx, "unknown_1")
        expected_second = (before_second["live_hi"] - before_second["live_lo"] - 1) // 4
        assert _width(second) == expected_second
        assert _width(second) < _width(first)
    finally:
        ctx.close()


def test_checkpoint_uses_unknown_fanout_harmonic_allocation() -> None:
    ctx = ChronosBranchContext.connect(
        "sqlite:///:memory:",
        backend="interval",
        interval_reserve_bits=(4, 3, 2),
        interval_harmonic_reserve=2,
    )
    try:
        ctx.create_branch("work", from_branch="main", fanout=1)

        before_first = _current_segment(ctx, "work")
        first = ctx.create_checkpoint("snap_0", branch="work")
        first_snapshot = _segment(ctx, int(first.ref))
        expected_first = (
            before_first["live_hi"] - before_first["live_lo"] - 1
        ) // 3
        assert _width(first_snapshot) == expected_first

        before_second = _current_segment(ctx, "work")
        second = ctx.create_checkpoint("snap_1", branch="work")
        second_snapshot = _segment(ctx, int(second.ref))
        expected_second = (
            before_second["live_hi"] - before_second["live_lo"] - 1
        ) // 4
        assert _width(second_snapshot) == expected_second
    finally:
        ctx.close()


def test_allocator_rejects_invalid_reserve_order_and_harmonic_value() -> None:
    with pytest.raises(ValueError, match="non-increasing"):
        ChronosBranchContext.connect(
            "sqlite:///:memory:",
            backend="interval",
            interval_reserve_bits=(4, 5, 2),
        )
    with pytest.raises(ValueError, match="positive"):
        ChronosBranchContext.connect(
            "sqlite:///:memory:",
            backend="interval",
            interval_harmonic_reserve=0,
        )
