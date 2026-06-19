from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from tests.test_branching import _postgres_dsn


def _load_hotel_booking_module():
    module_name = "_chronos_bench_hotel_booking_agent"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    module_path = (
        Path(__file__).resolve().parents[1]
        / "bench"
        / "agent_workloads"
        / "hotel_booking_agent.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _request(transaction_type: str = "book", nights: int = 2):
    bench = _load_hotel_booking_module()
    return bench.HotelRequest(
        request_id="req_test",
        transaction_type=transaction_type,
        hotel_id="hotel_001",
        room_type_id="standard",
        guest_id="guest_001",
        checkin_date="2026-07-01",
        nights=nights,
        amount_cents=36000,
        new_rate_cents=15000,
        new_min_stay=3,
        reservation_to_cancel="seed_cancel_001",
    )


def _inventory_row(**overrides):
    row = {
        "hotel_id": "hotel_001",
        "room_type_id": "standard",
        "stay_date": "2026-07-01",
        "total_capacity": 4,
        "available_count": 2,
        "held_count": 0,
        "reserved_count": 1,
        "base_rate_cents": 18000,
        "promo_rate_cents": None,
        "last_quote_cents": None,
        "min_stay_nights": 1,
        "cleaning_hold_count": 0,
        "maintenance_blocked_count": 0,
        "last_priced_at": None,
        "policy_note": "standard policy",
    }
    row.update(overrides)
    return row


def test_default_model_is_openrouter_deepseek_v4_flash() -> None:
    bench = _load_hotel_booking_module()

    assert bench.DEFAULT_MODEL == "openrouter/deepseek/deepseek-v4-flash"


def test_live_agent_prompt_is_backend_neutral_and_general() -> None:
    bench = _load_hotel_booking_module()

    prompt = bench.build_hotel_agent_prompt(_request("book"))

    assert "Use the available tools to satisfy the user's request" in prompt
    assert "Choose the tool calls yourself" in prompt
    assert "Never call a tool with empty arguments" in prompt
    assert "inspect_stay before mutating" not in prompt
    assert "at most once" not in prompt
    assert "User request: I'm Guest 001." in prompt
    assert "Chronos Hotel 001 (hotel_001)" in prompt
    assert "Standard King (standard)" in prompt
    assert "backend internals" in prompt
    assert "transaction" not in prompt.lower()
    assert "Request id:" not in prompt
    assert "Type:" not in prompt
    assert "Room type:" not in prompt
    assert "big_txn" not in prompt
    assert "saga" not in prompt
    assert "branch" not in prompt


def test_tool_contract_errors_are_not_transaction_retries() -> None:
    bench = _load_hotel_booking_module()

    assert bench.is_transaction_abort("AgentToolContractError: AgentToolCallError: Argument hotel_id is required") is False


def test_chronos_internal_interval_duplicate_is_transaction_retry() -> None:
    bench = _load_hotel_booking_module()

    assert bench.is_transaction_abort(
        'UniqueViolation: duplicate key value violates unique constraint "_chronos_b_interval_room_inventory_pkey"'
    ) is True
    assert bench.is_transaction_abort(
        'UniqueViolation: duplicate key value violates unique constraint "reservations_pkey"'
    ) is False


def test_same_row_booking_price_refresh_resolves_as_disjoint() -> None:
    bench = _load_hotel_booking_module()
    base = _inventory_row()
    source = _inventory_row(available_count=1, reserved_count=2)
    target = _inventory_row(
        promo_rate_cents=15000,
        last_quote_cents=15000,
        last_priced_at="req_price",
    )

    decision = bench.merge_inventory_conflict(
        request=_request("book"),
        base=base,
        source=source,
        target=target,
    )

    assert decision.resolved is True
    assert decision.reason == "same_row_disjoint"
    assert decision.merged_row["available_count"] == 1
    assert decision.merged_row["reserved_count"] == 2
    assert decision.merged_row["promo_rate_cents"] == 15000


def test_hold_policy_note_resolves_as_disjoint_policy_conflict() -> None:
    bench = _load_hotel_booking_module()
    base = _inventory_row()
    source = _inventory_row(available_count=1, held_count=1)
    target = _inventory_row(policy_note="front desk exception")

    decision = bench.merge_inventory_conflict(
        request=_request("hold"),
        base=base,
        source=source,
        target=target,
    )

    assert decision.resolved is True
    assert decision.reason == "same_row_disjoint"
    assert decision.merged_row["held_count"] == 1
    assert decision.merged_row["policy_note"] == "front desk exception"


def test_booking_cancel_resolves_with_counter_arithmetic() -> None:
    bench = _load_hotel_booking_module()
    base = _inventory_row(available_count=2, reserved_count=1)
    source = _inventory_row(available_count=1, reserved_count=2)
    target = _inventory_row(available_count=3, reserved_count=0)

    decision = bench.merge_inventory_conflict(
        request=_request("book"),
        base=base,
        source=source,
        target=target,
    )

    assert decision.resolved is True
    assert decision.reason == "counter_arithmetic"
    assert decision.merged_row["available_count"] == 2
    assert decision.merged_row["reserved_count"] == 1


def test_booking_booking_capacity_conflict_rejects() -> None:
    bench = _load_hotel_booking_module()
    base = _inventory_row(total_capacity=1, available_count=1, reserved_count=0)
    source = _inventory_row(total_capacity=1, available_count=0, reserved_count=1)
    target = _inventory_row(total_capacity=1, available_count=0, reserved_count=1)

    decision = bench.merge_inventory_conflict(
        request=_request("book"),
        base=base,
        source=source,
        target=target,
    )

    assert decision.resolved is False
    assert decision.reason in {"capacity", "same_column"}


def test_booking_rejects_when_target_min_stay_invalidates_request() -> None:
    bench = _load_hotel_booking_module()
    base = _inventory_row(min_stay_nights=1)
    source = _inventory_row(min_stay_nights=1, available_count=1, reserved_count=2)
    target = _inventory_row(min_stay_nights=3)

    decision = bench.merge_inventory_conflict(
        request=_request("book", nights=2),
        base=base,
        source=source,
        target=target,
    )

    assert decision.resolved is False
    assert decision.reason == "semantic_policy"


def test_saga_compensation_undos_committed_agent_trace_steps() -> None:
    bench = _load_hotel_booking_module()
    dsn = _postgres_dsn()
    bench.setup_hotel_database(
        "saga",
        dsn,
        hotel_count=1,
        rooms_per_hotel=8,
        date_count=3,
    )
    db = bench.connect_sql_database(dsn)
    metrics = bench.AttemptMetrics("saga", "req_saga_abort", "book")
    request = bench.HotelRequest(
        request_id="req_saga_abort",
        transaction_type="book",
        hotel_id="hotel_001",
        room_type_id="standard",
        guest_id="guest_001",
        checkin_date="2026-07-01",
        nights=2,
        amount_cents=36000,
    )
    try:
        store = bench.SagaHotelBookingStore(db, metrics=metrics)
        before = [
            store.get_inventory_row(request.hotel_id, request.room_type_id, stay_date)
            for stay_date in bench.stay_dates(request)
        ]
        room_id = store.available_rooms(
            request.hotel_id,
            request.room_type_id,
            request.checkin_date,
            request.nights,
        )[0]["room_id"]

        reservation_id = store.create_reservation(request, status="pending", room_id=room_id)
        store.reserve_inventory(request, reservation_id, room_id)
        payment_id = store.record_payment(reservation_id, request.amount_cents)

        assert store.saga_trace == [
            "create_reservation",
            "reserve_inventory",
            "record_payment",
        ]

        store.compensate()

        after = [
            store.get_inventory_row(request.hotel_id, request.room_type_id, stay_date)
            for stay_date in bench.stay_dates(request)
        ]
        reservation_count = db.execute(
            "SELECT COUNT(*) AS count FROM reservations WHERE reservation_id = :reservation_id",
            {"reservation_id": reservation_id},
        ).fetchone()["count"]
        payment_count = db.execute(
            "SELECT COUNT(*) AS count FROM payments WHERE payment_id = :payment_id",
            {"payment_id": payment_id},
        ).fetchone()["count"]
        occupancy_count = db.execute(
            "SELECT COUNT(*) AS count FROM room_occupancy WHERE reservation_id = :reservation_id",
            {"reservation_id": reservation_id},
        ).fetchone()["count"]

        assert after == before
        assert reservation_count == 0
        assert payment_count == 0
        assert occupancy_count == 0
        assert metrics.compensation_attempts == 3
        assert metrics.compensation_failures == 0
    finally:
        db.close()


def test_physical_room_occupancy_prevents_double_booking() -> None:
    bench = _load_hotel_booking_module()
    dsn = _postgres_dsn()
    bench.setup_hotel_database(
        "big_txn",
        dsn,
        hotel_count=1,
        rooms_per_hotel=8,
        date_count=3,
    )
    db = bench.connect_sql_database(dsn)
    metrics = bench.AttemptMetrics("big_txn", "req_double_booking", "book")
    try:
        store = bench.HotelBookingStore(db, metrics=metrics)
        first = bench.HotelRequest(
            request_id="req_first",
            transaction_type="book",
            hotel_id="hotel_001",
            room_type_id="standard",
            guest_id="guest_001",
            checkin_date="2026-07-01",
            nights=2,
            amount_cents=36000,
        )
        second = bench.HotelRequest(
            request_id="req_second",
            transaction_type="book",
            hotel_id="hotel_001",
            room_type_id="standard",
            guest_id="guest_002",
            checkin_date="2026-07-01",
            nights=2,
            amount_cents=36000,
        )
        room_id = store.available_rooms(
            first.hotel_id,
            first.room_type_id,
            first.checkin_date,
            first.nights,
        )[0]["room_id"]

        reservation_id = store.create_reservation(first, status="pending", room_id=room_id)
        store.reserve_inventory(first, reservation_id, room_id)
        store.finalize_booking(reservation_id)
        db.commit()

        try:
            store.create_reservation(second, status="pending", room_id=room_id)
        except bench.AgentTransactionAborted as exc:
            assert "room_occupied" in str(exc)
        else:
            raise AssertionError("second booking unexpectedly occupied the same room")

        duplicate_rows = db.execute(
            """
            SELECT COUNT(*) AS count
            FROM (
              SELECT room_id, stay_date
              FROM room_occupancy
              GROUP BY room_id, stay_date
              HAVING COUNT(*) > 1
            ) duplicates
            """
        ).fetchone()["count"]
        assert duplicate_rows == 0
    finally:
        db.close()


def test_branch_setup_seeds_through_chronos_main_not_source_tables() -> None:
    bench = _load_hotel_booking_module()
    dsn = _postgres_dsn()
    bench.setup_hotel_database(
        "branch",
        dsn,
        hotel_count=1,
        rooms_per_hotel=8,
        date_count=3,
    )

    ctx = bench.connect_chronos(dsn)
    try:
        main = ctx.checkout("main")
        logical_inventory_rows = main.query("SELECT COUNT(*) AS count FROM room_inventory")[0]["count"]
        source_inventory_rows = ctx.db.execute(
            "SELECT COUNT(*) AS count FROM room_inventory"
        ).fetchone()["count"]

        assert logical_inventory_rows == 6
        assert source_inventory_rows == 0
    finally:
        ctx.close()


def test_branch_snapshot_isolation_resolves_disjoint_written_row_conflict() -> None:
    bench = _load_hotel_booking_module()
    dsn = _postgres_dsn()
    bench.setup_hotel_database(
        "branch",
        dsn,
        hotel_count=1,
        rooms_per_hotel=8,
        date_count=3,
    )
    request = _request("book", nights=1)
    metrics = bench.AttemptMetrics("branch", request.request_id, request.transaction_type)
    ctx = bench.connect_chronos(dsn)
    branch_id = "si_source_disjoint"
    touched_inventory = {}
    try:
        main = ctx.checkout("main")
        base_row = bench.HotelBookingStore(main, metrics=metrics).get_inventory_row(
            "hotel_001",
            "standard",
            "2026-07-01",
        )
        touched_inventory[("hotel_001", "standard", "2026-07-01")] = base_row
        ctx.create_branch(branch_id, from_branch="main")
        source = ctx.checkout(branch_id)
        with source.transaction():
            source.execute(
                """
                UPDATE room_inventory
                SET available_count = available_count - 1,
                    reserved_count = reserved_count + 1
                WHERE hotel_id = 'hotel_001'
                  AND room_type_id = 'standard'
                  AND stay_date = '2026-07-01'
                """
            )
        with main.transaction():
            main.execute(
                """
                UPDATE room_inventory
                SET promo_rate_cents = 15000,
                    last_quote_cents = 15000,
                    last_priced_at = 'price_refresh'
                WHERE hotel_id = 'hotel_001'
                  AND room_type_id = 'standard'
                  AND stay_date = '2026-07-01'
                """
            )
    finally:
        ctx.close()

    attempt = bench.BranchAttempt(
        request=request,
        branch_id=branch_id,
        metrics=metrics,
        touched_inventory=touched_inventory,
        completed=True,
    )
    result = bench.merge_branch_attempts(
        dsn,
        [attempt],
        scripted_agent=True,
        model=bench.DEFAULT_MODEL,
        max_agent_steps=1,
        max_retries=0,
    )[0]

    ctx = bench.connect_chronos(dsn)
    try:
        row = bench.HotelBookingStore(ctx.checkout("main"), metrics=metrics).get_inventory_row(
            "hotel_001",
            "standard",
            "2026-07-01",
        )
        assert result.success is True
        assert result.merge_conflicts == 1
        assert result.same_row_disjoint_resolved == 1
        assert row["reserved_count"] == 2
        assert row["promo_rate_cents"] == 15000
    finally:
        ctx.close()


def test_branch_snapshot_isolation_rejects_unmergeable_capacity_conflict() -> None:
    bench = _load_hotel_booking_module()
    dsn = _postgres_dsn()
    bench.setup_hotel_database(
        "branch",
        dsn,
        hotel_count=1,
        rooms_per_hotel=8,
        date_count=3,
    )
    request = _request("book", nights=1)
    metrics = bench.AttemptMetrics("branch", request.request_id, request.transaction_type)
    ctx = bench.connect_chronos(dsn)
    branch_id = "si_source_capacity"
    touched_inventory = {}
    try:
        main = ctx.checkout("main")
        with main.transaction():
            main.execute(
                """
                UPDATE room_inventory
                SET total_capacity = 1,
                    available_count = 1,
                    reserved_count = 0,
                    held_count = 0,
                    cleaning_hold_count = 0,
                    maintenance_blocked_count = 0
                WHERE hotel_id = 'hotel_001'
                  AND room_type_id = 'standard'
                  AND stay_date = '2026-07-01'
                """
            )
        base_row = bench.HotelBookingStore(main, metrics=metrics).get_inventory_row(
            "hotel_001",
            "standard",
            "2026-07-01",
        )
        touched_inventory[("hotel_001", "standard", "2026-07-01")] = base_row
        ctx.create_branch(branch_id, from_branch="main")
        source = ctx.checkout(branch_id)
        main = ctx.checkout("main")
        with source.transaction():
            source.execute(
                """
                UPDATE room_inventory
                SET available_count = 0,
                    reserved_count = 1
                WHERE hotel_id = 'hotel_001'
                  AND room_type_id = 'standard'
                  AND stay_date = '2026-07-01'
                """
            )
        with main.transaction():
            main.execute(
                """
                UPDATE room_inventory
                SET available_count = 0,
                    maintenance_blocked_count = 1
                WHERE hotel_id = 'hotel_001'
                  AND room_type_id = 'standard'
                  AND stay_date = '2026-07-01'
                """
            )
    finally:
        ctx.close()

    attempt = bench.BranchAttempt(
        request=request,
        branch_id=branch_id,
        metrics=metrics,
        touched_inventory=touched_inventory,
        completed=True,
    )
    result = bench.merge_branch_attempts(
        dsn,
        [attempt],
        scripted_agent=True,
        model=bench.DEFAULT_MODEL,
        max_agent_steps=1,
        max_retries=0,
    )[0]

    assert result.success is False
    assert result.merge_conflicts == 1
    assert result.merge_rejected == 1
    assert result.abort_reason == "capacity"


def test_branch_runner_merges_each_attempt_immediately(monkeypatch) -> None:
    bench = _load_hotel_booking_module()
    calls = []

    def fake_run_branch_attempt(
        database_url,
        request,
        *,
        scripted_agent,
        model,
        max_agent_steps,
        barrier,
        branch_create_lock=None,
    ):
        assert branch_create_lock is not None
        calls.append(("prepare", request.request_id))
        metrics = bench.AttemptMetrics("branch", request.request_id, request.transaction_type)
        metrics.workflow_attempts = 1
        return bench.BranchAttempt(
            request=request,
            branch_id=f"branch_{request.request_id}",
            metrics=metrics,
            completed=True,
        )

    def fake_merge_prepared_branch_attempt(database_url, attempt, *, merge_lock=None):
        assert merge_lock is not None
        calls.append(("merge", attempt.request.request_id))
        attempt.metrics.success = True
        return False

    monkeypatch.setattr(bench, "run_branch_attempt", fake_run_branch_attempt)
    monkeypatch.setattr(bench, "merge_prepared_branch_attempt", fake_merge_prepared_branch_attempt)

    requests = [
        bench.HotelRequest(
            request_id=f"req_{index:04d}",
            transaction_type="book",
            hotel_id="hotel_001",
            room_type_id="standard",
            guest_id="guest_001",
            checkin_date="2026-07-01",
            nights=1,
            amount_cents=18000,
        )
        for index in range(2)
    ]

    rows = bench.run_parallel_metrics(
        "branch",
        "postgresql://unused",
        requests,
        scripted_agent=True,
        model=bench.DEFAULT_MODEL,
        max_agent_steps=1,
        max_retries=0,
        parallel_agents=1,
    )

    assert [row.success for row in rows] == [True, True]
    assert calls == [
        ("prepare", "req_0000"),
        ("merge", "req_0000"),
        ("prepare", "req_0001"),
        ("merge", "req_0001"),
    ]
