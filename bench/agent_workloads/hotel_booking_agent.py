from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import threading
import time
import uuid
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

from chronos_core.branching import ChronosBranchContext
from chronos_core.branching.sql_adapters import SQLDatabaseAdapter, connect_sql_database


BACKENDS = ("big_txn", "saga", "branch")
DEFAULT_BACKENDS = ("big_txn", "saga", "branch")
DEFAULT_MODEL = "openrouter/deepseek/deepseek-v4-flash"

INVENTORY_COLUMNS = (
    "hotel_id",
    "room_type_id",
    "stay_date",
    "total_capacity",
    "available_count",
    "held_count",
    "reserved_count",
    "base_rate_cents",
    "promo_rate_cents",
    "last_quote_cents",
    "min_stay_nights",
    "cleaning_hold_count",
    "maintenance_blocked_count",
    "last_priced_at",
    "policy_note",
)
HOTEL_TABLES: dict[str, list[str]] = {
    "hotels": ["hotel_id"],
    "room_types": ["room_type_id"],
    "rooms": ["room_id"],
    "nightly_rates": ["hotel_id", "room_type_id", "stay_date"],
    "room_inventory": ["hotel_id", "room_type_id", "stay_date"],
    "guests": ["guest_id"],
    "reservations": ["reservation_id"],
    "reservation_nights": ["reservation_id", "stay_date"],
    "room_occupancy": ["room_id", "stay_date"],
    "payments": ["payment_id"],
}
DETAIL_FIELDS = [
    "backend",
    "request_id",
    "transaction_type",
    "success",
    "abort_reason",
    "workflow_attempts",
    "workflow_aborts",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "llm_calls",
    "tool_calls",
    "total_latency_ms",
    "llm_latency_ms",
    "tool_latency_ms",
    "transaction_latency_ms",
    "commit_or_merge_latency_ms",
    "merge_conflicts",
    "merge_resolved",
    "merge_rejected",
    "same_row_disjoint_resolved",
    "counter_arithmetic_resolved",
    "semantic_policy_rejected",
    "compensation_attempts",
    "compensation_failures",
]
SUMMARY_FIELDS = [
    "backend",
    "requests",
    "parallel_agents",
    "attempts",
    "commits",
    "aborts",
    "abort_rate",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "tokens_per_success",
    "merge_conflicts",
    "merge_resolved",
    "merge_rejected",
    "same_row_disjoint_resolved",
    "counter_arithmetic_resolved",
    "semantic_policy_rejected",
    "compensation_attempts",
    "compensation_failures",
    "avg_ms",
    "p50_ms",
    "p95_ms",
    "total_latency_ms",
    "llm_latency_ms",
    "tool_latency_ms",
    "transaction_latency_ms",
    "commit_or_merge_latency_ms",
]
VERIFICATION_FIELDS = [
    "backend",
    "valid",
    "inventory_rows",
    "reservation_rows",
    "payment_rows",
    "message",
]


@dataclass(frozen=True)
class HotelRequest:
    request_id: str
    transaction_type: str
    hotel_id: str
    room_type_id: str
    guest_id: str
    checkin_date: str
    nights: int
    amount_cents: int
    new_rate_cents: int | None = None
    new_min_stay: int | None = None
    reservation_to_cancel: str | None = None


@dataclass
class AttemptMetrics:
    backend: str
    request_id: str
    transaction_type: str
    success: bool = False
    abort_reason: str = ""
    workflow_attempts: int = 0
    workflow_aborts: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    llm_calls: int = 0
    tool_calls: int = 0
    total_latency_ms: float = 0.0
    llm_latency_ms: float = 0.0
    tool_latency_ms: float = 0.0
    transaction_latency_ms: float = 0.0
    commit_or_merge_latency_ms: float = 0.0
    merge_conflicts: int = 0
    merge_resolved: int = 0
    merge_rejected: int = 0
    same_row_disjoint_resolved: int = 0
    counter_arithmetic_resolved: int = 0
    semantic_policy_rejected: int = 0
    compensation_attempts: int = 0
    compensation_failures: int = 0
    agent_trace: list[dict[str, Any]] = field(default_factory=list)

    def as_row(self) -> dict[str, Any]:
        return {field_name: getattr(self, field_name) for field_name in DETAIL_FIELDS}

    def trace(self, event: str, **payload: Any) -> None:
        self.agent_trace.append(
            {
                "index": len(self.agent_trace),
                "event": event,
                "elapsed_ms": payload.pop("elapsed_ms", None),
                **payload,
            }
        )

    def as_trace_row(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "request_id": self.request_id,
            "transaction_type": self.transaction_type,
            "success": self.success,
            "abort_reason": self.abort_reason,
            "events": self.agent_trace,
        }


@dataclass
class BranchAttempt:
    request: HotelRequest
    branch_id: str
    metrics: AttemptMetrics
    touched_inventory: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    completed: bool = False


class AgentTransactionAborted(RuntimeError):
    """Raised after a live agent observes a backend transaction abort."""


class AgentToolContractError(RuntimeError):
    """Raised when the live agent calls a tool with invalid arguments."""


class Timer:
    def __init__(self) -> None:
        self.start = time.perf_counter()

    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self.start) * 1000


def parse_csv(value: str) -> list[str]:
    parsed = [part.strip() for part in value.split(",") if part.strip()]
    if not parsed:
        raise argparse.ArgumentTypeError("expected at least one value")
    return parsed


def parse_backends(value: str) -> list[str]:
    backends = parse_csv(value)
    unknown = sorted(set(backends) - set(BACKENDS))
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown backends: {', '.join(unknown)}")
    return backends


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def reset_postgres_schema(db: SQLDatabaseAdapter) -> None:
    db.execute("DROP SCHEMA IF EXISTS public CASCADE")
    db.execute("CREATE SCHEMA public")
    db.commit()


def execute_script(db: Any, statements: Iterable[str]) -> None:
    for statement in statements:
        db.execute(statement)


def create_hotel_schema(db: Any) -> None:
    execute_script(
        db,
        [
            """
            CREATE TABLE hotels (
              hotel_id TEXT PRIMARY KEY,
              city TEXT NOT NULL,
              name TEXT NOT NULL,
              star_rating INTEGER NOT NULL,
              brand TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE room_types (
              room_type_id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              beds INTEGER NOT NULL,
              max_guests INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE rooms (
              room_id TEXT PRIMARY KEY,
              hotel_id TEXT NOT NULL,
              room_type_id TEXT NOT NULL,
              floor INTEGER NOT NULL,
              view_name TEXT NOT NULL,
              bed_type TEXT NOT NULL,
              popularity_score INTEGER NOT NULL,
              status TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE nightly_rates (
              hotel_id TEXT NOT NULL,
              room_type_id TEXT NOT NULL,
              stay_date TEXT NOT NULL,
              rate_cents INTEGER NOT NULL,
              PRIMARY KEY (hotel_id, room_type_id, stay_date)
            )
            """,
            """
            CREATE TABLE room_inventory (
              hotel_id TEXT NOT NULL,
              room_type_id TEXT NOT NULL,
              stay_date TEXT NOT NULL,
              total_capacity INTEGER NOT NULL,
              available_count INTEGER NOT NULL,
              held_count INTEGER NOT NULL,
              reserved_count INTEGER NOT NULL,
              base_rate_cents INTEGER NOT NULL,
              promo_rate_cents INTEGER,
              last_quote_cents INTEGER,
              min_stay_nights INTEGER NOT NULL,
              cleaning_hold_count INTEGER NOT NULL,
              maintenance_blocked_count INTEGER NOT NULL,
              last_priced_at TEXT,
              policy_note TEXT NOT NULL,
              PRIMARY KEY (hotel_id, room_type_id, stay_date)
            )
            """,
            """
            CREATE TABLE guests (
              guest_id TEXT PRIMARY KEY,
              full_name TEXT NOT NULL,
              loyalty_tier TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE reservations (
              reservation_id TEXT PRIMARY KEY,
              guest_id TEXT NOT NULL,
              hotel_id TEXT NOT NULL,
              room_type_id TEXT NOT NULL,
              checkin_date TEXT NOT NULL,
              checkout_date TEXT NOT NULL,
              status TEXT NOT NULL,
              total_cents INTEGER NOT NULL,
              request_id TEXT NOT NULL,
              created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE reservation_nights (
              reservation_id TEXT NOT NULL,
              stay_date TEXT NOT NULL,
              room_id TEXT NOT NULL,
              hotel_id TEXT NOT NULL,
              room_type_id TEXT NOT NULL,
              rate_cents INTEGER NOT NULL,
              PRIMARY KEY (reservation_id, stay_date)
            )
            """,
            """
            CREATE TABLE room_occupancy (
              room_id TEXT NOT NULL,
              stay_date TEXT NOT NULL,
              reservation_id TEXT NOT NULL,
              hotel_id TEXT NOT NULL,
              room_type_id TEXT NOT NULL,
              occupancy_type TEXT NOT NULL,
              PRIMARY KEY (room_id, stay_date)
            )
            """,
            """
            CREATE TABLE payments (
              payment_id TEXT PRIMARY KEY,
              reservation_id TEXT NOT NULL,
              amount_cents INTEGER NOT NULL,
              status TEXT NOT NULL,
              created_at TEXT NOT NULL
            )
            """,
        ],
    )


def seed_hotel_data(
    db: Any,
    *,
    hotel_count: int,
    rooms_per_hotel: int,
    date_count: int,
) -> None:
    room_types = [
        ("standard", "Standard King", 1, 2),
        ("suite", "Junior Suite", 2, 4),
    ]
    for room_type in room_types:
        db.execute(
            """
            INSERT INTO room_types (room_type_id, name, beds, max_guests)
            VALUES (:room_type_id, :name, :beds, :max_guests)
            """,
            {
                "room_type_id": room_type[0],
                "name": room_type[1],
                "beds": room_type[2],
                "max_guests": room_type[3],
            },
        )
    for guest_index in range(1, 101):
        db.execute(
            """
            INSERT INTO guests (guest_id, full_name, loyalty_tier)
            VALUES (:guest_id, :full_name, :loyalty_tier)
            """,
            {
                "guest_id": f"guest_{guest_index:03d}",
                "full_name": f"Guest {guest_index:03d}",
                "loyalty_tier": "gold" if guest_index % 5 == 0 else "standard",
            },
        )
    start_date = date(2026, 7, 1)
    for hotel_index in range(1, hotel_count + 1):
        hotel_id = f"hotel_{hotel_index:03d}"
        db.execute(
            """
            INSERT INTO hotels (hotel_id, city, name, star_rating, brand)
            VALUES (:hotel_id, :city, :name, :star_rating, :brand)
            """,
            {
                "hotel_id": hotel_id,
                "city": "San Francisco" if hotel_index % 2 else "New York",
                "name": f"Chronos Hotel {hotel_index:03d}",
                "star_rating": 4 + (hotel_index % 2),
                "brand": "Chronos Suites",
            },
        )
        for room_index in range(1, rooms_per_hotel + 1):
            room_type_id = "suite" if room_index % 5 == 0 else "standard"
            db.execute(
                """
                INSERT INTO rooms (
                  room_id, hotel_id, room_type_id, floor,
                  view_name, bed_type, popularity_score, status
                ) VALUES (
                  :room_id, :hotel_id, :room_type_id, :floor,
                  :view_name, :bed_type, :popularity_score, 'open'
                )
                """,
                {
                    "room_id": f"{hotel_id}_room_{room_index:04d}",
                    "hotel_id": hotel_id,
                    "room_type_id": room_type_id,
                    "floor": 1 + room_index // 20,
                    "view_name": "bay" if room_index % 7 == 0 else "city" if room_index % 3 == 0 else "courtyard",
                    "bed_type": "king" if room_type_id == "standard" else "king+sofa",
                    "popularity_score": 1000 - room_index,
                },
            )
        for room_type_id in ("standard", "suite"):
            capacity = max(1, rooms_per_hotel // (5 if room_type_id == "suite" else 2))
            for offset in range(date_count):
                stay_date = (start_date + timedelta(days=offset)).isoformat()
                base_rate = 18_000 + hotel_index * 250 + offset * 25
                if room_type_id == "suite":
                    base_rate += 12_000
                db.execute(
                    """
                    INSERT INTO nightly_rates (
                      hotel_id, room_type_id, stay_date, rate_cents
                    ) VALUES (
                      :hotel_id, :room_type_id, :stay_date, :rate_cents
                    )
                    """,
                    {
                        "hotel_id": hotel_id,
                        "room_type_id": room_type_id,
                        "stay_date": stay_date,
                        "rate_cents": base_rate,
                    },
                )
                db.execute(
                    """
                    INSERT INTO room_inventory (
                      hotel_id, room_type_id, stay_date, total_capacity,
                      available_count, held_count, reserved_count, base_rate_cents,
                      promo_rate_cents, last_quote_cents, min_stay_nights,
                      cleaning_hold_count, maintenance_blocked_count, last_priced_at,
                      policy_note
                    ) VALUES (
                      :hotel_id, :room_type_id, :stay_date, :total_capacity,
                      :available_count, 0, 0, :base_rate_cents,
                      NULL, NULL, 1, 0, 0, NULL, 'standard policy'
                    )
                    """,
                    {
                        "hotel_id": hotel_id,
                        "room_type_id": room_type_id,
                        "stay_date": stay_date,
                        "total_capacity": capacity,
                        "available_count": capacity,
                        "base_rate_cents": base_rate,
                    },
                )
    seed_cancelled_reservation(db)


def seed_cancelled_reservation(db: Any) -> None:
    reservation_id = "seed_cancel_001"
    checkin = date(2026, 7, 1)
    checkout = checkin + timedelta(days=2)
    room_id = "hotel_001_room_0001"
    db.execute(
        """
        INSERT INTO reservations (
          reservation_id, guest_id, hotel_id, room_type_id, checkin_date,
          checkout_date, status, total_cents, request_id, created_at
        ) VALUES (
          :reservation_id, 'guest_001', 'hotel_001', 'standard', :checkin_date,
          :checkout_date, 'confirmed', 36000, 'seed', 'seed'
        )
        """,
        {
            "reservation_id": reservation_id,
            "checkin_date": checkin.isoformat(),
            "checkout_date": checkout.isoformat(),
        },
    )
    for offset in range(2):
        stay_date = (checkin + timedelta(days=offset)).isoformat()
        db.execute(
            """
            INSERT INTO reservation_nights (
              reservation_id, stay_date, room_id, hotel_id, room_type_id, rate_cents
            ) VALUES (
              :reservation_id, :stay_date, :room_id, 'hotel_001', 'standard', 18000
            )
            """,
            {"reservation_id": reservation_id, "stay_date": stay_date, "room_id": room_id},
        )
        db.execute(
            """
            INSERT INTO room_occupancy (
              room_id, stay_date, reservation_id, hotel_id, room_type_id, occupancy_type
            ) VALUES (
              :room_id, :stay_date, :reservation_id, 'hotel_001', 'standard', 'reserved'
            )
            """,
            {"reservation_id": reservation_id, "stay_date": stay_date, "room_id": room_id},
        )
        db.execute(
            """
            UPDATE room_inventory
            SET available_count = available_count - 1,
                reserved_count = reserved_count + 1
            WHERE hotel_id = 'hotel_001'
              AND room_type_id = 'standard'
              AND stay_date = :stay_date
            """,
            {"stay_date": stay_date},
        )
    db.execute(
        """
        INSERT INTO payments (payment_id, reservation_id, amount_cents, status, created_at)
        VALUES ('seed_payment_001', :reservation_id, 36000, 'captured', 'seed')
        """,
        {"reservation_id": reservation_id},
    )


def connect_chronos(database_url: str) -> ChronosBranchContext:
    backend = os.environ.get("CHRONOS_HOTEL_BRANCH_BACKEND", "interval")
    kwargs: dict[str, Any] = {"backend": backend}
    if backend == "interval":
        kwargs["interval_child_width"] = int(os.environ.get("CHRONOS_HOTEL_INTERVAL_CHILD_WIDTH", "1000"))
    return ChronosBranchContext.connect(
        database_url,
        **kwargs,
    )


def register_hotel_tables(ctx: ChronosBranchContext) -> None:
    for table, pk in HOTEL_TABLES.items():
        ctx.register_table(table, pk)


def setup_postgres_hotel_database(
    database_url: str,
    *,
    hotel_count: int,
    rooms_per_hotel: int,
    date_count: int,
) -> None:
    db = connect_sql_database(database_url)
    try:
        reset_postgres_schema(db)
        create_hotel_schema(db)
        seed_hotel_data(db, hotel_count=hotel_count, rooms_per_hotel=rooms_per_hotel, date_count=date_count)
        db.commit()
    finally:
        db.close()


def setup_chronos_hotel_database(
    database_url: str,
    *,
    hotel_count: int,
    rooms_per_hotel: int,
    date_count: int,
) -> None:
    db = connect_sql_database(database_url)
    try:
        reset_postgres_schema(db)
        create_hotel_schema(db)
        db.commit()
    finally:
        db.close()

    ctx = connect_chronos(database_url)
    try:
        register_hotel_tables(ctx)
        main = ctx.checkout("main")
        with main.transaction():
            seed_hotel_data(main, hotel_count=hotel_count, rooms_per_hotel=rooms_per_hotel, date_count=date_count)
    finally:
        ctx.close()


def setup_hotel_database(
    mode: str,
    database_url: str,
    *,
    hotel_count: int,
    rooms_per_hotel: int,
    date_count: int,
) -> None:
    setup = setup_chronos_hotel_database if mode == "branch" else setup_postgres_hotel_database
    setup(database_url, hotel_count=hotel_count, rooms_per_hotel=rooms_per_hotel, date_count=date_count)


def stay_dates(request: HotelRequest) -> list[str]:
    return stay_date_range(request.checkin_date, request.nights)


def stay_date_range(checkin_date: str, nights: int) -> list[str]:
    first = date.fromisoformat(checkin_date)
    return [(first + timedelta(days=offset)).isoformat() for offset in range(nights)]


def checkout_date(request: HotelRequest) -> str:
    return (date.fromisoformat(request.checkin_date) + timedelta(days=request.nights)).isoformat()


def money(cents: int | None) -> str:
    return "unknown" if cents is None else f"${cents / 100:.2f}"


def hotel_display_name(hotel_id: str) -> str:
    suffix = hotel_id.rsplit("_", 1)[-1]
    return f"Chronos Hotel {suffix}"


def room_display_name(room_type_id: str) -> str:
    return {
        "standard": "Standard King",
        "suite": "Junior Suite",
    }.get(room_type_id, room_type_id.replace("_", " ").title())


def human_date(value: str) -> str:
    return date.fromisoformat(value).strftime("%B %d, %Y").replace(" 0", " ")


def natural_language_request(request: HotelRequest) -> str:
    hotel = f"{hotel_display_name(request.hotel_id)} ({request.hotel_id})"
    room = f"{room_display_name(request.room_type_id)} ({request.room_type_id})"
    checkin = human_date(request.checkin_date)
    guest = request.guest_id.replace("_", " ").title()
    if request.transaction_type == "book":
        return (
            f"I'm {guest}. Please inspect the available physical rooms and book the best recommended {room} "
            f"at {hotel} starting {checkin} "
            f"for {request.nights} nights. My budget is around {money(request.amount_cents)} total, "
            "but a close price is okay if the room is available. Please go ahead and complete the booking."
        )
    if request.transaction_type == "hold":
        return (
            f"I'm {guest}. Please inspect the available physical rooms and place a temporary hold on the best {room} "
            f"at {hotel} starting {checkin} "
            f"for {request.nights} nights while I confirm plans."
        )
    if request.transaction_type == "cancel_booking":
        return f"Please cancel reservation {request.reservation_to_cancel} for me."
    if request.transaction_type == "price_refresh":
        return (
            f"Please refresh the promo quote for the {room} at {hotel} starting {checkin} "
            f"for {request.nights} nights to {money(request.new_rate_cents)} per night."
        )
    if request.transaction_type == "policy_update":
        return (
            f"Please update the minimum stay policy for the {room} at {hotel} starting {checkin} "
            f"for this stay window to {request.new_min_stay} nights."
        )
    if request.transaction_type == "maintenance_block":
        return (
            f"Please inspect available physical rooms and block the best candidate {room} at {hotel} "
            f"for maintenance starting {checkin} "
            f"for {request.nights} nights."
        )
    if request.transaction_type == "housekeeping_hold":
        return (
            f"Please inspect available physical rooms and hold the best candidate {room} at {hotel} "
            f"for housekeeping starting {checkin} "
            f"for {request.nights} nights."
        )
    raise RuntimeError(f"unknown transaction type: {request.transaction_type}")


def generate_requests(count: int, conflict_mix: list[str]) -> list[HotelRequest]:
    requests: list[HotelRequest] = []
    mix = conflict_mix or ["disjoint", "arithmetic", "capacity", "policy"]
    seen: dict[str, int] = {}
    for index in range(count):
        conflict_class = mix[index % len(mix)]
        occurrence = seen.get(conflict_class, 0)
        seen[conflict_class] = occurrence + 1
        guest_id = f"guest_{(index % 100) + 1:03d}"
        base = {
            "request_id": f"req_{index:04d}",
            "hotel_id": "hotel_001",
            "room_type_id": "standard",
            "guest_id": guest_id,
            "checkin_date": "2026-07-01",
            "nights": 2,
            "amount_cents": 36_000,
        }
        if conflict_class == "disjoint":
            txn_type = "book" if occurrence % 2 == 0 else "price_refresh"
        elif conflict_class == "disjoint_policy":
            txn_type = "hold" if occurrence % 2 == 0 else "policy_update"
        elif conflict_class == "arithmetic":
            txn_type = "book" if occurrence % 2 == 0 else "cancel_booking"
        elif conflict_class == "capacity":
            txn_type = "book" if occurrence % 2 == 0 else "maintenance_block"
        elif conflict_class == "policy":
            txn_type = "book" if occurrence % 2 == 0 else "policy_update"
        else:
            txn_type = conflict_class
        requests.append(
            HotelRequest(
                **base,
                transaction_type=txn_type,
                new_rate_cents=15_000 + index,
                new_min_stay=3,
                reservation_to_cancel="seed_cancel_001",
            )
        )
    return requests


class HotelBookingStore:
    def __init__(
        self,
        session: Any,
        *,
        metrics: AttemptMetrics,
        base_row_recorder: Callable[[tuple[str, str, str], dict[str, Any]], None] | None = None,
    ) -> None:
        self.session = session
        self.metrics = metrics
        self.base_row_recorder = base_row_recorder

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        rows = self.session.query(sql, params or {}) if hasattr(self.session, "query") else [
            dict(row) for row in self.session.execute(sql, params or {}).fetchall()
        ]
        return [dict(row) for row in rows]

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        if hasattr(self.session, "execute"):
            return self.session.execute(sql, params or {})
        raise TypeError("session does not support execute")

    def _tool(self, name: str, fn: Callable[[], Any]) -> Any:
        self.metrics.tool_calls += 1
        timer = Timer()
        self.metrics.trace("tool_start", tool=name)
        try:
            value = fn()
            self.metrics.trace(
                "tool_success",
                tool=name,
                elapsed_ms=timer.elapsed_ms(),
                result_type=type(value).__name__,
            )
            return value
        except Exception as exc:
            self.metrics.trace(
                "tool_error",
                tool=name,
                elapsed_ms=timer.elapsed_ms(),
                error=abort_message(exc),
            )
            raise
        finally:
            self.metrics.tool_latency_ms += timer.elapsed_ms()

    def _mutating_tool(self, name: str, fn: Callable[[], Any]) -> Any:
        return self._tool(name, fn)

    def _require_updated(self, result: Any, message: str) -> None:
        rowcount = getattr(result, "rowcount", None)
        if rowcount == 0:
            raise RuntimeError(message)

    def _record_base_row(self, hotel_id: str, room_type_id: str, stay_date: str) -> None:
        if self.base_row_recorder is None:
            return
        key = (hotel_id, room_type_id, stay_date)
        row = self.get_inventory_row(hotel_id, room_type_id, stay_date)
        self.base_row_recorder(key, row)

    def search_hotels(self, city: str, min_star_rating: int = 0) -> list[dict[str, Any]]:
        return self._tool(
            "search_hotels",
            lambda: self.query(
                """
                SELECT hotel_id, city, name, star_rating, brand
                FROM hotels
                WHERE lower(city) = lower(:city)
                  AND star_rating >= :min_star_rating
                ORDER BY star_rating DESC, hotel_id
                """,
                {"city": city, "min_star_rating": min_star_rating},
            )
        )

    def search_room_rates(
        self,
        hotel_id: str,
        room_type_id: str,
        checkin_date: str,
        nights: int,
    ) -> list[dict[str, Any]]:
        first = date.fromisoformat(checkin_date)
        last = (first + timedelta(days=nights - 1)).isoformat()
        return self._tool(
            "search_room_rates",
            lambda: self.query(
                """
                SELECT hotel_id, room_type_id, stay_date, rate_cents
                FROM nightly_rates
                WHERE hotel_id = :hotel_id
                  AND room_type_id = :room_type_id
                  AND stay_date BETWEEN :first_date AND :last_date
                ORDER BY stay_date
                """,
                {
                    "hotel_id": hotel_id,
                    "room_type_id": room_type_id,
                    "first_date": first.isoformat(),
                    "last_date": last,
                },
            )
        )

    def get_inventory_row(self, hotel_id: str, room_type_id: str, stay_date: str) -> dict[str, Any]:
        rows = self.query(
            """
            SELECT *
            FROM room_inventory
            WHERE hotel_id = :hotel_id
              AND room_type_id = :room_type_id
              AND stay_date = :stay_date
            """,
            {
                "hotel_id": hotel_id,
                "room_type_id": room_type_id,
                "stay_date": stay_date,
            },
        )
        if not rows:
            raise RuntimeError(f"missing inventory row: {hotel_id} {room_type_id} {stay_date}")
        return rows[0]

    def check_availability(
        self,
        hotel_id: str,
        room_type_id: str,
        checkin_date: str,
        nights: int,
    ) -> list[dict[str, Any]]:
        def run() -> list[dict[str, Any]]:
            rows = []
            for stay_date in stay_date_range(checkin_date, nights):
                self._record_base_row(hotel_id, room_type_id, stay_date)
                rows.append(self.room_availability_row(hotel_id, room_type_id, stay_date))
            return rows

        return self._tool("check_availability", run)

    def room_availability_row(self, hotel_id: str, room_type_id: str, stay_date: str) -> dict[str, Any]:
        rows = self.query(
            """
            SELECT
              :hotel_id AS hotel_id,
              :room_type_id AS room_type_id,
              :stay_date AS stay_date,
              COUNT(r.room_id) AS physical_room_count,
              COUNT(o.room_id) AS occupied_room_count,
              COUNT(r.room_id) - COUNT(o.room_id) AS available_room_count
            FROM rooms r
            LEFT JOIN room_occupancy o
              ON o.room_id = r.room_id
             AND o.stay_date = :stay_date
            WHERE r.hotel_id = :hotel_id
              AND r.room_type_id = :room_type_id
              AND r.status = 'open'
            """,
            {
                "hotel_id": hotel_id,
                "room_type_id": room_type_id,
                "stay_date": stay_date,
            },
        )
        if not rows:
            raise RuntimeError(f"missing room availability: {hotel_id} {room_type_id} {stay_date}")
        return rows[0]

    def available_rooms(self, hotel_id: str, room_type_id: str, checkin_date: str, nights: int) -> list[dict[str, Any]]:
        dates = stay_date_range(checkin_date, nights)
        params = {
            "hotel_id": hotel_id,
            "room_type_id": room_type_id,
            "first_date": dates[0],
            "last_date": dates[-1],
            "nights": nights,
        }
        return self.query(
            """
            SELECT
              r.room_id,
              r.hotel_id,
              r.room_type_id,
              r.floor,
              r.view_name,
              r.bed_type,
              r.popularity_score,
              r.status
            FROM rooms r
            LEFT JOIN room_occupancy o
              ON o.room_id = r.room_id
             AND o.stay_date BETWEEN :first_date AND :last_date
            WHERE r.hotel_id = :hotel_id
              AND r.room_type_id = :room_type_id
              AND r.status = 'open'
            GROUP BY
              r.room_id, r.hotel_id, r.room_type_id, r.floor,
              r.view_name, r.bed_type, r.popularity_score, r.status
            HAVING COUNT(o.room_id) = 0
            ORDER BY r.popularity_score DESC, r.floor DESC, r.room_id
            """,
            params,
        )

    def _require_room_available(self, room_id: str, request: HotelRequest) -> dict[str, Any]:
        rows = self.query(
            """
            SELECT room_id, hotel_id, room_type_id, floor, view_name, bed_type, popularity_score, status
            FROM rooms
            WHERE room_id = :room_id
              AND hotel_id = :hotel_id
              AND room_type_id = :room_type_id
              AND status = 'open'
            """,
            {
                "room_id": room_id,
                "hotel_id": request.hotel_id,
                "room_type_id": request.room_type_id,
            },
        )
        if not rows:
            raise RuntimeError(f"room unavailable or wrong type: {room_id}")
        occupied = self.query(
            """
            SELECT stay_date
            FROM room_occupancy
            WHERE room_id = :room_id
              AND stay_date BETWEEN :first_date AND :last_date
            ORDER BY stay_date
            """,
            {
                "room_id": room_id,
                "first_date": request.checkin_date,
                "last_date": stay_dates(request)[-1],
            },
        )
        if occupied:
            raise AgentTransactionAborted(f"room_occupied:{room_id}")
        return rows[0]

    def create_reservation(self, request: HotelRequest, *, status: str, room_id: str) -> str:
        def run() -> str:
            self._require_room_available(room_id, request)
            reservation_id = f"res_{request.request_id}_{uuid.uuid4().hex[:8]}"
            self.execute(
                """
                INSERT INTO reservations (
                  reservation_id, guest_id, hotel_id, room_type_id, checkin_date,
                  checkout_date, status, total_cents, request_id, created_at
                ) VALUES (
                  :reservation_id, :guest_id, :hotel_id, :room_type_id, :checkin_date,
                  :checkout_date, :status, :total_cents, :request_id, :created_at
                )
                """,
                {
                    "reservation_id": reservation_id,
                    "guest_id": request.guest_id,
                    "hotel_id": request.hotel_id,
                    "room_type_id": request.room_type_id,
                    "checkin_date": request.checkin_date,
                    "checkout_date": checkout_date(request),
                    "status": status,
                    "total_cents": request.amount_cents,
                    "request_id": request.request_id,
                    "created_at": request.request_id,
                },
            )
            for stay_date in stay_dates(request):
                row = self.get_inventory_row(request.hotel_id, request.room_type_id, stay_date)
                rate = row["promo_rate_cents"] or row["base_rate_cents"]
                self.execute(
                    """
                    INSERT INTO reservation_nights (
                      reservation_id, stay_date, room_id, hotel_id, room_type_id, rate_cents
                    ) VALUES (
                      :reservation_id, :stay_date, :room_id, :hotel_id, :room_type_id, :rate_cents
                    )
                    """,
                    {
                        "reservation_id": reservation_id,
                        "stay_date": stay_date,
                        "room_id": room_id,
                        "hotel_id": request.hotel_id,
                        "room_type_id": request.room_type_id,
                        "rate_cents": rate,
                    },
                )
            return reservation_id

        return self._mutating_tool("create_reservation", run)

    def reserve_inventory(self, request: HotelRequest, reservation_id: str, room_id: str) -> None:
        return self._occupy_room(request, reservation_id, room_id, "reserve_inventory", "reserved")

    def hold_inventory(self, request: HotelRequest, reservation_id: str, room_id: str) -> None:
        return self._occupy_room(request, reservation_id, room_id, "hold_inventory", "held")

    def _occupy_room(
        self,
        request: HotelRequest,
        reservation_id: str,
        room_id: str,
        tool_name: str,
        occupancy_type: str,
    ) -> None:
        def run() -> None:
            self._require_room_available(room_id, request)
            for stay_date in stay_dates(request):
                self._record_base_row(request.hotel_id, request.room_type_id, stay_date)
                self.execute(
                    """
                    INSERT INTO room_occupancy (
                      room_id, stay_date, reservation_id, hotel_id, room_type_id, occupancy_type
                    ) VALUES (
                      :room_id, :stay_date, :reservation_id, :hotel_id, :room_type_id, :occupancy_type
                    )
                    """,
                    {
                        "room_id": room_id,
                        "stay_date": stay_date,
                        "reservation_id": reservation_id,
                        "hotel_id": request.hotel_id,
                        "room_type_id": request.room_type_id,
                        "occupancy_type": occupancy_type,
                    },
                )

        return self._mutating_tool(tool_name, run)

    def _consume_inventory(self, request: HotelRequest, tool_name: str, counter_column: str) -> None:
        if counter_column not in {"reserved_count", "held_count", "maintenance_blocked_count", "cleaning_hold_count"}:
            raise ValueError(f"unsupported inventory counter: {counter_column}")

        def run() -> None:
            for stay_date in stay_dates(request):
                self._record_base_row(request.hotel_id, request.room_type_id, stay_date)
                result = self.execute(
                    f"""
                    UPDATE room_inventory
                    SET available_count = available_count - 1,
                        {counter_column} = {counter_column} + 1
                    WHERE hotel_id = :hotel_id
                      AND room_type_id = :room_type_id
                      AND stay_date = :stay_date
                      AND available_count >= 1
                    """,
                    {
                        "hotel_id": request.hotel_id,
                        "room_type_id": request.room_type_id,
                        "stay_date": stay_date,
                    },
                )
                self._require_updated(result, "inventory unavailable")
                row = self.get_inventory_row(request.hotel_id, request.room_type_id, stay_date)
                if row["available_count"] < 0:
                    raise RuntimeError("inventory overbooked")

        return self._mutating_tool(tool_name, run)

    def cancel_booking(self, request: HotelRequest) -> None:
        def run() -> None:
            if not request.reservation_to_cancel:
                raise RuntimeError("cancel request missing reservation id")
            rows = self.query(
                """
                SELECT stay_date, room_id, hotel_id, room_type_id
                FROM reservation_nights
                WHERE reservation_id = :reservation_id
                ORDER BY stay_date
                """,
                {"reservation_id": request.reservation_to_cancel},
            )
            if not rows:
                raise RuntimeError("reservation not found")
            for row in rows:
                self._record_base_row(row["hotel_id"], row["room_type_id"], row["stay_date"])
                if row["room_id"]:
                    self.execute(
                        """
                        DELETE FROM room_occupancy
                        WHERE room_id = :room_id
                          AND stay_date = :stay_date
                          AND reservation_id = :reservation_id
                        """,
                        {
                            "room_id": row["room_id"],
                            "stay_date": row["stay_date"],
                            "reservation_id": request.reservation_to_cancel,
                        },
                    )
            self.execute(
                """
                DELETE FROM room_occupancy
                WHERE reservation_id = :reservation_id
                """,
                {"reservation_id": request.reservation_to_cancel},
            )
            self.execute(
                """
                UPDATE reservations
                SET status = 'cancelled'
                WHERE reservation_id = :reservation_id
                """,
                {"reservation_id": request.reservation_to_cancel},
            )

        return self._mutating_tool("cancel_booking", run)

    def update_price_quote(self, request: HotelRequest) -> None:
        def run() -> None:
            for stay_date in stay_dates(request):
                self._record_base_row(request.hotel_id, request.room_type_id, stay_date)
                self.execute(
                    """
                    UPDATE room_inventory
                    SET promo_rate_cents = :promo_rate_cents,
                        last_quote_cents = :promo_rate_cents,
                        last_priced_at = :last_priced_at
                    WHERE hotel_id = :hotel_id
                      AND room_type_id = :room_type_id
                      AND stay_date = :stay_date
                    """,
                    {
                        "hotel_id": request.hotel_id,
                        "room_type_id": request.room_type_id,
                        "stay_date": stay_date,
                        "promo_rate_cents": request.new_rate_cents or request.amount_cents,
                        "last_priced_at": request.request_id,
                    },
                )

        return self._mutating_tool("update_price_quote", run)

    def update_policy(self, request: HotelRequest) -> None:
        def run() -> None:
            for stay_date in stay_dates(request):
                self._record_base_row(request.hotel_id, request.room_type_id, stay_date)
                self.execute(
                    """
                    UPDATE room_inventory
                    SET min_stay_nights = :min_stay_nights,
                        policy_note = :policy_note
                    WHERE hotel_id = :hotel_id
                      AND room_type_id = :room_type_id
                      AND stay_date = :stay_date
                    """,
                    {
                        "hotel_id": request.hotel_id,
                        "room_type_id": request.room_type_id,
                        "stay_date": stay_date,
                        "min_stay_nights": request.new_min_stay or 2,
                        "policy_note": f"policy:{request.request_id}",
                    },
                )

        return self._mutating_tool("update_policy", run)

    def maintenance_block(self, request: HotelRequest, room_id: str) -> None:
        reservation_id = f"maintenance_{request.request_id}"
        return self._occupy_room(request, reservation_id, room_id, "maintenance_block", "maintenance")

    def housekeeping_hold(self, request: HotelRequest, room_id: str) -> None:
        reservation_id = f"housekeeping_{request.request_id}"
        return self._occupy_room(request, reservation_id, room_id, "housekeeping_hold", "housekeeping")

    def record_payment(self, reservation_id: str, amount_cents: int) -> str:
        def run() -> str:
            payment_id = f"pay_{reservation_id}_{uuid.uuid4().hex[:8]}"
            self.execute(
                """
                INSERT INTO payments (
                  payment_id, reservation_id, amount_cents, status, created_at
                ) VALUES (
                  :payment_id, :reservation_id, :amount_cents, 'captured', :created_at
                )
                """,
                {
                    "payment_id": payment_id,
                    "reservation_id": reservation_id,
                    "amount_cents": amount_cents,
                    "created_at": reservation_id,
                },
            )
            return payment_id

        return self._mutating_tool("record_payment", run)

    def finalize_booking(self, reservation_id: str) -> None:
        def run() -> None:
            self.execute(
                """
                UPDATE reservations
                SET status = 'confirmed'
                WHERE reservation_id = :reservation_id
                """,
                {"reservation_id": reservation_id},
            )

        return self._mutating_tool("finalize_booking", run)


class SagaHotelBookingStore(HotelBookingStore):
    def __init__(
        self,
        session: SQLDatabaseAdapter,
        *,
        metrics: AttemptMetrics,
        base_row_recorder: Callable[[tuple[str, str, str], dict[str, Any]], None] | None = None,
    ) -> None:
        super().__init__(session, metrics=metrics, base_row_recorder=base_row_recorder)
        self.db = session
        self.saga_trace: list[str] = []
        self.compensations: list[tuple[str, Callable[[], None]]] = []

    def _mutating_tool(self, name: str, fn: Callable[[], Any]) -> Any:
        self.metrics.tool_calls += 1
        tool_timer = Timer()
        txn_timer = Timer()
        self.metrics.trace("tool_start", tool=name, saga_transaction=True)
        if self.db.in_transaction:
            self.db.commit()
        self.db.begin()
        try:
            value = fn()
            self.db.commit()
            elapsed = tool_timer.elapsed_ms()
            self.metrics.trace(
                "tool_success",
                tool=name,
                elapsed_ms=elapsed,
                result_type=type(value).__name__,
                saga_transaction=True,
            )
            return value
        except Exception as exc:
            self.db.rollback()
            self.metrics.trace(
                "tool_error",
                tool=name,
                elapsed_ms=tool_timer.elapsed_ms(),
                error=abort_message(exc),
                saga_transaction=True,
            )
            raise
        finally:
            self.metrics.tool_latency_ms += tool_timer.elapsed_ms()
            self.metrics.transaction_latency_ms += txn_timer.elapsed_ms()

    def _append_compensation(self, name: str, undo: Callable[[], None]) -> None:
        self.saga_trace.append(name)
        self.compensations.append((name, undo))

    def compensate(self) -> None:
        while self.compensations:
            name, undo = self.compensations.pop()
            self.metrics.compensation_attempts += 1
            timer = Timer()
            try:
                if self.db.in_transaction:
                    self.db.rollback()
                self.db.begin()
                self.metrics.trace("compensation_start", step=name)
                undo()
                self.db.commit()
                self.metrics.trace(
                    "compensation_success",
                    step=name,
                    elapsed_ms=timer.elapsed_ms(),
                )
            except Exception:
                self.metrics.compensation_failures += 1
                self.db.rollback()
                self.metrics.trace(
                    "compensation_error",
                    step=name,
                    elapsed_ms=timer.elapsed_ms(),
                )
            finally:
                self.metrics.transaction_latency_ms += timer.elapsed_ms()

    def _inventory_rows_for_request(self, request: HotelRequest) -> list[dict[str, Any]]:
        return [
            self.get_inventory_row(request.hotel_id, request.room_type_id, item_date)
            for item_date in stay_dates(request)
        ]

    def _with_inventory_compensation(self, name: str, request: HotelRequest, fn: Callable[[], None]) -> None:
        before = self._inventory_rows_for_request(request)
        fn()
        self._append_compensation(name, lambda before=before: restore_inventory_rows(self.db, before))

    def _reservation_row(self, reservation_id: str) -> dict[str, Any]:
        rows = self.query(
            """
            SELECT *
            FROM reservations
            WHERE reservation_id = :reservation_id
            """,
            {"reservation_id": reservation_id},
        )
        if not rows:
            raise RuntimeError(f"missing reservation: {reservation_id}")
        return rows[0]

    def _restore_reservation_row(self, row: dict[str, Any]) -> None:
        self.db.execute(
            """
            UPDATE reservations
            SET guest_id = :guest_id,
                hotel_id = :hotel_id,
                room_type_id = :room_type_id,
                checkin_date = :checkin_date,
                checkout_date = :checkout_date,
                status = :status,
                total_cents = :total_cents,
                request_id = :request_id,
                created_at = :created_at
            WHERE reservation_id = :reservation_id
            """,
            row,
        )

    def _delete_reservation_artifacts(self, reservation_id: str) -> None:
        self.db.execute(
            "DELETE FROM room_occupancy WHERE reservation_id = :reservation_id",
            {"reservation_id": reservation_id},
        )
        self.db.execute(
            "DELETE FROM payments WHERE reservation_id = :reservation_id",
            {"reservation_id": reservation_id},
        )
        self.db.execute(
            "DELETE FROM reservation_nights WHERE reservation_id = :reservation_id",
            {"reservation_id": reservation_id},
        )
        self.db.execute(
            "DELETE FROM reservations WHERE reservation_id = :reservation_id",
            {"reservation_id": reservation_id},
        )

    def create_reservation(self, request: HotelRequest, *, status: str, room_id: str) -> str:
        reservation_id = super().create_reservation(request, status=status, room_id=room_id)
        self._append_compensation(
            "create_reservation",
            lambda reservation_id=reservation_id: self._delete_reservation_artifacts(reservation_id),
        )
        return reservation_id

    def reserve_inventory(self, request: HotelRequest, reservation_id: str, room_id: str) -> None:
        HotelBookingStore.reserve_inventory(self, request, reservation_id, room_id)
        self._append_compensation(
            "reserve_inventory",
            lambda reservation_id=reservation_id: self.db.execute(
                "DELETE FROM room_occupancy WHERE reservation_id = :reservation_id",
                {"reservation_id": reservation_id},
            ),
        )

    def hold_inventory(self, request: HotelRequest, reservation_id: str, room_id: str) -> None:
        HotelBookingStore.hold_inventory(self, request, reservation_id, room_id)
        self._append_compensation(
            "hold_inventory",
            lambda reservation_id=reservation_id: self.db.execute(
                "DELETE FROM room_occupancy WHERE reservation_id = :reservation_id",
                {"reservation_id": reservation_id},
            ),
        )

    def cancel_booking(self, request: HotelRequest) -> None:
        if not request.reservation_to_cancel:
            raise RuntimeError("cancel request missing reservation id")
        reservation_before = self._reservation_row(request.reservation_to_cancel)
        nights = self.query(
            """
            SELECT stay_date, room_id, hotel_id, room_type_id
            FROM reservation_nights
            WHERE reservation_id = :reservation_id
            ORDER BY stay_date
            """,
            {"reservation_id": request.reservation_to_cancel},
        )
        inventory_before = [
            self.get_inventory_row(row["hotel_id"], row["room_type_id"], row["stay_date"])
            for row in nights
        ]
        occupancy_before = self.query(
            """
            SELECT *
            FROM room_occupancy
            WHERE reservation_id = :reservation_id
            ORDER BY room_id, stay_date
            """,
            {"reservation_id": request.reservation_to_cancel},
        )
        super().cancel_booking(request)
        self._append_compensation(
            "cancel_booking",
            lambda reservation_before=reservation_before, inventory_before=inventory_before, occupancy_before=occupancy_before: (
                restore_inventory_rows(self.db, inventory_before),
                restore_occupancy_rows(self.db, occupancy_before),
                self._restore_reservation_row(reservation_before),
            ),
        )

    def update_price_quote(self, request: HotelRequest) -> None:
        self._with_inventory_compensation(
            "update_price_quote",
            request,
            lambda: HotelBookingStore.update_price_quote(self, request),
        )

    def update_policy(self, request: HotelRequest) -> None:
        self._with_inventory_compensation(
            "update_policy",
            request,
            lambda: HotelBookingStore.update_policy(self, request),
        )

    def maintenance_block(self, request: HotelRequest, room_id: str) -> None:
        HotelBookingStore.maintenance_block(self, request, room_id)
        self._append_compensation(
            "maintenance_block",
            lambda reservation_id=f"maintenance_{request.request_id}": self.db.execute(
                "DELETE FROM room_occupancy WHERE reservation_id = :reservation_id",
                {"reservation_id": reservation_id},
            ),
        )

    def housekeeping_hold(self, request: HotelRequest, room_id: str) -> None:
        HotelBookingStore.housekeeping_hold(self, request, room_id)
        self._append_compensation(
            "housekeeping_hold",
            lambda reservation_id=f"housekeeping_{request.request_id}": self.db.execute(
                "DELETE FROM room_occupancy WHERE reservation_id = :reservation_id",
                {"reservation_id": reservation_id},
            ),
        )

    def record_payment(self, reservation_id: str, amount_cents: int) -> str:
        payment_id = super().record_payment(reservation_id, amount_cents)
        self._append_compensation(
            "record_payment",
            lambda payment_id=payment_id: self.db.execute(
                "DELETE FROM payments WHERE payment_id = :payment_id",
                {"payment_id": payment_id},
            ),
        )
        return payment_id

    def finalize_booking(self, reservation_id: str) -> None:
        before = self._reservation_row(reservation_id)
        super().finalize_booking(reservation_id)
        self._append_compensation(
            "finalize_booking",
            lambda before=before: self._restore_reservation_row(before),
        )


def scripted_agent_run(store: HotelBookingStore, request: HotelRequest) -> None:
    store.search_hotels("San Francisco", 4)
    store.search_room_rates(
        request.hotel_id,
        request.room_type_id,
        request.checkin_date,
        request.nights,
    )
    store.check_availability(
        request.hotel_id,
        request.room_type_id,
        request.checkin_date,
        request.nights,
    )
    rooms = store.available_rooms(
        request.hotel_id,
        request.room_type_id,
        request.checkin_date,
        request.nights,
    )
    room_id = rooms[0]["room_id"] if rooms else ""
    if request.transaction_type == "book":
        if not room_id:
            raise RuntimeError("no physical room available")
        reservation_id = store.create_reservation(request, status="pending", room_id=room_id)
        store.reserve_inventory(request, reservation_id, room_id)
        store.record_payment(reservation_id, request.amount_cents)
        store.finalize_booking(reservation_id)
    elif request.transaction_type == "hold":
        if not room_id:
            raise RuntimeError("no physical room available")
        reservation_id = store.create_reservation(request, status="held", room_id=room_id)
        store.hold_inventory(request, reservation_id, room_id)
    elif request.transaction_type == "cancel_booking":
        store.cancel_booking(request)
    elif request.transaction_type == "price_refresh":
        store.update_price_quote(request)
    elif request.transaction_type == "policy_update":
        store.update_policy(request)
    elif request.transaction_type == "maintenance_block":
        if not room_id:
            raise RuntimeError("no physical room available")
        store.maintenance_block(request, room_id)
    elif request.transaction_type == "housekeeping_hold":
        if not room_id:
            raise RuntimeError("no physical room available")
        store.housekeeping_hold(request, room_id)
    else:
        raise RuntimeError(f"unknown transaction type: {request.transaction_type}")


def build_hotel_agent_prompt(request: HotelRequest) -> str:
    return (
        "You are a hotel booking agent. Use the available tools to satisfy the user's request. "
        "Choose the tool calls yourself from the user's wording and prior tool results. "
        "Never call a tool with empty arguments; use identifiers shown in parentheses when a tool requires ids. "
        "Do not search for ids the user has already provided. "
        "Do not mention backend internals. If a tool returns OPERATION_ABORTED or ALREADY_COMPLETED, stop and give a brief final answer. "
        f"User request: {natural_language_request(request)}"
    )


def mutation_completed(metrics: AttemptMetrics, request: HotelRequest) -> bool:
    expected_tool = {
        "book": "finalize_booking",
        "hold": "hold_inventory",
        "cancel_booking": "cancel_booking",
        "price_refresh": "update_price_quote",
        "policy_update": "update_policy",
        "maintenance_block": "maintenance_block",
        "housekeeping_hold": "housekeeping_hold",
    }.get(request.transaction_type)
    if expected_tool is None:
        raise RuntimeError(f"unknown transaction type: {request.transaction_type}")
    return any(
        event.get("event") == "tool_success" and event.get("tool") == expected_tool
        for event in metrics.agent_trace
    )


def live_agent_run(
    store: HotelBookingStore,
    request: HotelRequest,
    *,
    model_id: str,
    max_agent_steps: int,
    metrics: AttemptMetrics,
) -> None:
    try:
        from smolagents import LiteLLMModel, LogLevel, ToolCallingAgent, tool
    except ImportError as exc:
        raise RuntimeError(
            "smolagents[litellm] is required for live hotel booking benchmark runs. "
            "Use --scripted-agent for deterministic tests."
        ) from exc
    transaction_abort: list[Exception] = []
    completed_mutation: list[str] = []

    def request_variant(
        *,
        transaction_type: str,
        hotel_id: str,
        room_type_id: str,
        checkin_date: str,
        nights: int,
        new_rate_cents: int | None,
        new_min_stay: int | None,
        reservation_to_cancel: str | None,
    ) -> HotelRequest:
        return HotelRequest(
            request_id=request.request_id,
            transaction_type=transaction_type,
            hotel_id=hotel_id,
            room_type_id=room_type_id,
            guest_id=request.guest_id,
            checkin_date=checkin_date,
            nights=nights,
            amount_cents=request.amount_cents,
            new_rate_cents=new_rate_cents,
            new_min_stay=new_min_stay,
            reservation_to_cancel=reservation_to_cancel,
        )

    def run_mutating_tool(name: str, fn: Callable[[], str]) -> str:
        if transaction_abort:
            return "OPERATION_ABORTED: the operation already failed; stop and return final answer."
        if completed_mutation:
            return f"ALREADY_COMPLETED: {completed_mutation[0]}. Stop and return final answer."
        try:
            result = fn()
            completed_mutation.append(result)
            return result
        except Exception as exc:
            transaction_abort.append(exc)
            metrics.trace(
                "transaction_abort_observed",
                tool=name,
                error=abort_message(exc),
            )
            return f"OPERATION_ABORTED: {abort_message(exc)}"

    @tool
    def search_hotels(city: str, min_star_rating: int) -> list[dict[str, Any]]:
        """Find candidate hotels in a city when the user has not provided a hotel id.

        Do not use this tool if the user already named a hotel id such as hotel_001.
        Use the provided hotel id directly in inspect_stay, book_room, or the relevant operation tool.

        Args:
            city: City name to search in.
            min_star_rating: Minimum acceptable hotel star rating.
        """
        if transaction_abort:
            return [{"error": "OPERATION_ABORTED"}]
        return store.search_hotels(city, min_star_rating)

    @tool
    def inspect_stay(hotel_id: str, room_type_id: str, checkin_date: str, nights: int) -> dict[str, Any]:
        """Look up nightly prices and room inventory for a specific stay.

        This is a read-only lookup. It does not reserve, hold, book, cancel, or modify anything.

        Use this when the user has provided a hotel id, room type id, check-in date, and stay length,
        or after search_hotels has identified a hotel id.

        Args:
            hotel_id: Hotel identifier.
            room_type_id: Room type identifier.
            checkin_date: Check-in date in ISO format.
            nights: Number of stay nights.
        """
        if transaction_abort:
            return {"error": "OPERATION_ABORTED"}
        rates = store.search_room_rates(hotel_id, room_type_id, checkin_date, nights)
        availability = store.check_availability(hotel_id, room_type_id, checkin_date, nights)
        rooms = store.available_rooms(hotel_id, room_type_id, checkin_date, nights)
        total_cents = sum(int(row["rate_cents"]) for row in rates)
        min_available = min((int(row["available_room_count"]) for row in availability), default=0)
        return {
            "summary": {
                "hotel_id": hotel_id,
                "room_type_id": room_type_id,
                "checkin_date": checkin_date,
                "nights": nights,
                "available": min_available > 0,
                "min_available_count": min_available,
                "total_cents": total_cents,
                "total_usd": f"{total_cents / 100:.2f}",
            },
            "rates": rates,
            "availability": availability,
            "recommended_rooms": rooms[:5],
        }

    @tool
    def book_room(hotel_id: str, room_type_id: str, checkin_date: str, nights: int, room_id: str) -> str:
        """Create and confirm one hotel room booking for the specified stay.

        This is the operation tool that satisfies a guest request to book a room. It creates
        the reservation, occupies the specific physical room, records payment, and confirms
        the reservation. Choose room_id from inspect_stay's recommended_rooms.

        Args:
            hotel_id: Hotel identifier.
            room_type_id: Room type identifier.
            checkin_date: Check-in date in ISO format.
            nights: Number of stay nights.
            room_id: Specific physical room identifier from inspect_stay.

        Returns:
            Completion status for the booking operation.
        """
        def run() -> str:
            booking = request_variant(
                transaction_type="book",
                hotel_id=hotel_id,
                room_type_id=room_type_id,
                checkin_date=checkin_date,
                nights=nights,
                new_rate_cents=request.new_rate_cents,
                new_min_stay=request.new_min_stay,
                reservation_to_cancel=request.reservation_to_cancel,
            )
            reservation_id = store.create_reservation(booking, status="pending", room_id=room_id)
            store.reserve_inventory(booking, reservation_id, room_id)
            store.record_payment(reservation_id, booking.amount_cents)
            store.finalize_booking(reservation_id)
            return f"BOOKED reservation_id={reservation_id} room_id={room_id}"

        return run_mutating_tool("book_room", run)

    @tool
    def place_hold(hotel_id: str, room_type_id: str, checkin_date: str, nights: int, room_id: str) -> str:
        """Place one temporary hold on hotel room inventory for the specified stay.

        Use this when the user wants time to decide but does not want a confirmed booking yet.

        Args:
            hotel_id: Hotel identifier.
            room_type_id: Room type identifier.
            checkin_date: Check-in date in ISO format.
            nights: Number of stay nights.
            room_id: Specific physical room identifier from inspect_stay.

        Returns:
            Completion status for the hold operation.
        """
        def run() -> str:
            hold = request_variant(
                transaction_type="hold",
                hotel_id=hotel_id,
                room_type_id=room_type_id,
                checkin_date=checkin_date,
                nights=nights,
                new_rate_cents=request.new_rate_cents,
                new_min_stay=request.new_min_stay,
                reservation_to_cancel=request.reservation_to_cancel,
            )
            reservation_id = store.create_reservation(hold, status="held", room_id=room_id)
            store.hold_inventory(hold, reservation_id, room_id)
            return f"HELD reservation_id={reservation_id} room_id={room_id}"

        return run_mutating_tool("place_hold", run)

    @tool
    def cancel_reservation(reservation_id: str) -> str:
        """Cancel an existing hotel reservation.

        Use this when the user asks to cancel a known reservation id.

        Args:
            reservation_id: Reservation identifier to cancel.

        Returns:
            Completion status for the cancellation operation.
        """
        def run() -> str:
            cancel = request_variant(
                transaction_type="cancel_booking",
                hotel_id=request.hotel_id,
                room_type_id=request.room_type_id,
                checkin_date=request.checkin_date,
                nights=request.nights,
                new_rate_cents=request.new_rate_cents,
                new_min_stay=request.new_min_stay,
                reservation_to_cancel=reservation_id,
            )
            store.cancel_booking(cancel)
            return f"CANCELLED reservation_id={reservation_id}"

        return run_mutating_tool("cancel_reservation", run)

    @tool
    def refresh_price_quote(
        hotel_id: str,
        room_type_id: str,
        checkin_date: str,
        nights: int,
        new_rate_cents: int,
    ) -> str:
        """Set a new promotional nightly price quote for the specified stay.

        Use this for staff or system requests to refresh pricing, not for ordinary guest booking.

        Args:
            hotel_id: Hotel identifier.
            room_type_id: Room type identifier.
            checkin_date: Check-in date in ISO format.
            nights: Number of stay nights.
            new_rate_cents: New promotional nightly rate in cents.

        Returns:
            Completion status for the price refresh operation.
        """
        def run() -> str:
            price = request_variant(
                transaction_type="price_refresh",
                hotel_id=hotel_id,
                room_type_id=room_type_id,
                checkin_date=checkin_date,
                nights=nights,
                new_rate_cents=new_rate_cents,
                new_min_stay=request.new_min_stay,
                reservation_to_cancel=request.reservation_to_cancel,
            )
            store.update_price_quote(price)
            return "PRICE_REFRESHED"

        return run_mutating_tool("refresh_price_quote", run)

    @tool
    def update_min_stay_policy(
        hotel_id: str,
        room_type_id: str,
        checkin_date: str,
        nights: int,
        new_min_stay: int,
    ) -> str:
        """Update the minimum-stay policy for the specified hotel stay window.

        Use this for staff or system policy changes, not for ordinary guest booking.

        Args:
            hotel_id: Hotel identifier.
            room_type_id: Room type identifier.
            checkin_date: Check-in date in ISO format.
            nights: Number of stay nights.
            new_min_stay: New minimum stay length in nights.

        Returns:
            Completion status for the policy update operation.
        """
        def run() -> str:
            policy = request_variant(
                transaction_type="policy_update",
                hotel_id=hotel_id,
                room_type_id=room_type_id,
                checkin_date=checkin_date,
                nights=nights,
                new_rate_cents=request.new_rate_cents,
                new_min_stay=new_min_stay,
                reservation_to_cancel=request.reservation_to_cancel,
            )
            store.update_policy(policy)
            return "POLICY_UPDATED"

        return run_mutating_tool("update_min_stay_policy", run)

    @tool
    def block_for_maintenance(hotel_id: str, room_type_id: str, checkin_date: str, nights: int, room_id: str) -> str:
        """Remove one room from guest inventory for maintenance for the specified stay.

        Use this for staff or operations requests to block a specific room for maintenance.
        Choose room_id from inspect_stay's recommended_rooms.

        Args:
            hotel_id: Hotel identifier.
            room_type_id: Room type identifier.
            checkin_date: Check-in date in ISO format.
            nights: Number of stay nights.
            room_id: Specific physical room identifier from inspect_stay.

        Returns:
            Completion status for the maintenance block operation.
        """
        def run() -> str:
            maintenance = request_variant(
                transaction_type="maintenance_block",
                hotel_id=hotel_id,
                room_type_id=room_type_id,
                checkin_date=checkin_date,
                nights=nights,
                new_rate_cents=request.new_rate_cents,
                new_min_stay=request.new_min_stay,
                reservation_to_cancel=request.reservation_to_cancel,
            )
            store.maintenance_block(maintenance, room_id)
            return f"MAINTENANCE_BLOCKED room_id={room_id}"

        return run_mutating_tool("block_for_maintenance", run)

    @tool
    def add_housekeeping_hold(hotel_id: str, room_type_id: str, checkin_date: str, nights: int, room_id: str) -> str:
        """Remove one room from guest inventory for housekeeping for the specified stay.

        Use this for staff or operations requests to hold a specific room for housekeeping.
        Choose room_id from inspect_stay's recommended_rooms.

        Args:
            hotel_id: Hotel identifier.
            room_type_id: Room type identifier.
            checkin_date: Check-in date in ISO format.
            nights: Number of stay nights.
            room_id: Specific physical room identifier from inspect_stay.

        Returns:
            Completion status for the housekeeping hold operation.
        """
        def run() -> str:
            housekeeping = request_variant(
                transaction_type="housekeeping_hold",
                hotel_id=hotel_id,
                room_type_id=room_type_id,
                checkin_date=checkin_date,
                nights=nights,
                new_rate_cents=request.new_rate_cents,
                new_min_stay=request.new_min_stay,
                reservation_to_cancel=request.reservation_to_cancel,
            )
            store.housekeeping_hold(housekeeping, room_id)
            return f"HOUSEKEEPING_HELD room_id={room_id}"

        return run_mutating_tool("add_housekeeping_hold", run)

    def fail_fast_on_agent_error(step: Any, agent: Any = None) -> None:
        error = getattr(step, "error", None)
        if error is None:
            return
        steps = list(getattr(getattr(agent, "memory", None), "steps", None) or []) + [step]
        for index, memory_step in enumerate(steps):
            step_usage = token_usage_tuple(getattr(memory_step, "token_usage", None))
            if step_usage is None:
                continue
            metrics.prompt_tokens += step_usage[0]
            metrics.completion_tokens += step_usage[1]
            metrics.total_tokens += step_usage[2]
            metrics.llm_calls += 1
            metrics.trace(
                "llm_step",
                step=index + 1,
                step_type=type(memory_step).__name__,
                prompt_tokens=step_usage[0],
                completion_tokens=step_usage[1],
                total_tokens=step_usage[2],
            )
        reason = abort_message(error)
        metrics.trace("agent_tool_contract_error", error=reason)
        raise AgentToolContractError(reason)

    if model_id.startswith("openrouter/") and not os.environ.get("OPENROUTER_API_KEY"):
        raise RuntimeError("OPENROUTER_API_KEY is required for the default OpenRouter model")
    model = LiteLLMModel(model_id=model_id, temperature=0)
    agent = ToolCallingAgent(
        tools=[
            search_hotels,
            inspect_stay,
            book_room,
            place_hold,
            cancel_reservation,
            refresh_price_quote,
            update_min_stay_policy,
            block_for_maintenance,
            add_housekeeping_hold,
        ],
        model=model,
        max_steps=max_agent_steps,
        verbosity_level=LogLevel.DEBUG,
        step_callbacks=[fail_fast_on_agent_error],
    )
    prompt = build_hotel_agent_prompt(request)
    timer = Timer()
    print(
        f"  agent_run: backend={metrics.backend} request_id={request.request_id} "
        f"transaction_type={request.transaction_type} model={model_id}",
        flush=True,
    )
    metrics.trace("llm_run_start", backend=metrics.backend, model=model_id, prompt=prompt)
    result = agent.run(prompt)
    llm_elapsed = timer.elapsed_ms()
    metrics.llm_latency_ms += llm_elapsed
    metrics.llm_calls += max(1, getattr(agent, "step_number", 1))
    usage = extract_smolagents_usage(agent, model, result)
    metrics.prompt_tokens += usage[0]
    metrics.completion_tokens += usage[1]
    metrics.total_tokens += usage[2]
    metrics.trace(
        "llm_run_end",
        model=model_id,
        elapsed_ms=llm_elapsed,
        prompt_tokens=usage[0],
        completion_tokens=usage[1],
        total_tokens=usage[2],
        output=str(result)[:2000],
    )
    for index, step in enumerate(getattr(getattr(agent, "memory", None), "steps", None) or []):
        step_usage = token_usage_tuple(getattr(step, "token_usage", None))
        if step_usage is None:
            continue
        metrics.trace(
            "llm_step",
            step=index,
            step_type=type(step).__name__,
            prompt_tokens=step_usage[0],
            completion_tokens=step_usage[1],
            total_tokens=step_usage[2],
        )
    if transaction_abort:
        raise AgentTransactionAborted(abort_message(transaction_abort[0]))
    if not mutation_completed(metrics, request):
        raise RuntimeError("agent did not complete the requested hotel operation")


def run_agent_workflow(
    store: HotelBookingStore,
    request: HotelRequest,
    *,
    scripted_agent: bool,
    model: str,
    max_agent_steps: int,
    metrics: AttemptMetrics,
) -> None:
    if scripted_agent:
        scripted_agent_run(store, request)
    else:
        live_agent_run(
            store,
            request,
            model_id=model,
            max_agent_steps=max_agent_steps,
            metrics=metrics,
        )


def token_usage_tuple(usage: Any) -> tuple[int, int, int] | None:
    if usage is None:
        return None
    if isinstance(usage, dict):
        prompt = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
        completion = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
        total = int(usage.get("total_tokens") or prompt + completion)
        return prompt, completion, total
    prompt = getattr(usage, "prompt_tokens", None)
    if prompt is None:
        prompt = getattr(usage, "input_tokens", None)
    completion = getattr(usage, "completion_tokens", None)
    if completion is None:
        completion = getattr(usage, "output_tokens", None)
    if prompt is None and completion is None:
        return None
    prompt = int(prompt or 0)
    completion = int(completion or 0)
    total = int(getattr(usage, "total_tokens", prompt + completion))
    return prompt, completion, total


def extract_smolagents_usage(agent: Any, model: Any, result: Any = None) -> tuple[int, int, int]:
    for usage in (getattr(result, "token_usage", None),):
        parsed = token_usage_tuple(usage)
        if parsed is not None:
            return parsed

    monitor = getattr(agent, "monitor", None)
    if monitor is not None and hasattr(monitor, "get_total_token_counts"):
        parsed = token_usage_tuple(monitor.get_total_token_counts())
        if parsed is not None:
            return parsed

    for owner in (agent, monitor, model):
        if owner is None:
            continue
        for attr in ("total_token_usage", "token_usage", "usage"):
            parsed = token_usage_tuple(getattr(owner, attr, None))
            if parsed is not None:
                return parsed

    steps = getattr(getattr(agent, "memory", None), "steps", None) or []
    prompt = 0
    completion = 0
    for step in steps:
        parsed = token_usage_tuple(getattr(step, "token_usage", None))
        if parsed is not None:
            prompt += parsed[0]
            completion += parsed[1]
    if prompt or completion:
        return prompt, completion, prompt + completion
    return 0, 0, 0


def abort_message(exc: Exception) -> str:
    detail = str(exc).strip()
    if not detail:
        return type(exc).__name__
    detail = " ".join(detail.split())
    return f"{type(exc).__name__}: {detail[:240]}"


def absorb_retry_metrics(target: AttemptMetrics, source: AttemptMetrics) -> None:
    for field_name in (
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "llm_calls",
        "tool_calls",
        "total_latency_ms",
        "llm_latency_ms",
        "tool_latency_ms",
        "transaction_latency_ms",
        "commit_or_merge_latency_ms",
        "merge_conflicts",
        "merge_resolved",
        "merge_rejected",
        "same_row_disjoint_resolved",
        "counter_arithmetic_resolved",
        "semantic_policy_rejected",
        "compensation_attempts",
        "compensation_failures",
        "workflow_attempts",
        "workflow_aborts",
    ):
        setattr(target, field_name, getattr(target, field_name) + getattr(source, field_name))
    for event in source.agent_trace:
        copied = dict(event)
        copied["index"] = len(target.agent_trace)
        target.agent_trace.append(copied)


def is_transaction_abort(reason: str) -> bool:
    return any(
        marker in reason
        for marker in (
            "SerializationFailure",
            "DeadlockDetected",
            "InFailedSqlTransaction",
            "AgentTransactionAborted",
            "could not serialize access",
            "deadlock detected",
            "_chronos_b_interval_",
            "room_occupied",
            "room_occupancy_pkey",
        )
    )


@contextmanager
def snapshot_transaction(db: SQLDatabaseAdapter) -> Iterator[None]:
    if db.in_transaction:
        db.commit()
    db.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
    try:
        yield
    except Exception:
        db.rollback()
        raise
    else:
        db.commit()


def run_big_txn_attempt(
    database_url: str,
    request: HotelRequest,
    *,
    scripted_agent: bool,
    model: str,
    max_agent_steps: int,
    max_retries: int,
    barrier: threading.Barrier | None,
) -> AttemptMetrics:
    metrics = AttemptMetrics("big_txn", request.request_id, request.transaction_type)
    total = Timer()
    for attempt_index in range(max_retries + 1):
        metrics.workflow_attempts += 1
        metrics.trace("workflow_attempt_start", attempt=attempt_index + 1)
        db = connect_sql_database(database_url)
        try:
            txn_timer = Timer()
            with snapshot_transaction(db):
                metrics.transaction_latency_ms += txn_timer.elapsed_ms()
                store = HotelBookingStore(db, metrics=metrics)
                if barrier is not None and attempt_index == 0:
                    store.check_availability(request.hotel_id, request.room_type_id, request.checkin_date, request.nights)
                    barrier.wait(timeout=30)
                run_agent_workflow(
                    store,
                    request,
                    scripted_agent=scripted_agent,
                    model=model,
                    max_agent_steps=max_agent_steps,
                    metrics=metrics,
                )
                commit_timer = Timer()
            metrics.commit_or_merge_latency_ms += commit_timer.elapsed_ms()
            metrics.success = True
            metrics.abort_reason = ""
            metrics.trace("workflow_attempt_success", attempt=attempt_index + 1)
            break
        except Exception as exc:
            reason = abort_message(exc)
            metrics.abort_reason = reason
            metrics.workflow_aborts += 1
            metrics.trace("workflow_attempt_abort", attempt=attempt_index + 1, error=reason)
            if attempt_index >= max_retries or not is_transaction_abort(reason):
                break
            metrics.trace("workflow_retry", next_attempt=attempt_index + 2, previous_error=reason)
        finally:
            db.close()
    metrics.total_latency_ms = total.elapsed_ms()
    return metrics


def restore_inventory_rows(db: SQLDatabaseAdapter, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        assignments = ", ".join(
            f"{column} = :{column}"
            for column in INVENTORY_COLUMNS
            if column not in {"hotel_id", "room_type_id", "stay_date"}
        )
        db.execute(
            f"""
            UPDATE room_inventory
            SET {assignments}
            WHERE hotel_id = :hotel_id
              AND room_type_id = :room_type_id
              AND stay_date = :stay_date
            """,
            row,
        )


def restore_occupancy_rows(db: SQLDatabaseAdapter, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        db.execute(
            """
            INSERT INTO room_occupancy (
              room_id, stay_date, reservation_id, hotel_id, room_type_id, occupancy_type
            ) VALUES (
              :room_id, :stay_date, :reservation_id, :hotel_id, :room_type_id, :occupancy_type
            )
            ON CONFLICT (room_id, stay_date) DO UPDATE
            SET reservation_id = EXCLUDED.reservation_id,
                hotel_id = EXCLUDED.hotel_id,
                room_type_id = EXCLUDED.room_type_id,
                occupancy_type = EXCLUDED.occupancy_type
            """,
            row,
        )


def run_saga_attempt(
    database_url: str,
    request: HotelRequest,
    *,
    scripted_agent: bool,
    model: str,
    max_agent_steps: int,
    max_retries: int,
    barrier: threading.Barrier | None,
) -> AttemptMetrics:
    metrics = AttemptMetrics("saga", request.request_id, request.transaction_type)
    total = Timer()
    for attempt_index in range(max_retries + 1):
        metrics.workflow_attempts += 1
        metrics.trace("workflow_attempt_start", attempt=attempt_index + 1)
        db = connect_sql_database(database_url)
        store: SagaHotelBookingStore | None = None
        try:
            store = SagaHotelBookingStore(db, metrics=metrics)
            if barrier is not None and attempt_index == 0:
                store.check_availability(request.hotel_id, request.room_type_id, request.checkin_date, request.nights)
                barrier.wait(timeout=30)
            run_agent_workflow(
                store,
                request,
                scripted_agent=scripted_agent,
                model=model,
                max_agent_steps=max_agent_steps,
                metrics=metrics,
            )
            metrics.success = True
            metrics.abort_reason = ""
            metrics.trace("workflow_attempt_success", attempt=attempt_index + 1)
            break
        except Exception as exc:
            reason = abort_message(exc)
            metrics.abort_reason = reason
            metrics.workflow_aborts += 1
            metrics.trace("workflow_attempt_abort", attempt=attempt_index + 1, error=reason)
            if store is not None:
                store.compensate()
            if attempt_index >= max_retries or not is_transaction_abort(reason):
                break
            metrics.trace("workflow_retry", next_attempt=attempt_index + 2, previous_error=reason)
        finally:
            db.close()
    metrics.total_latency_ms = total.elapsed_ms()
    return metrics


def run_branch_attempt(
    database_url: str,
    request: HotelRequest,
    *,
    scripted_agent: bool,
    model: str,
    max_agent_steps: int,
    barrier: threading.Barrier | None,
    branch_create_lock: threading.Lock | None = None,
) -> BranchAttempt:
    metrics = AttemptMetrics("branch", request.request_id, request.transaction_type)
    metrics.workflow_attempts += 1
    metrics.trace("workflow_attempt_start", attempt=metrics.workflow_attempts)
    branch_id = f"hotel_{request.request_id}_{uuid.uuid4().hex[:8]}"
    total = Timer()
    ctx = connect_chronos(database_url)
    attempt = BranchAttempt(
        request=request,
        branch_id=branch_id,
        metrics=metrics,
    )
    try:
        with branch_create_lock or nullcontext():
            ctx.create_branch(branch_id, from_branch="main")
        session = ctx.checkout(branch_id)

        def record_base(key: tuple[str, str, str], row: dict[str, Any]) -> None:
            attempt.touched_inventory.setdefault(key, dict(row))

        store = HotelBookingStore(session, metrics=metrics, base_row_recorder=record_base)
        if barrier is not None:
            store.check_availability(request.hotel_id, request.room_type_id, request.checkin_date, request.nights)
            barrier.wait(timeout=30)
        txn_timer = Timer()
        run_agent_workflow(
            store,
            request,
            scripted_agent=scripted_agent,
            model=model,
            max_agent_steps=max_agent_steps,
            metrics=metrics,
        )
        metrics.transaction_latency_ms += txn_timer.elapsed_ms()
        attempt.completed = True
        metrics.trace("workflow_attempt_prepared", attempt=metrics.workflow_attempts, branch_id=branch_id)
    except Exception as exc:
        metrics.abort_reason = abort_message(exc)
        metrics.workflow_aborts += 1
        metrics.trace("workflow_attempt_abort", attempt=metrics.workflow_attempts, error=metrics.abort_reason)
        safe_delete_branches(ctx, branch_id)
    finally:
        metrics.total_latency_ms = total.elapsed_ms()
        ctx.close()
    return attempt


COLUMN_GROUPS = {
    "capacity": {"available_count", "reserved_count", "held_count"},
    "pricing": {"base_rate_cents", "promo_rate_cents", "last_quote_cents", "last_priced_at"},
    "policy": {"min_stay_nights", "policy_note"},
    "ops": {"cleaning_hold_count", "maintenance_blocked_count"},
}


def changed_columns(base: dict[str, Any], row: dict[str, Any]) -> set[str]:
    return {
        column
        for column in INVENTORY_COLUMNS
        if column not in {"hotel_id", "room_type_id", "stay_date"}
        and base.get(column) != row.get(column)
    }


def changed_groups(columns: set[str]) -> set[str]:
    return {
        group
        for group, group_columns in COLUMN_GROUPS.items()
        if columns & group_columns
    }


def inventory_valid(row: dict[str, Any]) -> bool:
    if int(row["available_count"]) < 0:
        return False
    used = (
        int(row["available_count"])
        + int(row["held_count"])
        + int(row["reserved_count"])
        + int(row["cleaning_hold_count"])
        + int(row["maintenance_blocked_count"])
    )
    return used <= int(row["total_capacity"])


@dataclass
class ConflictDecision:
    resolved: bool
    reason: str
    merged_row: dict[str, Any] | None = None


def merge_inventory_conflict(
    *,
    request: HotelRequest,
    base: dict[str, Any],
    source: dict[str, Any],
    target: dict[str, Any],
) -> ConflictDecision:
    source_columns = changed_columns(base, source)
    target_columns = changed_columns(base, target)
    source_groups = changed_groups(source_columns)
    target_groups = changed_groups(target_columns)
    merged = dict(target)

    if source_groups.isdisjoint(target_groups):
        if ("capacity" in source_groups and "policy" in target_groups) or (
            "policy" in source_groups and "capacity" in target_groups
        ):
            policy_row = source if "policy" in source_groups else target
            capacity_row = source if "capacity" in source_groups else target
            min_stay = int(policy_row["min_stay_nights"])
            reservation_added = int(capacity_row["reserved_count"]) > int(base["reserved_count"])
            if reservation_added and request.nights < min_stay:
                return ConflictDecision(False, "semantic_policy")
        for column in source_columns:
            merged[column] = source[column]
        if inventory_valid(merged):
            return ConflictDecision(True, "same_row_disjoint", merged)
        return ConflictDecision(False, "invariant")

    counter_groups = {"capacity", "ops"}
    if source_groups <= counter_groups and target_groups <= counter_groups:
        for column in COLUMN_GROUPS["capacity"] | COLUMN_GROUPS["ops"]:
            if column in INVENTORY_COLUMNS:
                merged[column] = int(target[column]) + (int(source[column]) - int(base[column]))
        if inventory_valid(merged):
            return ConflictDecision(True, "counter_arithmetic", merged)
        return ConflictDecision(False, "capacity")

    overlap = source_columns & target_columns
    if overlap:
        if overlap & {"min_stay_nights", "policy_note"}:
            return ConflictDecision(False, "semantic_policy")
        return ConflictDecision(False, "same_column")
    return ConflictDecision(False, "semantic")


def apply_full_row(session: Any, table: str, row: dict[str, Any]) -> None:
    columns = list(row)
    placeholders = ", ".join(f":{column}" for column in columns)
    col_sql = ", ".join(columns)
    session.execute(
        f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
        row,
    )
    set_columns = [column for column in columns if column not in set(HOTEL_TABLES[table])]
    if set_columns:
        set_sql = ", ".join(f"{column} = :{column}" for column in set_columns)
        where_sql = " AND ".join(f"{column} = :{column}" for column in HOTEL_TABLES[table])
        session.execute(f"UPDATE {table} SET {set_sql} WHERE {where_sql}", row)


def apply_delete(session: Any, table: str, key: dict[str, Any]) -> None:
    where_sql = " AND ".join(f"{column} = :{column}" for column in key)
    session.execute(f"DELETE FROM {table} WHERE {where_sql}", key)


def apply_inventory_row(session: Any, row: dict[str, Any]) -> None:
    set_sql = ", ".join(
        f"{column} = :{column}"
        for column in INVENTORY_COLUMNS
        if column not in {"hotel_id", "room_type_id", "stay_date"}
    )
    session.execute(
        f"""
        UPDATE room_inventory
        SET {set_sql}
        WHERE hotel_id = :hotel_id
          AND room_type_id = :room_type_id
          AND stay_date = :stay_date
        """,
        row,
    )


def safe_delete_branches(ctx: ChronosBranchContext, *branch_ids: str) -> None:
    for branch_id in branch_ids:
        try:
            ctx.delete_branch(branch_id)
        except Exception:
            pass


def apply_row_change(session: Any, change: Any) -> None:
    if change.change == "deleted":
        apply_delete(session, change.table, change.key)
    else:
        assert change.after is not None
        apply_full_row(session, change.table, change.after)


def physical_room_change_conflict(session: Any, change: Any) -> str:
    if change.table != "room_occupancy" or change.change == "deleted" or change.after is None:
        return ""
    row = change.after
    existing = session.query(
        """
        SELECT reservation_id, occupancy_type
        FROM room_occupancy
        WHERE room_id = :room_id
          AND stay_date = :stay_date
        """,
        {"room_id": row["room_id"], "stay_date": row["stay_date"]},
    )
    if existing and existing[0]["reservation_id"] != row["reservation_id"]:
        return "room_occupied"
    return ""


@dataclass
class SnapshotIsolationMergePlan:
    clean_changes: list[Any] = field(default_factory=list)
    resolved_inventory_rows: list[dict[str, Any]] = field(default_factory=list)
    rejected_reason: str = ""


def build_snapshot_isolation_merge_plan(
    *,
    request: HotelRequest,
    clean_changes: list[Any],
    conflicts: list[Any],
    touched_inventory: dict[tuple[str, str, str], dict[str, Any]],
    metrics: AttemptMetrics,
) -> SnapshotIsolationMergePlan:
    plan = SnapshotIsolationMergePlan(clean_changes=list(clean_changes))
    for conflict in conflicts:
        metrics.merge_conflicts += 1
        metrics.trace(
            "snapshot_isolation_conflict",
            table=conflict.table,
            key=conflict.key,
            change=conflict.change,
        )
        if conflict.table == "room_occupancy":
            plan.rejected_reason = "room_occupied"
            metrics.merge_rejected += 1
            return plan
        if conflict.table != "room_inventory" or conflict.after is None or conflict.before is None:
            plan.rejected_reason = f"snapshot_write_conflict:{conflict.table}"
            metrics.merge_rejected += 1
            return plan

        key_tuple = (
            conflict.key["hotel_id"],
            conflict.key["room_type_id"],
            conflict.key["stay_date"],
        )
        base = touched_inventory.get(key_tuple)
        if base is None:
            plan.rejected_reason = "missing_base_row"
            metrics.merge_rejected += 1
            return plan

        decision = merge_inventory_conflict(
            request=request,
            base=base,
            source=conflict.after,
            target=conflict.before,
        )
        if not decision.resolved:
            plan.rejected_reason = decision.reason
            metrics.merge_rejected += 1
            if decision.reason == "semantic_policy":
                metrics.semantic_policy_rejected += 1
            return plan

        assert decision.merged_row is not None
        plan.resolved_inventory_rows.append(decision.merged_row)
        metrics.merge_resolved += 1
        if decision.reason == "same_row_disjoint":
            metrics.same_row_disjoint_resolved += 1
        elif decision.reason == "counter_arithmetic":
            metrics.counter_arithmetic_resolved += 1
        metrics.trace(
            "snapshot_isolation_conflict_resolved",
            table=conflict.table,
            key=conflict.key,
            reason=decision.reason,
        )
    return plan


def merge_prepared_branch_attempt(
    database_url: str,
    attempt: BranchAttempt,
    *,
    merge_lock: threading.Lock | None = None,
) -> bool:
    metrics = attempt.metrics
    timer = Timer()
    lock = merge_lock or nullcontext()
    try:
        with lock:
            ctx = connect_chronos(database_url)
            try:
                write_diff = ctx.diff("main", attempt.branch_id)
                preview = ctx.merge_preview(attempt.branch_id, "main")
                metrics.trace(
                    "snapshot_isolation_write_set",
                    branch_id=attempt.branch_id,
                    rows=len(write_diff.changes),
                    clean_rows=len(preview.changes),
                    conflict_rows=len(preview.conflicts),
                )
                if not write_diff.changes:
                    metrics.success = True
                    metrics.abort_reason = ""
                    metrics.trace(
                        "workflow_attempt_success",
                        attempt=metrics.workflow_attempts,
                        branch_id=attempt.branch_id,
                    )
                    safe_delete_branches(ctx, attempt.branch_id)
                    return False

                plan = build_snapshot_isolation_merge_plan(
                    request=attempt.request,
                    clean_changes=preview.changes,
                    conflicts=preview.conflicts,
                    touched_inventory=attempt.touched_inventory,
                    metrics=metrics,
                )
                if plan.rejected_reason:
                    metrics.workflow_aborts += 1
                    metrics.abort_reason = plan.rejected_reason
                    metrics.trace(
                        "workflow_attempt_abort",
                        attempt=metrics.workflow_attempts,
                        branch_id=attempt.branch_id,
                        error=metrics.abort_reason,
                    )
                    safe_delete_branches(ctx, attempt.branch_id)
                    return is_transaction_abort(metrics.abort_reason)

                main = ctx.checkout("main")
                with main.transaction():
                    for change in plan.clean_changes:
                        physical_conflict = physical_room_change_conflict(main, change)
                        if physical_conflict:
                            raise AgentTransactionAborted(physical_conflict)
                        apply_row_change(main, change)
                    for row in plan.resolved_inventory_rows:
                        apply_inventory_row(main, row)
                metrics.merge_resolved += len(plan.clean_changes)
                metrics.success = True
                metrics.abort_reason = ""
                metrics.trace(
                    "workflow_attempt_success",
                    attempt=metrics.workflow_attempts,
                    branch_id=attempt.branch_id,
                )
                safe_delete_branches(ctx, attempt.branch_id)
                return False
            finally:
                ctx.close()
    except Exception as exc:
        metrics.abort_reason = abort_message(exc)
        metrics.workflow_aborts += 1
        metrics.trace(
            "workflow_attempt_abort",
            attempt=metrics.workflow_attempts,
            branch_id=attempt.branch_id,
            error=metrics.abort_reason,
        )
        try:
            cleanup = connect_chronos(database_url)
            try:
                safe_delete_branches(cleanup, attempt.branch_id)
            finally:
                cleanup.close()
        except Exception:
            pass
        return is_transaction_abort(metrics.abort_reason)
    finally:
        metrics.commit_or_merge_latency_ms += timer.elapsed_ms()


def run_branch_transaction_attempt(
    database_url: str,
    request: HotelRequest,
    *,
    scripted_agent: bool,
    model: str,
    max_agent_steps: int,
    max_retries: int,
    barrier: threading.Barrier | None,
    branch_create_lock: threading.Lock,
    merge_lock: threading.Lock,
) -> AttemptMetrics:
    merged_metrics: AttemptMetrics | None = None
    for attempt_index in range(max_retries + 1):
        attempt = run_branch_attempt(
            database_url,
            request,
            scripted_agent=scripted_agent,
            model=model,
            max_agent_steps=max_agent_steps,
            barrier=barrier if attempt_index == 0 else None,
            branch_create_lock=branch_create_lock,
        )
        if merged_metrics is None:
            merged_metrics = attempt.metrics
        else:
            absorb_retry_metrics(merged_metrics, attempt.metrics)
            merged_metrics.abort_reason = attempt.metrics.abort_reason
            merged_metrics.success = attempt.metrics.success
            attempt.metrics = merged_metrics

        retry_after_abort = False
        if attempt.completed:
            retry_after_abort = merge_prepared_branch_attempt(
                database_url,
                attempt,
                merge_lock=merge_lock,
            )
        else:
            retry_after_abort = is_transaction_abort(merged_metrics.abort_reason)

        if merged_metrics.success:
            break
        if attempt_index >= max_retries or not retry_after_abort:
            break
        merged_metrics.trace(
            "workflow_retry",
            next_attempt=merged_metrics.workflow_attempts + 1,
            previous_error=merged_metrics.abort_reason,
        )

    assert merged_metrics is not None
    return merged_metrics


def merge_branch_attempts(
    database_url: str,
    attempts: list[BranchAttempt],
    *,
    scripted_agent: bool,
    model: str,
    max_agent_steps: int,
    max_retries: int,
) -> list[AttemptMetrics]:
    for index, attempt in enumerate(attempts):
        retry_count = 0
        metrics = attempt.metrics
        while True:
            retry_after_abort = False
            if attempt.completed:
                retry_after_abort = merge_prepared_branch_attempt(database_url, attempt)
            else:
                retry_after_abort = is_transaction_abort(metrics.abort_reason)

            if metrics.success:
                break
            if retry_count >= max_retries or not retry_after_abort:
                break

            retry_count += 1
            metrics.trace(
                "workflow_retry",
                next_attempt=metrics.workflow_attempts + 1,
                previous_error=metrics.abort_reason,
            )
            retry = run_branch_attempt(
                database_url,
                attempt.request,
                scripted_agent=scripted_agent,
                model=model,
                max_agent_steps=max_agent_steps,
                barrier=None,
                branch_create_lock=None,
            )
            absorb_retry_metrics(metrics, retry.metrics)
            metrics.abort_reason = retry.metrics.abort_reason
            metrics.success = retry.metrics.success
            retry.metrics = metrics
            attempt = retry
            attempts[index] = attempt
    return [attempt.metrics for attempt in attempts]


def run_parallel_metrics(
    backend: str,
    database_url: str,
    requests: list[HotelRequest],
    *,
    scripted_agent: bool,
    model: str,
    max_agent_steps: int,
    max_retries: int,
    parallel_agents: int,
) -> list[AttemptMetrics]:
    barrier = threading.Barrier(min(parallel_agents, len(requests))) if len(requests) > 1 else None
    results: list[Any] = [None] * len(requests)
    branch_create_lock = threading.Lock()
    merge_lock = threading.Lock()

    def run_index(index: int, request: HotelRequest) -> None:
        if backend == "big_txn":
            results[index] = run_big_txn_attempt(
                database_url,
                request,
                scripted_agent=scripted_agent,
                model=model,
                max_agent_steps=max_agent_steps,
                max_retries=max_retries,
                barrier=barrier,
            )
        elif backend == "saga":
            results[index] = run_saga_attempt(
                database_url,
                request,
                scripted_agent=scripted_agent,
                model=model,
                max_agent_steps=max_agent_steps,
                max_retries=max_retries,
                barrier=barrier,
            )
        elif backend == "branch":
            results[index] = run_branch_transaction_attempt(
                database_url,
                request,
                scripted_agent=scripted_agent,
                model=model,
                max_agent_steps=max_agent_steps,
                max_retries=max_retries,
                barrier=barrier,
                branch_create_lock=branch_create_lock,
                merge_lock=merge_lock,
            )
        else:
            raise AssertionError(backend)

    for start in range(0, len(requests), parallel_agents):
        chunk = requests[start : start + parallel_agents]
        barrier = threading.Barrier(len(chunk)) if len(chunk) > 1 else None
        threads = [
            threading.Thread(target=run_index, args=(start + offset, request), daemon=True)
            for offset, request in enumerate(chunk)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    return [item for item in results if isinstance(item, AttemptMetrics)]


def summarize_metrics(rows: list[AttemptMetrics], parallel_agents: int) -> dict[str, Any]:
    commits = sum(1 for row in rows if row.success)
    attempts = sum(row.workflow_attempts for row in rows)
    aborts = sum(row.workflow_aborts for row in rows)
    latencies = [row.total_latency_ms for row in rows]
    total_tokens = sum(row.total_tokens for row in rows)
    return {
        "backend": rows[0].backend if rows else "",
        "requests": len(rows),
        "parallel_agents": parallel_agents,
        "attempts": attempts,
        "commits": commits,
        "aborts": aborts,
        "abort_rate": (aborts / attempts) if attempts else 0.0,
        "prompt_tokens": sum(row.prompt_tokens for row in rows),
        "completion_tokens": sum(row.completion_tokens for row in rows),
        "total_tokens": total_tokens,
        "tokens_per_success": (total_tokens / commits) if commits else 0.0,
        "merge_conflicts": sum(row.merge_conflicts for row in rows),
        "merge_resolved": sum(row.merge_resolved for row in rows),
        "merge_rejected": sum(row.merge_rejected for row in rows),
        "same_row_disjoint_resolved": sum(row.same_row_disjoint_resolved for row in rows),
        "counter_arithmetic_resolved": sum(row.counter_arithmetic_resolved for row in rows),
        "semantic_policy_rejected": sum(row.semantic_policy_rejected for row in rows),
        "compensation_attempts": sum(row.compensation_attempts for row in rows),
        "compensation_failures": sum(row.compensation_failures for row in rows),
        "avg_ms": statistics.fmean(latencies) if latencies else 0.0,
        "p50_ms": statistics.median(latencies) if latencies else 0.0,
        "p95_ms": percentile(latencies, 0.95),
        "total_latency_ms": sum(latencies),
        "llm_latency_ms": sum(row.llm_latency_ms for row in rows),
        "tool_latency_ms": sum(row.tool_latency_ms for row in rows),
        "transaction_latency_ms": sum(row.transaction_latency_ms for row in rows),
        "commit_or_merge_latency_ms": sum(row.commit_or_merge_latency_ms for row in rows),
    }


def verify_database(database_url: str, backend: str) -> dict[str, Any]:
    physical_checks = [
        """
        SELECT COUNT(*) AS count
        FROM (
          SELECT room_id, stay_date
          FROM room_occupancy
          GROUP BY room_id, stay_date
          HAVING COUNT(*) > 1
        ) duplicate_occupancy
        """,
        """
        SELECT COUNT(*) AS count
        FROM room_occupancy o
        LEFT JOIN rooms r
          ON r.room_id = o.room_id
        WHERE r.room_id IS NULL
           OR r.hotel_id <> o.hotel_id
           OR r.room_type_id <> o.room_type_id
           OR r.status <> 'open'
        """,
        """
        SELECT COUNT(*) AS count
        FROM reservation_nights rn
        JOIN reservations r
          ON r.reservation_id = rn.reservation_id
        LEFT JOIN room_occupancy o
          ON o.room_id = rn.room_id
         AND o.stay_date = rn.stay_date
         AND o.reservation_id = rn.reservation_id
        WHERE r.status IN ('confirmed', 'held')
          AND o.room_id IS NULL
        """,
        """
        SELECT COUNT(*) AS count
        FROM room_occupancy o
        LEFT JOIN reservations r
          ON r.reservation_id = o.reservation_id
        WHERE o.occupancy_type IN ('reserved', 'held')
          AND (r.reservation_id IS NULL OR r.status NOT IN ('confirmed', 'held'))
        """,
    ]
    if backend == "branch":
        ctx = connect_chronos(database_url)
        try:
            main = ctx.checkout("main")
            invalid = sum(int(main.query(sql)[0]["count"]) for sql in physical_checks)
            inventory_rows = main.query("SELECT COUNT(*) AS count FROM room_inventory")[0]["count"]
            reservation_rows = main.query("SELECT COUNT(*) AS count FROM reservations")[0]["count"]
            payment_rows = main.query("SELECT COUNT(*) AS count FROM payments")[0]["count"]
            occupancy_rows = main.query("SELECT COUNT(*) AS count FROM room_occupancy")[0]["count"]
            return {
                "backend": backend,
                "valid": int(invalid) == 0,
                "inventory_rows": int(inventory_rows),
                "reservation_rows": int(reservation_rows),
                "payment_rows": int(payment_rows),
                "message": "ok" if int(invalid) == 0 else f"{invalid} invalid physical occupancy rows",
            }
        finally:
            ctx.close()

    db = connect_sql_database(database_url)
    try:
        invalid = sum(int(db.execute(sql).fetchone()["count"]) for sql in physical_checks)
        inventory_rows = db.execute("SELECT COUNT(*) AS count FROM room_inventory").fetchone()["count"]
        reservation_rows = db.execute("SELECT COUNT(*) AS count FROM reservations").fetchone()["count"]
        payment_rows = db.execute("SELECT COUNT(*) AS count FROM payments").fetchone()["count"]
        occupancy_rows = db.execute("SELECT COUNT(*) AS count FROM room_occupancy").fetchone()["count"]
        return {
            "backend": backend,
            "valid": int(invalid) == 0,
            "inventory_rows": int(inventory_rows),
            "reservation_rows": int(reservation_rows),
            "payment_rows": int(payment_rows),
            "message": "ok" if int(invalid) == 0 else f"{invalid} invalid physical occupancy rows",
        }
    finally:
        db.close()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True, default=str))
            f.write("\n")


def write_text_log(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + ("\n" if lines else ""))


def run_backend(
    backend: str,
    *,
    database_url: str,
    requests: list[HotelRequest],
    scripted_agent: bool,
    model: str,
    max_agent_steps: int,
    max_retries: int,
    parallel_agents: int,
    hotel_count: int,
    rooms_per_hotel: int,
    date_count: int,
) -> tuple[list[AttemptMetrics], dict[str, Any]]:
    setup_hotel_database(
        backend,
        database_url,
        hotel_count=hotel_count,
        rooms_per_hotel=rooms_per_hotel,
        date_count=date_count,
    )
    metrics = run_parallel_metrics(
        backend,
        database_url,
        requests,
        scripted_agent=scripted_agent,
        model=model,
        max_agent_steps=max_agent_steps,
        max_retries=max_retries,
        parallel_agents=parallel_agents,
    )
    return metrics, verify_database(database_url, backend)


def resolve_output_dir(value: str | None) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return (Path(".benchmarks") / f"hotel-booking-agent-{stamp}").resolve()


def main() -> None:
    parser = argparse.ArgumentParser(description="Hotel booking agent transaction benchmark.")
    parser.add_argument("--postgres-url", default=os.environ.get("CHRONOS_BRANCH_POSTGRES_DSN"))
    parser.add_argument("--backends", type=parse_backends, default=list(DEFAULT_BACKENDS))
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--requests", type=int, default=8)
    parser.add_argument("--parallel-agents", type=int, default=4)
    parser.add_argument("--hotel-count", type=int, default=4)
    parser.add_argument("--rooms-per-hotel", type=int, default=20)
    parser.add_argument("--date-count", type=int, default=7)
    parser.add_argument("--max-agent-steps", type=int, default=30)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument(
        "--conflict-mix",
        type=parse_csv,
        default=["disjoint", "arithmetic", "capacity", "policy"],
    )
    parser.add_argument("--scripted-agent", action="store_true")
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--output-dir", default=os.environ.get("CHRONOS_BENCH_OUTPUT_DIR"))
    args = parser.parse_args()
    if args.quick:
        args.requests = 8
        args.parallel_agents = 4
        args.hotel_count = 2
        args.rooms_per_hotel = 8
        args.date_count = 3
        args.scripted_agent = True
    if not args.postgres_url:
        raise SystemExit("--postgres-url or CHRONOS_BRANCH_POSTGRES_DSN is required")
    requests = generate_requests(args.requests, args.conflict_mix)
    output_dir = resolve_output_dir(args.output_dir)
    detail_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    verification_rows: list[dict[str, Any]] = []
    trace_rows: list[dict[str, Any]] = []
    run_log_lines: list[str] = [
        f"output_dir={output_dir}",
        f"model={args.model}",
        f"backends={','.join(args.backends)}",
        f"requests={args.requests}",
        f"parallel_agents={args.parallel_agents}",
        f"max_agent_steps={args.max_agent_steps}",
        f"max_retries={args.max_retries}",
    ]
    for backend in args.backends:
        progress_line = (
            f"progress backend={backend} requests={args.requests} "
            f"parallel_agents={args.parallel_agents} model={args.model}"
        )
        print(f"  {progress_line}", flush=True)
        run_log_lines.append(progress_line)
        metrics, verification = run_backend(
            backend,
            database_url=args.postgres_url,
            requests=requests,
            scripted_agent=args.scripted_agent,
            model=args.model,
            max_agent_steps=args.max_agent_steps,
            max_retries=args.max_retries,
            parallel_agents=args.parallel_agents,
            hotel_count=args.hotel_count,
            rooms_per_hotel=args.rooms_per_hotel,
            date_count=args.date_count,
        )
        detail_rows.extend(row.as_row() for row in metrics)
        trace_rows.extend(row.as_trace_row() for row in metrics)
        summary_rows.append(summarize_metrics(metrics, args.parallel_agents))
        verification_rows.append(verification)
        for row in metrics:
            run_log_lines.append(
                "agent_run "
                f"backend={row.backend} "
                f"request_id={row.request_id} "
                f"transaction_type={row.transaction_type} "
                f"success={row.success} "
                f"workflow_attempts={row.workflow_attempts} "
                f"workflow_aborts={row.workflow_aborts} "
                f"total_tokens={row.total_tokens} "
                f"abort_reason={row.abort_reason or '-'}"
            )
    write_csv(output_dir / "hotel_booking_details.csv", detail_rows, DETAIL_FIELDS)
    write_csv(output_dir / "hotel_booking_summary.csv", summary_rows, SUMMARY_FIELDS)
    write_csv(output_dir / "hotel_booking_verification.csv", verification_rows, VERIFICATION_FIELDS)
    write_jsonl(output_dir / "hotel_booking_traces.jsonl", trace_rows)
    write_text_log(output_dir / "hotel_booking_run.log", run_log_lines)
    print(f"Wrote hotel booking benchmark results to {output_dir}")


if __name__ == "__main__":
    main()
