"""Content-addressed JSONL traces and deterministic backend replay."""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from chronos_enterprise_knowledge.backend import (
    BackendOperation,
    OperationExecutor,
    OperationKind,
)
from chronos_enterprise_knowledge.models import canonical_json

_TRACE_SCHEMA_VERSION = 1
_BLOB_THRESHOLD = 4096


class TraceFormatError(RuntimeError):
    """Raised when a trace or one of its blobs is invalid."""


class BlobStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, data: bytes, *, encoding: str) -> dict[str, Any]:
        digest = hashlib.sha256(data).hexdigest()
        path = self.root / digest[:2] / digest[2:]
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return {
            "$blob": digest,
            "encoding": encoding,
            "bytes": len(data),
        }

    def get(self, reference: Mapping[str, Any]) -> Any:
        digest = str(reference["$blob"])
        path = self.root / digest[:2] / digest[2:]
        data = path.read_bytes()
        actual = hashlib.sha256(data).hexdigest()
        if actual != digest:
            raise TraceFormatError(
                f"trace blob checksum mismatch: expected {digest}, got {actual}"
            )
        encoding = reference.get("encoding")
        if encoding == "bytes":
            return data
        if encoding == "utf-8":
            return data.decode("utf-8")
        if encoding == "json":
            return json.loads(data)
        raise TraceFormatError(f"unsupported trace blob encoding: {encoding}")

    def externalize(self, value: Any) -> Any:
        if isinstance(value, bytes):
            return self.put(value, encoding="bytes")
        if isinstance(value, bytearray):
            return self.put(bytes(value), encoding="bytes")
        if isinstance(value, str):
            data = value.encode("utf-8")
            return (
                self.put(data, encoding="utf-8")
                if len(data) >= _BLOB_THRESHOLD
                else value
            )
        if isinstance(value, Mapping):
            materialized = {
                str(key): self.externalize(item) for key, item in value.items()
            }
            encoded = canonical_json(materialized).encode("utf-8")
            return (
                self.put(encoded, encoding="json")
                if len(encoded) >= _BLOB_THRESHOLD
                else materialized
            )
        if isinstance(value, (list, tuple)):
            materialized = [self.externalize(item) for item in value]
            encoded = canonical_json(materialized).encode("utf-8")
            return (
                self.put(encoded, encoding="json")
                if len(encoded) >= _BLOB_THRESHOLD
                else materialized
            )
        return value

    def materialize(self, value: Any) -> Any:
        if isinstance(value, Mapping) and "$blob" in value:
            return self.materialize(self.get(value))
        if isinstance(value, Mapping):
            return {str(key): self.materialize(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.materialize(item) for item in value]
        return value


@dataclass(frozen=True)
class ReplayMismatch:
    operation_id: str
    expected_digest: str
    actual_digest: str


@dataclass(frozen=True)
class ReplayTiming:
    operation_id: str
    kind: OperationKind
    elapsed_ns: int


@dataclass(frozen=True)
class ReplayReport:
    backend: str
    operations: int
    mutations: int
    reads: int
    mismatches: tuple[ReplayMismatch, ...]
    timings: tuple[ReplayTiming, ...]

    @property
    def matched(self) -> bool:
        return not self.mismatches

    def as_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "operations": self.operations,
            "mutations": self.mutations,
            "reads": self.reads,
            "matched": self.matched,
            "latency_by_operation": self.latency_summary(),
            "mismatches": [
                {
                    "operation_id": mismatch.operation_id,
                    "expected_digest": mismatch.expected_digest,
                    "actual_digest": mismatch.actual_digest,
                }
                for mismatch in self.mismatches
            ],
        }

    def latency_summary(self) -> dict[str, dict[str, float | int]]:
        grouped: dict[str, list[int]] = {}
        for timing in self.timings:
            grouped.setdefault(timing.kind, []).append(timing.elapsed_ns)
        result: dict[str, dict[str, float | int]] = {}
        for kind, values in sorted(grouped.items()):
            ordered = sorted(values)
            result[kind] = {
                "count": len(ordered),
                "p50_ms": _percentile(ordered, 0.50) / 1_000_000,
                "p95_ms": _percentile(ordered, 0.95) / 1_000_000,
                "max_ms": ordered[-1] / 1_000_000,
            }
        return result


class TraceRecorder:
    """Execute operations while writing replayable JSONL events."""

    def __init__(
        self,
        executor: OperationExecutor,
        trace_dir: str | Path,
        *,
        trace_id: str,
    ):
        self.executor = executor
        self.trace_dir = Path(trace_dir)
        self.trace_dir.mkdir(parents=True, exist_ok=True)
        self.trace_path = self.trace_dir / "events.jsonl"
        self.blobs = BlobStore(self.trace_dir / "blobs")
        self.trace_id = trace_id
        self._sequence = self._existing_event_count()

    def _existing_event_count(self) -> int:
        if not self.trace_path.exists():
            return 0
        with self.trace_path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())

    def execute(
        self,
        kind: OperationKind,
        *,
        branch_id: str | None = None,
        arguments: Mapping[str, Any] | None = None,
        operation_id: str | None = None,
    ) -> Any:
        sequence = self._sequence
        operation = BackendOperation(
            operation_id=operation_id or f"{self.trace_id}:{sequence:08d}",
            kind=kind,
            branch_id=branch_id,
            arguments=dict(arguments or {}),
        )
        started = time.perf_counter_ns()
        recorded_at = datetime.now(timezone.utc).isoformat()
        try:
            result = self.executor.execute(operation)
        except Exception as exc:
            elapsed = time.perf_counter_ns() - started
            self._append(
                operation,
                status="error",
                result=None,
                error={
                    "type": type(exc).__name__,
                    "message": str(exc),
                },
                recorded_at=recorded_at,
                elapsed_ns=elapsed,
            )
            self._sequence += 1
            raise
        elapsed = time.perf_counter_ns() - started
        self._append(
            operation,
            status="ok",
            result=result,
            error=None,
            recorded_at=recorded_at,
            elapsed_ns=elapsed,
        )
        self._sequence += 1
        return result

    def _append(
        self,
        operation: BackendOperation,
        *,
        status: str,
        result: Any,
        error: dict[str, str] | None,
        recorded_at: str,
        elapsed_ns: int,
    ) -> None:
        result_digest = (
            hashlib.sha256(canonical_json(result).encode("utf-8")).hexdigest()
            if status == "ok"
            else None
        )
        event = {
            "schema_version": _TRACE_SCHEMA_VERSION,
            "sequence": self._sequence,
            "trace_id": self.trace_id,
            "backend": self.executor.backend.backend_name,
            "recorded_at": recorded_at,
            "elapsed_ns": int(elapsed_ns),
            "status": status,
            "operation": self.blobs.externalize(operation.as_dict()),
            "result": self.blobs.externalize(result),
            "result_digest": result_digest,
            "error": error,
        }
        with self.trace_path.open("a", encoding="utf-8") as handle:
            handle.write(canonical_json(event))
            handle.write("\n")


class TraceReplayer:
    def __init__(self, executor: OperationExecutor, trace_dir: str | Path):
        self.executor = executor
        self.trace_dir = Path(trace_dir)
        self.trace_path = self.trace_dir / "events.jsonl"
        self.blobs = BlobStore(self.trace_dir / "blobs")

    def events(self) -> Iterator[dict[str, Any]]:
        with self.trace_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                event = json.loads(line)
                if event.get("schema_version") != _TRACE_SCHEMA_VERSION:
                    raise TraceFormatError(
                        f"unsupported trace schema on line {line_number}"
                    )
                yield event

    def replay(
        self,
        *,
        include_reads: bool = True,
        verify_results: bool = True,
    ) -> ReplayReport:
        operations = 0
        mutations = 0
        reads = 0
        mismatches: list[ReplayMismatch] = []
        timings: list[ReplayTiming] = []
        read_kinds = {"search", "branch_diff", "state_digest"}
        for event in self.events():
            if event.get("status") != "ok":
                continue
            operation_data = self.blobs.materialize(event["operation"])
            operation = BackendOperation.from_dict(operation_data)
            is_read = operation.kind in read_kinds
            if is_read and not include_reads:
                continue
            started = time.perf_counter_ns()
            actual = self.executor.execute(operation)
            elapsed = time.perf_counter_ns() - started
            timings.append(
                ReplayTiming(
                    operation.operation_id,
                    operation.kind,
                    elapsed,
                )
            )
            operations += 1
            if is_read:
                reads += 1
            else:
                mutations += 1
            if verify_results:
                actual_digest = hashlib.sha256(
                    canonical_json(actual).encode("utf-8")
                ).hexdigest()
                expected_digest = str(event["result_digest"])
                if actual_digest != expected_digest:
                    mismatches.append(
                        ReplayMismatch(
                            operation.operation_id,
                            expected_digest,
                            actual_digest,
                        )
                    )
        return ReplayReport(
            backend=self.executor.backend.backend_name,
            operations=operations,
            mutations=mutations,
            reads=reads,
            mismatches=tuple(mismatches),
            timings=tuple(timings),
        )


def _percentile(values: list[int], quantile: float) -> int:
    if not values:
        return 0
    index = max(0, math.ceil(quantile * len(values)) - 1)
    return values[index]


__all__ = [
    "BlobStore",
    "ReplayMismatch",
    "ReplayReport",
    "ReplayTiming",
    "TraceFormatError",
    "TraceRecorder",
    "TraceReplayer",
]
