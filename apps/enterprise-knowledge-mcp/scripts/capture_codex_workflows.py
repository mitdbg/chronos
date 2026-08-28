#!/usr/bin/env python3
"""Capture real Codex sessions and normalize them into replayable traces.

The script deliberately invokes Codex for each prompt instead of translating
prompts into storage operations.  Each output trace is therefore backed by a
persisted Codex rollout, raw Codex event stream, final response, and rollout
hash.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from chronos_enterprise_knowledge.backends import (
    create_knowledge_backend,
    default_chronos_postgres_dsn,
)
from chronos_enterprise_knowledge.rollout_trace import (
    RolloutAnalyzer,
    WorkloadTrace,
    resolve_memory_timestamps,
)

_TRACE_NAMES = {
    "01-create-department": "01-create-department-and-team.jsonl",
    "02-add-team-member": "02-add-team-member.jsonl",
    "03-debug-streaming-handshake": "03-debug-streaming-handshake.jsonl",
    "04-debug-predictive-headroom": "04-debug-predictive-headroom.jsonl",
    "05-debug-throttling-race": "05-debug-throttling-race.jsonl",
    "06-enterprise-rag-qa": "06-enterprise-rag-qa.jsonl",
    "07-product-launch-readiness": "07-product-launch-readiness.jsonl",
    "08-model-profile-branching-and-consolidation": (
        "08-model-profile-branching-and-consolidation.jsonl"
    ),
    "09-private-deployment-assurance": "09-private-deployment-assurance.jsonl",
    "10-employee-access-onboarding": "10-employee-access-onboarding.jsonl",
    "01-vllm-batch-api-contract": "01-vllm-batch-api-contract.jsonl",
    "02-vllm-flashinfer-virtual-buffers": (
        "02-vllm-flashinfer-virtual-buffers.jsonl"
    ),
    "03-litellm-retry-after": "03-litellm-retry-after.jsonl",
    "04-langfuse-rest-trace-metadata": (
        "04-langfuse-rest-trace-metadata.jsonl"
    ),
    "05-litellm-latency-routing-race": (
        "05-litellm-latency-routing-race.jsonl"
    ),
    "06-vllm-kv-cache-dtype": "06-vllm-kv-cache-dtype.jsonl",
    "07-langfuse-score-analytics": "07-langfuse-score-analytics.jsonl",
    "08-vllm-context-aware-speculation": (
        "08-vllm-context-aware-speculation.jsonl"
    ),
    "09-enterprise-rag-qa-memory": "09-enterprise-rag-qa-memory.jsonl",
    "10-platform-engineer-onboarding": (
        "10-platform-engineer-onboarding.jsonl"
    ),
    "11-form-runtime-diagnostics-team": (
        "11-form-runtime-diagnostics-team.jsonl"
    ),
    "12-parallel-incident-investigations": (
        "12-parallel-incident-investigations.jsonl"
    ),
    "13-launch-readiness-revision-cycle": (
        "13-launch-readiness-revision-cycle.jsonl"
    ),
    "14-selective-residency-runbook-promotion": (
        "14-selective-residency-runbook-promotion.jsonl"
    ),
    "15-competing-rollback-playbooks": (
        "15-competing-rollback-playbooks.jsonl"
    ),
    "16-retention-memory-supersession": (
        "16-retention-memory-supersession.jsonl"
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prompts-dir", type=Path, required=True)
    parser.add_argument("--runs-dir", type=Path, required=True)
    parser.add_argument("--traces-dir", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument(
        "--session-root",
        type=Path,
        default=Path.home() / ".codex" / "sessions",
    )
    parser.add_argument("--codex", default="codex")
    parser.add_argument(
        "--codex-config",
        action="append",
        default=[],
        help="Repeatable Codex -c override used for every captured session.",
    )
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--qdrant-url")
    parser.add_argument("--qdrant-storage-dir", type=Path)
    parser.add_argument(
        "--chronos-postgres-dsn",
        default=default_chronos_postgres_dsn(),
        help="PostgreSQL DSN for Chronos capture state (SQLite is disabled).",
    )
    parser.add_argument("--dimensions", type=int, required=True)
    parser.add_argument(
        "--snapshot-manifest",
        action="append",
        type=Path,
        required=True,
        help=(
            "Manifest for one snapshot present in the capture root; repeat "
            "for a layered company-document and source-code corpus."
        ),
    )
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=13)
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Reuse completed per-workflow captures and archive an interrupted "
            "workflow's partial files before retrying it."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    # The MCP child launched by Codex receives the same default through its
    # environment.  This keeps capture verification and the live session on
    # one PostgreSQL metadata plane even when the caller supplies a custom DSN.
    os.environ["CHRONOS_POSTGRES_DSN"] = args.chronos_postgres_dsn
    args.runs_dir.mkdir(parents=True, exist_ok=True)
    args.traces_dir.mkdir(parents=True, exist_ok=True)
    args.workdir.mkdir(parents=True, exist_ok=True)
    manifest_paths = [
        path.expanduser().resolve() for path in args.snapshot_manifest
    ]
    manifests = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in manifest_paths
    ]
    dimensions = {
        int(manifest["spec"]["dimensions"]) for manifest in manifests
    }
    if dimensions != {args.dimensions}:
        raise ValueError(
            "capture snapshot dimensions do not match --dimensions: "
            f"{sorted(dimensions)} != {args.dimensions}"
        )
    digest_payload = [
        {
            "manifest": str(path),
            "selection_digest": manifest["selection_digest"],
        }
        for path, manifest in zip(manifest_paths, manifests, strict=True)
    ]
    capture_corpus = {
        "snapshot_manifests": digest_payload,
        "snapshot_schema_versions": sorted(
            {int(manifest["schema_version"]) for manifest in manifests}
        ),
        "selection_digest": hashlib.sha256(
            json.dumps(
                digest_payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
        "documents": sum(int(manifest["documents"]) for manifest in manifests),
        "chunks": sum(int(manifest["chunks"]) for manifest in manifests),
        "bytes": sum(int(manifest["bytes"]) for manifest in manifests),
        "embedding_dimensions": args.dimensions,
        "embedding_models": sorted(
            {str(manifest["spec"]["embedding_model"]) for manifest in manifests}
        ),
        "embedding_mode": "forced-zero",
    }
    backend = _open_capture_backend(args)
    try:
        capture_corpus["ingested_state"] = backend.storage_stats()
    finally:
        backend.close()
    prompt_paths = [
        path
        for path in sorted(args.prompts_dir.glob("[0-9][0-9]-*.md"))
        if args.start <= int(path.name[:2]) <= args.end
        and path.stem in _TRACE_NAMES
    ]
    if not prompt_paths:
        raise SystemExit("no matching workflow prompts")

    provenance: list[dict[str, Any]] = []
    for prompt_path in prompt_paths:
        reused = (
            _load_completed_capture(prompt_path, args=args)
            if args.resume
            else None
        )
        if reused is not None:
            provenance.append(reused)
            print(
                json.dumps(
                    {
                        "workflow": prompt_path.stem,
                        "reused": True,
                        "trace": reused["trace"],
                    }
                ),
                flush=True,
            )
            continue
        provenance.append(
            capture_one(
                prompt_path,
                args=args,
                capture_corpus=capture_corpus,
            )
        )
    output = args.runs_dir / "capture-manifest.json"
    output.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "capture_corpus": capture_corpus,
                "workflows": provenance,
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"captured": len(provenance), "manifest": str(output)}))
    return 0


def capture_one(
    prompt_path: Path,
    *,
    args: argparse.Namespace,
    capture_corpus: dict[str, Any],
) -> dict[str, Any]:
    stem = prompt_path.stem
    trace_path = args.traces_dir / _TRACE_NAMES[stem]
    provenance_path = args.runs_dir / f"{stem}.capture.json"
    events_path = args.runs_dir / f"{stem}.events.jsonl"
    stderr_path = args.runs_dir / f"{stem}.stderr.log"
    final_path = args.runs_dir / f"{stem}.final.txt"
    capture_paths = (
        events_path,
        stderr_path,
        final_path,
        trace_path,
        provenance_path,
    )
    if args.resume and any(path.exists() for path in capture_paths):
        _archive_incomplete_capture(stem, capture_paths, args.runs_dir)
    for path in capture_paths:
        if path.exists():
            raise FileExistsError(
                f"refusing to overwrite an existing capture artifact: {path}"
            )
    command = [
        args.codex,
        "exec",
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        "--json",
        "--output-last-message",
        str(final_path),
        "-C",
        str(args.workdir.resolve()),
    ]
    for override in args.codex_config:
        command.extend(("-c", override))
    command.append("-")
    capture_started_ns = time.monotonic_ns()
    with (
        events_path.open("wb") as events,
        stderr_path.open("wb") as errors,
    ):
        completed = subprocess.run(
            command,
            input=prompt_path.read_bytes(),
            stdout=events,
            stderr=errors,
            check=False,
        )
    capture_wall_time_ms = round(
        (time.monotonic_ns() - capture_started_ns) / 1_000_000,
        3,
    )
    if not final_path.exists():
        _write_fallback_final(events_path, final_path, completed.returncode)

    thread_id = _thread_id(events_path)
    rollout = _find_rollout(args.session_root, thread_id)
    rollout_sha256 = _sha256(rollout)
    analysis = RolloutAnalyzer().analyze(rollout, trace_id=stem)
    backend = _open_capture_backend(args)
    try:
        trace, resolved = resolve_memory_timestamps(analysis.trace, backend)
    finally:
        backend.close()
    metadata = dict(trace.metadata)
    metadata.update(
        {
            "capture_corpus": capture_corpus,
            "codex_exit_code": completed.returncode,
            "capture_status": (
                "not_replayable"
                if not analysis.trace.metadata.get("fully_replayable", False)
                else ("ok" if completed.returncode == 0 else "codex_failed")
            ),
            "unsupported_calls": list(analysis.skipped_calls),
            "source_session_id": thread_id,
            "source_rollout_sha256": rollout_sha256,
            "memory_timestamps_resolved": resolved,
        }
    )
    trace = WorkloadTrace(trace.trace_id, trace.events, metadata)
    trace.write(trace_path)
    result = {
        "workflow": stem,
        "prompt": str(prompt_path.resolve()),
        "prompt_sha256": _sha256(prompt_path),
        "session_id": thread_id,
        "rollout": str(rollout),
        "rollout_sha256": rollout_sha256,
        "events": len(trace.events),
        "mcp_events": trace.summary()["mcp_events"],
        "shell_events": trace.summary()["shell_events"],
        "patch_events": trace.summary()["patch_events"],
        "memory_timestamps_resolved": resolved,
        "codex_exit_code": completed.returncode,
        "capture_status": (
            "not_replayable"
            if not analysis.trace.metadata.get("fully_replayable", False)
            else ("ok" if completed.returncode == 0 else "codex_failed")
        ),
        "unsupported_calls": list(analysis.skipped_calls),
        "llm_timing": trace.metadata["llm_timing"],
        "capture_wall_time_ms": capture_wall_time_ms,
        "trace": str(trace_path.resolve()),
        "trace_sha256": _sha256(trace_path),
        "events_output": str(events_path.resolve()),
        "stderr_output": str(stderr_path.resolve()),
        "final_output": str(final_path.resolve()),
    }
    provenance_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return result


def _write_fallback_final(
    events_path: Path,
    final_path: Path,
    exit_code: int,
) -> None:
    """Create a provenance-complete final output for a failed Codex turn."""

    messages: list[str] = []
    for line in events_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        item = row.get("item")
        if (
            row.get("type") == "item.completed"
            and isinstance(item, dict)
            and item.get("type") == "agent_message"
        ):
            messages.append(str(item.get("text") or ""))
    text = messages[-1] if messages else (
        f"Codex exited with status {exit_code}; see the persisted event stream.\n"
    )
    final_path.write_text(text + ("\n" if not text.endswith("\n") else ""), encoding="utf-8")


def _open_capture_backend(
    args: argparse.Namespace,
    *,
    timeout_seconds: float = 60.0,
):
    """Open capture state after Codex's MCP child releases local Qdrant."""

    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            return create_knowledge_backend(
                "chronos",
                args.state_dir.resolve(),
                vector_dimensions=args.dimensions,
                qdrant_url=args.qdrant_url,
                qdrant_storage_dir=(
                    args.qdrant_storage_dir.resolve()
                    if args.qdrant_storage_dir is not None
                    else None
                ),
                chronos_postgres_dsn=args.chronos_postgres_dsn,
            )
        except RuntimeError as exc:
            if (
                "already accessed by another instance of Qdrant client"
                not in str(exc)
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(0.25)


def _load_completed_capture(
    prompt_path: Path,
    *,
    args: argparse.Namespace,
) -> dict[str, Any] | None:
    provenance_path = args.runs_dir / f"{prompt_path.stem}.capture.json"
    if not provenance_path.is_file():
        return None
    result = json.loads(provenance_path.read_text(encoding="utf-8"))
    trace_path = Path(str(result["trace"]))
    required = (
        trace_path,
        Path(str(result["events_output"])),
        Path(str(result["stderr_output"])),
        Path(str(result["final_output"])),
    )
    valid = (
        result.get("prompt_sha256") == _sha256(prompt_path)
        and all(path.is_file() for path in required)
        and result.get("trace_sha256") == _sha256(trace_path)
    )
    if valid:
        return result
    _archive_incomplete_capture(
        prompt_path.stem,
        (*required, provenance_path),
        args.runs_dir,
    )
    return None


def _archive_incomplete_capture(
    stem: str,
    paths: tuple[Path, ...],
    runs_dir: Path,
) -> None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return
    archive = runs_dir / "incomplete" / f"{stem}-{time.time_ns()}"
    archive.mkdir(parents=True)
    for path in existing:
        path.rename(archive / path.name)


def _thread_id(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        value = json.loads(line)
        if value.get("type") == "thread.started" and value.get("thread_id"):
            return str(value["thread_id"])
    raise RuntimeError(f"Codex event stream has no thread ID: {path}")


def _find_rollout(session_root: Path, thread_id: str) -> Path:
    deadline = time.monotonic() + 30
    while True:
        matches = sorted(session_root.rglob(f"*{thread_id}.jsonl"))
        if len(matches) == 1:
            return matches[0].resolve()
        if len(matches) > 1:
            raise RuntimeError(
                f"multiple Codex rollouts found for session {thread_id}: {matches}"
            )
        if time.monotonic() >= deadline:
            raise FileNotFoundError(
                f"Codex rollout for session {thread_id} was not persisted"
            )
        time.sleep(0.5)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
