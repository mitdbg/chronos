#!/usr/bin/env python3
"""Capture one Codex trace per incident input for the swarm experiment.

The normal sixteen-workflow capture path only accepts numbered prompt files.
This driver keeps the same rollout normalization and provenance format while
rendering one reusable incident prompt for each input record.  Captures use
the configured Codex model; the default is Luna with maximum reasoning effort.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from chronos_enterprise_knowledge.rollout_trace import WorkloadTrace

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import capture_codex_workflows as capture  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prompt-template", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--runs-dir", type=Path, required=True)
    parser.add_argument("--traces-dir", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--session-root", type=Path, default=Path.home() / ".codex" / "sessions")
    parser.add_argument("--codex", default="codex")
    parser.add_argument(
        "--codex-config",
        action="append",
        default=[
            'model="gpt-5.6-luna"',
            'model_reasoning_effort="max"',
        ],
        help="Repeatable Codex -c override. Luna/max is the default.",
    )
    parser.add_argument("--qdrant-url")
    parser.add_argument("--qdrant-storage-dir", type=Path)
    parser.add_argument(
        "--chronos-postgres-dsn",
        default=capture.default_chronos_postgres_dsn(),
        help="PostgreSQL DSN for Chronos capture state (SQLite is disabled).",
    )
    parser.add_argument("--dimensions", type=int, required=True)
    parser.add_argument("--snapshot-manifest", action="append", type=Path, required=True)
    parser.add_argument("--input-id", action="append", help="Capture only this input; repeatable.")
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args(argv)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_inputs(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, Mapping):
            raise ValueError(f"input line {line_number} is not an object")
        required = {"incident_id", "incident_title", "incident_query", "source_hints"}
        missing = sorted(required - set(value))
        if missing:
            raise ValueError(f"input line {line_number} lacks {missing}")
        rows.append({key: str(value[key]) for key in required})
    if not rows:
        raise ValueError(f"no incident inputs found in {path}")
    ids = [row["incident_id"] for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError("incident_id values must be unique")
    return rows


def _capture_corpus(manifest_paths: Sequence[Path], dimensions: int) -> dict[str, Any]:
    manifests = [json.loads(path.read_text(encoding="utf-8")) for path in manifest_paths]
    observed_dimensions = {int(manifest["spec"]["dimensions"]) for manifest in manifests}
    if observed_dimensions != {dimensions}:
        raise ValueError(
            "snapshot dimensions do not match --dimensions: "
            f"{sorted(observed_dimensions)} != {[dimensions]}"
        )
    digest_payload = [
        {"manifest": str(path.resolve()), "selection_digest": manifest["selection_digest"]}
        for path, manifest in zip(manifest_paths, manifests, strict=True)
    ]
    selection_digest = hashlib.sha256(
        json.dumps(digest_payload, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "snapshot_manifests": digest_payload,
        "selection_digest": selection_digest,
        "documents": sum(int(manifest["documents"]) for manifest in manifests),
        "chunks": sum(int(manifest["chunks"]) for manifest in manifests),
        "bytes": sum(int(manifest["bytes"]) for manifest in manifests),
        "embedding_dimensions": dimensions,
        "embedding_models": sorted(
            {str(manifest["spec"]["embedding_model"]) for manifest in manifests}
        ),
        "embedding_mode": "forced-zero",
    }


def _render_prompt(template: str, row: Mapping[str, str]) -> tuple[str, dict[str, str]]:
    incident_id = row["incident_id"]
    values = {
        **row,
        "task_branch": f"task/swarm-{incident_id}",
        "artifact_path": f"/artifacts/incidents/swarm-{incident_id}.md",
        "document_id": f"incident-report/swarm-{incident_id}",
        "memory_id": f"episodic/swarm-{incident_id}",
    }
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace("{{" + key + "}}", value)
    unresolved = [token for token in ("incident_id", "incident_title", "incident_query", "source_hints", "task_branch", "artifact_path", "document_id", "memory_id") if "{{" + token + "}}" in rendered]
    if unresolved:
        raise ValueError(f"unresolved prompt placeholders: {unresolved}")
    return rendered, values


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    # capture_one() launches Codex, whose MCP child inherits this setting.
    os.environ["CHRONOS_POSTGRES_DSN"] = args.chronos_postgres_dsn
    if not args.qdrant_url:
        raise SystemExit(
            "incident-response trace capture requires the shared Docker "
            "Qdrant service; pass --qdrant-url"
        )
    prompt_template = args.prompt_template.expanduser().resolve()
    inputs_path = args.inputs.expanduser().resolve()
    if not prompt_template.is_file():
        raise SystemExit(f"prompt template does not exist: {prompt_template}")
    rows = _load_inputs(inputs_path)
    selected = set(args.input_id or ())
    if selected:
        unknown = sorted(selected - {row["incident_id"] for row in rows})
        if unknown:
            raise SystemExit(f"unknown incident inputs: {unknown}")
        rows = [row for row in rows if row["incident_id"] in selected]

    args.runs_dir = args.runs_dir.expanduser().resolve()
    args.traces_dir = args.traces_dir.expanduser().resolve()
    args.workdir = args.workdir.expanduser().resolve()
    args.state_dir = args.state_dir.expanduser().resolve()
    args.session_root = args.session_root.expanduser().resolve()
    args.runs_dir.mkdir(parents=True, exist_ok=True)
    args.traces_dir.mkdir(parents=True, exist_ok=True)
    rendered_dir = args.runs_dir / "rendered-prompts"
    rendered_dir.mkdir(parents=True, exist_ok=True)
    template = prompt_template.read_text(encoding="utf-8")
    manifest_paths = [path.expanduser().resolve() for path in args.snapshot_manifest]
    capture_corpus = _capture_corpus(manifest_paths, args.dimensions)

    # capture_one() is the established Codex invocation and rollout normalizer.
    # Its numbered-workflow map is extended only for this process; the normal
    # capture command retains its existing behavior.
    provenance: list[dict[str, Any]] = []
    for row in rows:
        stem = f"incident-response-swarm-{row['incident_id']}"
        rendered, values = _render_prompt(template, row)
        prompt_path = rendered_dir / f"{stem}.md"
        prompt_path.write_text(rendered, encoding="utf-8")
        capture._TRACE_NAMES[stem] = f"{stem}.jsonl"
        capture_args = SimpleNamespace(**vars(args))
        capture_args.resume = bool(args.resume)
        reused = (
            capture._load_completed_capture(prompt_path, args=capture_args)
            if args.resume
            else None
        )
        if reused is not None:
            provenance.append(reused)
            continue
        result = capture.capture_one(
            prompt_path,
            args=capture_args,
            capture_corpus=capture_corpus,
        )
        trace_path = Path(str(result["trace"]))
        trace = WorkloadTrace.load(trace_path)
        metadata = dict(trace.metadata)
        metadata.update(
            {
                "experiment": "incident-response-swarm-v1",
                "input_id": row["incident_id"],
                "template_values": values,
                "shared_target_branch": "team/site-reliability",
            }
        )
        WorkloadTrace(trace.trace_id, trace.events, metadata).write(trace_path)
        result["trace_sha256"] = _sha256(trace_path)
        provenance_path = args.runs_dir / f"{stem}.capture.json"
        provenance_path.write_text(
            json.dumps(result, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        provenance.append(result)

    (args.runs_dir / "capture-manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "experiment": "incident-response-swarm-v1",
                "prompt_template": str(prompt_template),
                "prompt_template_sha256": _sha256(prompt_template),
                "inputs": str(inputs_path),
                "inputs_sha256": _sha256(inputs_path),
                "capture_corpus": capture_corpus,
                "codex_config": list(args.codex_config),
                "workflows": provenance,
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"captured": len(provenance), "runs_dir": str(args.runs_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
