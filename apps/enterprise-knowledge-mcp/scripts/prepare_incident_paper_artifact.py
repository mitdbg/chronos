#!/usr/bin/env python3
"""Export an allowlisted, credential-redacted copy of retained incident runs.

Never exports an entire Codex home or service data directories. Original files
are not changed. Original and published checksums are deliberately separate.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import subprocess

APP = Path(__file__).resolve().parents[1]
REPO = APP.parents[1]
SECRET_KEY = re.compile(r"^(?:authorization|proxy-authorization|cookie|set-cookie|"
                        r"api[_-]?key|access_token|refresh_token|id_token|"
                        r"encrypted_content|password|client_secret)$", re.I)
PATTERNS = [
    ("url_credentials", re.compile(r"([a-z][a-z0-9+.-]*://)[^\s/@]+:[^\s/@]+@", re.I), r"\1REDACTED:REDACTED@"),
    ("bearer", re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{12,}", re.I), "Bearer REDACTED"),
    ("provider_key", re.compile(r"\b(?:sk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{20,}|github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,})\b"), "REDACTED_API_KEY"),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"), "REDACTED_JWT"),
    ("private_key", re.compile(r"-----BEGIN (?:[A-Z]+ )?PRIVATE KEY-----.*?-----END (?:[A-Z]+ )?PRIVATE KEY-----", re.S), "REDACTED_PRIVATE_KEY"),
]


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def redact_text(text: str, counts: Counter) -> str:
    for name, pattern, replacement in PATTERNS:
        text, number = pattern.subn(replacement, text)
        counts[name] += number
    return text


def redact_value(value, counts: Counter):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if SECRET_KEY.fullmatch(key) and item:
                counts[f"field:{key.lower()}"] += 1
                result[key] = "[REDACTED]"
            else:
                result[key] = redact_value(item, counts)
        return result
    if isinstance(value, list):
        return [redact_value(item, counts) for item in value]
    return redact_text(value, counts) if isinstance(value, str) else value


def redact(data: bytes, suffix: str) -> tuple[bytes, dict]:
    counts = Counter()
    original = data.decode("utf-8")
    if suffix == ".jsonl":
        records = [redact_value(json.loads(line), counts) for line in original.splitlines() if line.strip()]
        updated = "\n".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in records) + "\n"
    elif suffix == ".json":
        updated = json.dumps(redact_value(json.loads(original), counts), indent=2, ensure_ascii=False) + "\n"
    else:
        updated = redact_text(original, counts)
    counts = {key: count for key, count in counts.items() if count}
    # Preserve original bytes, including formatting, whenever no redaction is necessary.
    return (updated.encode() if counts else data), counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=APP / ".results/incident-response-swarm-v1")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    inventory = []

    def export(source: Path, relative: Path, expected=None, immutable=False):
        original = source.read_bytes()
        if expected and digest(original) != expected:
            raise ValueError(f"Original capture checksum mismatch: {source.name}")
        if source.suffix in {".json", ".jsonl", ".log", ".txt", ".md", ".csv"}:
            published, redactions = redact(original, source.suffix)
        else:
            published, redactions = original, {}
        if immutable and published != original:
            raise ValueError(f"Replay trace contains credential material; review before release: {source.name}")
        target = args.output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.read_bytes() != published:
                raise ValueError(f"Refusing to replace a different release file: {target}")
        else:
            target.write_bytes(published)
        inventory.append({"path": relative.as_posix(), "bytes": len(published),
                          "original_sha256": digest(original), "sha256": digest(published),
                          "redactions": redactions})

    runs = args.source / "runs/luna-max-20260821"
    captures = sorted(runs.glob("*.capture.json"))
    if len(captures) != 8:
        raise ValueError("Expected eight completed capture records")
    for capture_path in captures:
        capture = json.loads(capture_path.read_text())
        if capture["capture_status"] != "ok":
            raise ValueError(f"Capture not complete: {capture_path.name}")
        # Resolve only the recorded basename inside the known source tree.
        raw = args.source / "codex-home/sessions/2026/08/21" / Path(capture["rollout"]).name
        identities = {"model": set(), "effort": set(), "cli_version": set()}
        for line in raw.read_text().splitlines():
            payload = json.loads(line).get("payload", {})
            if isinstance(payload, dict):
                for field in identities:
                    if payload.get(field):
                        identities[field].add(payload[field])
        if identities != {"model": {"gpt-5.6-luna"}, "effort": {"max"}, "cli_version": {"0.145.0"}}:
            raise ValueError(f"Unexpected model/CLI identity in {raw.name}; do not mislabel capture provenance")
        export(raw, Path("captures/raw") / raw.name, capture["rollout_sha256"])
        trace = args.source / "traces/luna-max-20260821" / Path(capture["trace"]).name
        export(trace, Path("traces") / trace.name, capture["trace_sha256"], immutable=True)
    for source in sorted(runs.rglob("*")):
        if source.is_file() and source.suffix in {".json", ".jsonl", ".md", ".txt", ".log"}:
            export(source, Path("captures/cli") / source.relative_to(runs))
    for source in sorted(args.results.rglob("*")):
        if source.is_file() and source.suffix in {".json", ".jsonl", ".log", ".txt", ".csv", ".pdf", ".png"}:
            export(source, Path("replay") / source.relative_to(args.results))
    inputs = APP / "workflows/curated-v2/concurrency-v1/incident-response-swarm/incident-inputs.jsonl"
    export(inputs, Path("incident-inputs.jsonl"), immutable=True)
    manifest = {
        "schema_version": 1, "experiment": "incident-response-swarm-v1",
        "export_code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        "export_code_commit_note": "Export checkout only; original trial code commits were not recorded.",
        "export_tools_sha256": {
            name: digest((APP / name).read_bytes()) for name in [
                "scripts/prepare_incident_paper_artifact.py",
                "scripts/reproduce_incident_paper_artifact.py",
                "scripts/replay_incident_paper_artifact.sh",
                "datasets/enterprise_rag_infra_v1/artifact.py",
            ]
        },
        "capture_date": "2026-08-21", "model": "gpt-5.6-luna", "reasoning_effort": "max",
        "codex_cli_version": "0.145.0", "immutable_model_build": None,
        "dataset": "../../datasets/enterprise_rag_infra_v1/artifacts/manifest.json",
        "redaction": "Credential fields, encrypted provider content, URL credentials, bearer tokens, API keys, JWTs and private keys. No auth/config homes included. Replay traces unchanged.",
        "redaction_limit": "Rule-based sanitization, not a guarantee that every sensitive value is recognized; review before public upload.",
        "files": inventory,
    }
    (args.output / "release-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"files": len(inventory), "redacted_files": sum(bool(row["redactions"]) for row in inventory),
                      "bytes": sum(row["bytes"] for row in inventory)}, indent=2))


if __name__ == "__main__":
    main()
