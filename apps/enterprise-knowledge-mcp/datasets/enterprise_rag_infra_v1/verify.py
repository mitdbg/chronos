#!/usr/bin/env python3
"""Compare a rebuilt indexed corpus with the artifact used by the paper."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Iterable


HERE = Path(__file__).resolve().parent
APP_ROOT = HERE.parents[1]
sys.path.insert(0, str(APP_ROOT / "src"))

from chronos_enterprise_knowledge.enterprise_rag import EnterpriseRAGCorpus  # noqa: E402
from chronos_enterprise_knowledge.snapshot import select_corpus_paths  # noqa: E402


def content_digest(root: Path, paths: Iterable[Path]) -> tuple[int, int, str]:
    digest = hashlib.sha256()
    count = 0
    byte_count = 0
    for path in sorted(paths, key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        count += 1
        byte_count += len(payload)
    return count, byte_count, digest.hexdigest()


def verify(corpus_root: Path) -> dict[str, object]:
    expected = json.loads((HERE / "paper_artifact.json").read_text(encoding="utf-8"))
    manifest = json.loads((corpus_root / "manifest.json").read_text(encoding="utf-8"))
    validation = json.loads(
        (corpus_root / "provenance" / "validation.json").read_text(encoding="utf-8")
    )
    selection = select_corpus_paths(
        EnterpriseRAGCorpus(corpus_root),
        fraction=1.0,
        seed="chronos-enterprise-infra-v1",
    )
    public_count, public_bytes, public_digest = content_digest(
        corpus_root, (corpus_root / "sources" / "github_public").rglob("*.json")
    )
    internal_count, internal_bytes, internal_digest = content_digest(
        corpus_root, (corpus_root / "sources").rglob("infra-v1-*.json")
    )
    expected_counts = expected["structured_document_counts"]
    observed_counts = manifest["artifact_counts"]["connector_documents"]
    mismatches: list[str] = []
    if observed_counts != {
        key: value for key, value in expected_counts.items() if key != "total"
    }:
        mismatches.append("structured_document_counts")
    if selection.selected_documents != expected["source_corpus"]["documents"]:
        mismatches.append("source_corpus.documents")
    if selection.digest != expected["source_corpus"]["path_selection_digest"]:
        mismatches.append("source_corpus.path_selection_digest")
    if public_digest != expected["indexed_content_digests"]["github_public"]:
        mismatches.append("indexed_content_digests.github_public")
    if internal_digest != expected["indexed_content_digests"]["internal_infra_v1"]:
        mismatches.append("indexed_content_digests.internal_infra_v1")
    if public_count != expected_counts["github_public"]:
        mismatches.append("github_public.count")
    if internal_count != expected["internal_documents_added"]:
        mismatches.append("internal_infra_v1.count")
    if not validation.get("valid"):
        mismatches.append("validation.valid")
    return {
        "matches_paper_indexed_corpus": not mismatches,
        "mismatches": mismatches,
        "observed": {
            "documents": selection.selected_documents,
            "path_selection_digest": selection.digest,
            "github_public": {
                "documents": public_count,
                "bytes": public_bytes,
                "digest": public_digest,
            },
            "internal_infra_v1": {
                "documents": internal_count,
                "bytes": internal_bytes,
                "digest": internal_digest,
            },
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("corpus", type=Path)
    arguments = parser.parse_args()
    result = verify(arguments.corpus.expanduser().resolve())
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["matches_paper_indexed_corpus"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
