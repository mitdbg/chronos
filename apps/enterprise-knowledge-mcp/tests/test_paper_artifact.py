"""Offline release checks: no model, service, or large dataset required."""
from __future__ import annotations

import argparse
import csv
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

APP = Path(__file__).resolve().parents[1]


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, APP / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


exporter = load("paper_export", "scripts/prepare_incident_paper_artifact.py")
analysis = load("paper_analysis", "scripts/reproduce_incident_paper_artifact.py")
archive = load("paper_archive", "datasets/enterprise_rag_infra_v1/artifact.py")


class ReleaseTests(unittest.TestCase):
    def test_unchanged_trace_is_byte_identical(self):
        data = b'{ "tool": "knowledge_search", "query": "incident response" }\n'
        published, counts = exporter.redact(data, ".jsonl")
        self.assertEqual(data, published)
        self.assertFalse(counts)

    def test_nested_credentials_and_encrypted_content_removed(self):
        value = {"payload": {"encrypted_content": "provider-private", "access_token": "private-token"},
                 "url": "postgresql://user:private-password@host/db", "tokens_used": 123}
        published, counts = exporter.redact(json.dumps(value).encode(), ".json")
        self.assertNotIn(b"provider-private", published)
        self.assertNotIn(b"private-token", published)
        self.assertNotIn(b"private-password", published)
        self.assertEqual(json.loads(published)["tokens_used"], 123)
        self.assertTrue(counts)

    def test_key_in_log_removed(self):
        secret = "sk-proj-" + "x" * 40
        published, counts = exporter.redact(f"key={secret}".encode(), ".log")
        self.assertNotIn(secret.encode(), published)
        self.assertEqual(counts["provider_key"], 1)

    def test_shards_have_correct_boundaries_and_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            parts = archive.split_stream(io.BytesIO(b"abcdefghijkl"), root, "test", 5)
            self.assertEqual([row["bytes"] for row in parts], [5, 5, 2])
            archive.verify_parts(root, {"parts": parts})
            (root / parts[0]["path"]).write_bytes(b"wrong")
            with self.assertRaises(ValueError):
                archive.verify_parts(root, {"parts": parts})

    def test_archive_path_traversal_rejected(self):
        with self.assertRaises(ValueError):
            archive.verify_parts(Path("."), {"parts": [{"path": "../secret"}]})

    def test_release_manifest_tampering_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = b"data"
            (root / "file").write_bytes(payload)
            (root / "release-manifest.json").write_text(json.dumps({"files": [
                {"path": "file", "bytes": 4, "sha256": exporter.digest(payload)}]}))
            self.assertEqual(analysis.verify(root), 1)
            (root / "file").write_bytes(b"oops")
            with self.assertRaises(ValueError):
                analysis.verify(root)

    def test_partial_trial_uses_requested_not_finished_workers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trial = root / "replay/native-branching-lock/workers-128"
            trial.mkdir(parents=True)
            (trial / "config.json").write_text('{"workers":128}')
            row = {"system": "native-branching-lock", "output": "/old/path/workers-128",
                   "workers": 121, "successful_workers": 121, "status": "partial",
                   "elapsed_seconds": 29506, "verifier_violations": "", "incomplete_bundles": ""}
            with (root / "replay/sweep-results.csv").open("w") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(row))
                writer.writeheader()
                writer.writerow(row)
            result = analysis.rows_from_results(root)[0]
            self.assertEqual(result["requested_workers"], 128)
            self.assertEqual(result["completion_percent"], 121 / 128 * 100)
            self.assertIsNone(result["verifier_violations"])
            self.assertFalse(result["has_final_result"])

    def test_replay_wrapper_forwards_release_and_service_paths(self):
        env = dict(os.environ, PYTHON="/bin/echo", EXPERIMENT_ROOT="/tmp/paper-replay-test",
                   CHRONOS_POSTGRES_DSN="postgresql://test/chronos", DOLTGRES_DSN="postgresql://test/native",
                   BTRFS_ROOT="/tmp/test-btrfs", CHRONOS_QDRANT_URL="http://chronos-qdrant:6333",
                   NATIVE_QDRANT_URL="http://native-qdrant:6333", CHRONOS_QDRANT_STORAGE_DIR="/tmp/cq",
                   NATIVE_QDRANT_STORAGE_DIR="/tmp/nq", CHRONOS_POSTGRES_DATA_DIR="/tmp/pg",
                   DOLTGRES_DATA_DIR="/tmp/dolt", SNAPSHOT_DIR="/tmp/test-snapshot")
        script = APP / "scripts/replay_incident_paper_artifact.sh"
        output = subprocess.check_output(["bash", str(script), "run", "--workers", "1"], env=env, text=True)
        self.assertIn("--merge-retries unlimited", output)
        self.assertIn("--traces-dir " + str(APP / ".results/incident-response-swarm-v1/traces"), output)
        self.assertIn("--chronos-state-dir /tmp/paper-replay-test/states/chronos", output)
        self.assertTrue(output.strip().endswith("--workers 1"))
        output = subprocess.check_output(["bash", str(script), "prepare", "native"], env=env, text=True)
        self.assertIn("--backend doltgres-qdrant-btrfs", output)
        self.assertIn("ingest-snapshot /tmp/test-snapshot", output)
        self.assertIn("init-hierarchy --snapshot /tmp/test-snapshot", output)

    @unittest.skipUnless(shutil.which("tar") and shutil.which("zstd"), "Requires tar and zstd")
    def test_archive_restore_roundtrip_and_exclusions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            corpus = root / "corpus"
            corpus.mkdir()
            (corpus / "document.txt").write_text("company knowledge")
            for excluded in ["codebases/vllm/.git", "codebases/langfuse/ee",
                             "codebases/langfuse/web/src/ee", "codebases/langfuse/worker/src/ee"]:
                path = corpus / excluded
                path.mkdir(parents=True)
                (path / "private").write_text("must not ship")
            snapshot = root / "snapshot"
            snapshot.mkdir()
            (snapshot / "snapshot.sqlite").write_bytes(b"placeholder fixture database")
            (snapshot / "manifest.json").write_text('{"complete":true}')
            args = argparse.Namespace(artifacts=root / "artifacts", corpus=corpus,
                                      snapshot=snapshot, command="restore", dataset="all", output=root / "restored")
            archive.pack(args)
            manifest = json.loads((args.artifacts / "manifest.json").read_text())
            for name, record in manifest["archives"].items():
                archive.verify_stream(args.artifacts, name, record, manifest)
            archive.unpack(args)
            restored = root / "restored/generated_data_infra_v1"
            self.assertEqual((restored / "document.txt").read_text(), "company knowledge")
            self.assertFalse(list(restored.rglob("private")))
            self.assertEqual((root / "restored/document-snapshot/snapshot.sqlite").read_bytes(),
                             (snapshot / "snapshot.sqlite").read_bytes())
            with self.assertRaises(SystemExit):
                archive.unpack(args)


if __name__ == "__main__":
    unittest.main()
