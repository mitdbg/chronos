from __future__ import annotations

import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

import build_infra_dataset
import prepare


class PrepareDatasetTests(unittest.TestCase):
    def test_crawl_preflight_rejects_invalid_credential(self) -> None:
        error = urllib.error.HTTPError(
            "https://api.github.com/rate_limit", 401, "Unauthorized", {}, None
        )
        with (
            mock.patch.dict("os.environ", {"GITHUB_TOKEN": "invalid"}, clear=True),
            mock.patch("prepare.urllib.request.urlopen", side_effect=error),
            self.assertRaisesRegex(RuntimeError, "HTTP 401"),
        ):
            prepare.validate_github_credential()

    def test_manifest_pins_match_builder(self) -> None:
        manifest = json.loads(
            (prepare.HERE / "seed" / "codebases" / "manifest.json").read_text(
                encoding="utf-8"
            )
        )
        pins = {
            repository["upstream"].removeprefix("https://github.com/"): repository[
                "commit"
            ]
            for repository in manifest["repositories"]
        }
        self.assertEqual(pins, build_infra_dataset.PINNED_COMMITS)

    def test_seed_files_install_exact_recipe_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            prepare.install_seed_files(base)
            installed = base / "codebases" / "manifest.json"
            self.assertTrue(installed.is_file())
            issue = base / "codebases" / "upstream-issues" / "litellm-24720.md"
            self.assertTrue(issue.read_bytes().endswith(b"\n\n"))
            installed.write_text("different\n", encoding="utf-8")
            prepare.install_seed_files(base)
            self.assertNotEqual(installed.read_text(encoding="utf-8"), "different\n")

    def test_paper_reproduction_is_the_default(self) -> None:
        arguments = prepare.parser().parse_args([])
        self.assertEqual(arguments.phase, "all")
        self.assertFalse(arguments.llm_generated_internal)

    def test_paper_artifact_counts_reconcile(self) -> None:
        artifact = json.loads(
            (prepare.HERE / "paper_artifact.json").read_text(encoding="utf-8")
        )
        structured = artifact["structured_document_counts"]
        self.assertEqual(
            structured["total"],
            sum(value for key, value in structured.items() if key != "total"),
        )
        self.assertEqual(
            artifact["source_corpus"]["documents"] - structured["total"],
            18_583,
        )
        self.assertEqual(
            artifact["ingested_main_branch"]["documents"],
            artifact["source_corpus"]["documents"] + 1,
        )


if __name__ == "__main__":
    unittest.main()
