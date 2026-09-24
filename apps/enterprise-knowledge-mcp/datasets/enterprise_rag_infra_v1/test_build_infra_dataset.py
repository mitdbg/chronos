from __future__ import annotations

import gzip
import json
import tempfile
import unittest
import urllib.error
from email.message import Message
from pathlib import Path

from build_infra_dataset import (
    CUTOFF,
    GitHubClient,
    _read_pages,
    _reconstruct_body_at_cutoff,
    _reverse_unified_diff,
    _state_at_cutoff,
    _write_page,
    crawl_graphql_metadata,
    crawl_stream,
    deterministic_uuid,
    normalize_repository,
    parse_link_header,
)


class _Response:
    def __init__(self, payload: bytes, status: int = 200, headers: dict[str, str] | None = None):
        self.status = status
        self.headers = headers or {}
        self.payload = payload

    def read(self) -> bytes:
        return self.payload

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _Opener:
    def __init__(self, responses: list[_Response]):
        self.responses = responses
        self.requests = []

    def __call__(self, request: object, timeout: int = 0) -> _Response:
        self.requests.append(request)
        return self.responses.pop(0)


class BuildInfraDatasetTests(unittest.TestCase):
    def test_link_header_and_uuid_are_deterministic(self) -> None:
        self.assertEqual(
            parse_link_header('<https://example.test?page=2>; rel="next"'),
            {"next": "https://example.test?page=2"},
        )
        first = deterministic_uuid("vllm-project/vllm", "I_kwDO123")
        self.assertEqual(first, deterministic_uuid("vllm-project/vllm", "I_kwDO123"))
        self.assertNotEqual(first, deterministic_uuid("langfuse/langfuse", "I_kwDO123"))

    def test_etag_cache_reuse_does_not_archive_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            opener = _Opener(
                [
                    _Response(b'{"items":[1]}', headers={"ETag": '"fixture-etag"'}),
                    _Response(b"", status=304, headers={"ETag": '"fixture-etag"'}),
                ]
            )
            client = GitHubClient(
                root,
                token="fixture-secret-token",
                opener=opener,
                sleep=lambda _seconds: None,
                min_interval=0,
            )
            first, _headers, first_hit = client.request_json("/fixture", endpoint="rest:fixture")
            second, _headers, second_hit = client.request_json("/fixture", endpoint="rest:fixture")
            self.assertEqual(first, second)
            self.assertFalse(first_hit)
            self.assertTrue(second_hit)
            request_headers = opener.requests[1].headers
            self.assertEqual(request_headers["If-none-match"], '"fixture-etag"')
            archived = "".join(path.read_text(errors="ignore") for path in root.rglob("*.json"))
            self.assertNotIn("fixture-secret-token", archived)
            with gzip.open(next(root.glob("responses/*.json.gz")), "rt") as handle:
                self.assertEqual(json.load(handle), {"items": [1]})

    def test_repository_stream_resumes_pages_and_records_cutoff_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            class FakeClient:
                def __init__(self) -> None:
                    self.checkpoints: dict[str, dict[str, object]] = {}
                    self.calls: list[dict[str, object]] = []

                def request_json(self, path: str, params: dict[str, object], *, endpoint: str):
                    self.calls.append(dict(params))
                    if params["page"] == 1:
                        return ([{"number": 1, "created_at": "2026-07-01T00:00:00Z"}], {"link": '<https://x?page=2>; rel="next"'}, False)
                    return ([], {}, False)

                def mark_checkpoint(self, key: str, value: dict[str, object]) -> None:
                    self.checkpoints[key] = value

            client = FakeClient()
            crawl_stream(client, root, "owner", "repo", "issues", "/issues", {}, stop_on_cutoff=True)
            self.assertEqual([call["per_page"] for call in client.calls], [100, 100])
            self.assertTrue(client.checkpoints["owner/repo:issues"]["complete"])
            self.assertEqual(list(_read_pages(root, "owner/repo", "issues")), [{"number": 1, "created_at": "2026-07-01T00:00:00Z"}])

    def test_repository_stream_uses_resume_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            class FakeClient:
                def __init__(self) -> None:
                    self.checkpoints = {
                        "owner/repo:issues": {"complete": False, "next_page": 2}
                    }
                    self.calls: list[int] = []

                def request_json(self, _path: str, params: dict[str, object], *, endpoint: str):
                    del endpoint
                    self.calls.append(int(params["page"]))
                    return ([{"number": 2, "created_at": "2026-07-02T00:00:00Z"}], {}, False)

                def mark_checkpoint(self, key: str, value: dict[str, object]) -> None:
                    self.checkpoints[key] = value

            client = FakeClient()
            crawl_stream(client, root, "owner", "repo", "issues", "/issues", {})
            self.assertEqual(client.calls, [2])
            self.assertTrue(client.checkpoints["owner/repo:issues"]["complete"])

    def test_rate_limit_waits_and_retries_without_persisting_token(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            retry_headers = Message()
            retry_headers["Retry-After"] = "2"
            retry = urllib.error.HTTPError(
                "https://api.github.com/fixture", 429, "rate limited", retry_headers, None
            )
            opener = _Opener([retry, _Response(b'{"ok":true}')])
            waits: list[float] = []

            class RaisingOpener(_Opener):
                def __call__(self, request: object, timeout: int = 0) -> _Response:
                    self.requests.append(request)
                    response = self.responses.pop(0)
                    if isinstance(response, Exception):
                        raise response
                    return response

            opener = RaisingOpener([retry, _Response(b'{"ok":true}')])
            client = GitHubClient(
                root,
                token="fixture-secret-token",
                opener=opener,
                sleep=waits.append,
                min_interval=0,
            )
            data, _headers, cache_hit = client.request_json("/fixture", endpoint="rest:fixture")
            self.assertEqual(data, {"ok": True})
            self.assertFalse(cache_hit)
            self.assertEqual(waits, [2.0])
            self.assertEqual(client.metrics["rest:fixture"]["requests"], 2)

    def test_graphql_batch_growth_and_overflow_resume(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            class FakeGraphQLClient:
                def __init__(self) -> None:
                    self.checkpoints: dict[str, dict[str, object]] = {}
                    self.calls: list[tuple[str, int | str]] = []
                    self.metrics = {"graphql:overflow": {"overflow_calls": 0}}

                def request_json(self, _path: str, *, endpoint: str, payload: dict[str, object], graphql: bool):
                    self.assert_graphql(graphql)
                    variables = payload["variables"]
                    if endpoint == "graphql:overflow":
                        cursors = [
                            str(value)
                            for key, value in variables.items()
                            if key.startswith("cursor")
                        ]
                        self.calls.append((endpoint, len(cursors)))
                        nodes = {}
                        for index, cursor in enumerate(cursors):
                            if cursor == "cursor-1":
                                nodes[f"n{index}"] = {
                                    "reviews": {
                                        "nodes": [{"id": "review-2"}],
                                        "pageInfo": {"hasNextPage": True, "endCursor": "cursor-2"},
                                    }
                                }
                            else:
                                nodes[f"n{index}"] = {
                                    "reviews": {
                                        "nodes": [{"id": "review-3"}],
                                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                                    }
                                }
                        return ({"data": nodes}, {}, False)
                    ids = list(variables["ids"])
                    self.calls.append((endpoint, len(ids)))
                    return (
                        {
                            "data": {
                                "rateLimit": {"cost": 100},
                                "nodes": [
                                    {
                                        "id": node_id,
                                        "reviews": {
                                            "nodes": [{"id": "review-1"}],
                                            "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                                        },
                                    }
                                    for node_id in ids
                                ],
                            }
                        },
                        {},
                        False,
                    )

                def assert_graphql(self, value: bool) -> None:
                    if not value:
                        raise AssertionError("expected GraphQL request")

                def mark_checkpoint(self, key: str, value: dict[str, object]) -> None:
                    self.checkpoints[key] = value

                def _metric(self, endpoint: str) -> dict[str, int]:
                    return self.metrics.setdefault(endpoint, {"overflow_calls": 0})

                def _save_state(self) -> None:
                    return None

            client = FakeGraphQLClient()
            node_ids = [f"node-{index}" for index in range(51)]
            crawl_graphql_metadata(client, root, "owner/repo", node_ids)
            self.assertEqual(
                [item[1] for item in client.calls if item[0] == "graphql:pull-request-nodes"],
                [50, 1],
            )
            self.assertEqual(client.metrics["graphql:overflow"]["overflow_calls"], 21)
            records = json.loads(
                (root / "graphql/owner__repo/records.json").read_text()
            )["records"]
            self.assertEqual(len(records), 51)
            self.assertEqual(
                len(records["node-0"]["reviews"]["nodes"]),
                1,
            )
            overflow = json.loads(
                (root / "graphql/owner__repo/overflow.json").read_text()
            )
            self.assertTrue(overflow["node-0:reviews"]["complete"])

    def test_graphql_batch_growth_and_overflow_query_are_bounded(self) -> None:
        from build_infra_dataset import _graphql_query, _graphql_overflow_query

        query = _graphql_query()
        self.assertIn("first: 25", query)
        self.assertIn("$ids", query)
        self.assertIn("after: $cursor", _graphql_overflow_query("files"))
        self.assertIn("userContentEdits", _graphql_overflow_query("userContentEdits"))

    def test_cutoff_body_reconstruction_and_normalization(self) -> None:
        self.assertEqual(
            _state_at_cutoff(
                {"closed_at": "2026-08-01T00:00:00Z"},
                [
                    {"event": "closed", "created_at": "2026-07-01T00:00:00Z"},
                    {"event": "reopened", "created_at": "2026-07-20T00:00:00Z"},
                ],
            ),
            "open",
        )
        self.assertEqual(
            _reverse_unified_diff(
                "before\nnew line\n",
                "@@ -1,2 +1,2 @@\n before\n-old line\n+new line\n",
            ),
            "before\nold line\n",
        )
        self.assertEqual(
            _reconstruct_body_at_cutoff("current", [{"editedAt": "2026-07-01T00:00:00Z"}]),
            ("current", "edit-history-confirms-before-cutoff"),
        )
        self.assertEqual(
            _reconstruct_body_at_cutoff("future", [{"editedAt": "2026-08-01T00:00:00Z"}]),
            ("", "omitted-unrecoverable-post-cutoff-edit"),
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            crawl = output / "provenance" / "github_crawl"
            issue = {
                "node_id": "I_fixture",
                "number": 50026,
                "title": "fixture target",
                "body": "body before cutoff",
                "created_at": "2026-07-01T00:00:00Z",
                "updated_at": "2026-07-01T00:00:00Z",
                "state": "open",
                "html_url": "https://github.com/vllm-project/vllm/issues/50026",
                "labels": [],
                "milestone": None,
            }
            _write_page(crawl / "pages" / "vllm-project__vllm" / "issues" / "page-000001.json", [issue], {}, "fixture")
            _write_page(crawl / "pages" / "vllm-project__vllm" / "releases" / "page-000001.json", [], {}, "fixture")
            count = normalize_repository(output, "vllm-project", "vllm")
            self.assertEqual(count, 1)
            document = json.loads((output / "sources/github_public/vllm-project/vllm/issues/50026.json").read_text())
            self.assertEqual(document["state_at_cutoff"], "open")
            self.assertEqual(document["repository"], "vllm-project/vllm")
            self.assertLessEqual(CUTOFF.isoformat(), "2026-07-27T23:59:59+00:00")


if __name__ == "__main__":
    unittest.main()
