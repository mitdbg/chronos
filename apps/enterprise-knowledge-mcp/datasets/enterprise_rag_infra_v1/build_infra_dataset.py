"""Build the EnterpriseRAG infrastructure-workflow derivative corpus.

The command is deliberately independent of the stage-one generator.  That keeps
the original corpus immutable and makes the expensive public-history crawl
resumable:

    python -m src.scripts.build_infra_dataset init
    python -m src.scripts.build_infra_dataset crawl
    python -m src.scripts.build_infra_dataset normalize
    python -m src.scripts.build_infra_dataset generate-internal
    python -m src.scripts.build_infra_dataset validate

``all`` runs the same phases in order.  ``--template-fallback`` reproduces the
deterministic internal records used by the Chronos paper; omit it only to build
the optional LLM-generated variant with EnterpriseRAG-Bench's configured
primary and cheap models.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import http.client
import json
import os
import re
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence


REPO_ROOT = Path.cwd()
DEFAULT_BASE = REPO_ROOT / "generated_data"
DEFAULT_OUTPUT = REPO_ROOT / "generated_data_infra_v1"
CUTOFF = dt.datetime(2026, 7, 27, 23, 59, 59, tzinfo=dt.timezone.utc)
MIN_FREE_BYTES = 15 * 1024**3
ISSUE_PAGE_BOUNDARY = 100
COMMENT_PAGE_BOUNDARY = 300
COMMENT_STREAMS = {"issue_comments", "review_comments"}
LARGE_REST_STREAMS = {"issues", *COMMENT_STREAMS}
OVERFLOW_BATCH_SIZE = 5
OVERFLOW_FLUSH_EVERY = 5
PUBLIC_REPOSITORIES = (
    ("vllm-project", "vllm"),
    ("BerriAI", "litellm"),
    ("langfuse", "langfuse"),
)
REMOVED_UPSTREAM_SUMMARIES = (
    "codebases/upstream-issues/vllm-50026.md",
    "codebases/upstream-issues/vllm-49980.md",
    "codebases/upstream-issues/vllm-34752.md",
    "codebases/upstream-issues/vllm-48627.md",
    "codebases/upstream-issues/langfuse-12736.md",
    "codebases/upstream-issues/langfuse-15208.md",
)
DERIVED_INDEX_FILES = {"uuid_index.json", "source_tree.txt"}
TARGET_ISSUES = {
    ("vllm-project", "vllm", 50026),
    ("vllm-project", "vllm", 49980),
    ("vllm-project", "vllm", 34752),
    ("vllm-project", "vllm", 48627),
    ("langfuse", "langfuse", 12736),
    ("langfuse", "langfuse", 15208),
}
PINNED_COMMITS = {
    "vllm-project/vllm": "272abd5f486967f1fb9db7ca7504f8c34235ef50",
    "BerriAI/litellm": "8f86c87f8e065343af6e74e0823ab8a8276b9528",
    "langfuse/langfuse": "c4eaf1f4c0cd1851f0c9dbea2802c243cf787a09",
}
INTERNAL_COUNTS = {
    "slack": 70,
    "linear": 30,
    "gmail": 25,
    "github": 20,
    "confluence": 20,
    "google_drive": 15,
    "jira": 10,
    "fireflies": 10,
}
INTERNAL_TITLE_FIELDS = {
    "slack": "channel",
    "gmail": "subject",
    "jira": "summary",
}
PROJECTS = (
    (
        "vllm-ownership-adoption",
        "Adopt vLLM as an upstream-first serving dependency and define Redwood ownership, qualification, and contribution boundaries.",
        "vllm-project/vllm",
        (50026, 49980),
    ),
    (
        "vllm-gpu-cache-performance",
        "Qualify vLLM GPU scheduling, attention, FlashInfer, and KV-cache behavior against Redwood serving workloads.",
        "vllm-project/vllm",
        (49980, 34752),
    ),
    (
        "vllm-api-speculative-qualification",
        "Validate vLLM API contracts and speculative-decoding behavior before rollout, while keeping fixes upstreamable.",
        "vllm-project/vllm",
        (50026, 48627),
    ),
    (
        "langfuse-ingestion-observability",
        "Operate Langfuse ingestion and trace observability as a qualified upstream dependency with bounded downstream patches.",
        "langfuse/langfuse",
        (12736,),
    ),
    (
        "langfuse-analytics-evaluation-correctness",
        "Qualify Langfuse analytics and evaluation semantics, especially aggregation, histogram, and historical trace correctness.",
        "langfuse/langfuse",
        (15208,),
    ),
    (
        "shared-upstream-governance-incident-response",
        "Coordinate upstream issue response, incident containment, release qualification, and durable contribution policy across both dependencies.",
        "vllm-project/vllm; langfuse/langfuse",
        (50026, 12736, 15208),
    ),
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def parse_timestamp(value: Any) -> dt.datetime | None:
    if value is None or value == "":
        return None
    text = str(value).strip()
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        text += "T23:59:59Z"
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def at_or_before_cutoff(value: Any) -> bool:
    parsed = parse_timestamp(value)
    return parsed is not None and parsed <= CUTOFF


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_bytes(
        path,
        (json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        ),
    )


def read_json(path: Path, default: Any = None) -> Any:
    if not path.is_file():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.replace("\x00", "").strip()
    return str(value)


def parse_link_header(value: str | None) -> dict[str, str]:
    """Parse GitHub's RFC 8288 Link header without relying on requests."""

    links: dict[str, str] = {}
    if not value:
        return links
    for item in value.split(","):
        match = re.search(r"<([^>]+)>\s*;\s*rel=\"?([^\";]+)", item)
        if match:
            links[match.group(2)] = match.group(1)
    return links


def endpoint_key(url: str, payload: Any | None = None) -> str:
    encoded = url if payload is None else url + "\n" + json.dumps(payload, sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class GitHubHTTPError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class GraphQLResponseError(RuntimeError):
    pass


class GitHubClient:
    """Serialized authenticated GitHub client with ETag and checkpoint support."""

    def __init__(
        self,
        crawl_root: Path,
        token: str | None = None,
        *,
        opener: Callable[..., Any] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        min_interval: float | None = None,
    ):
        self.root = crawl_root
        self.responses = crawl_root / "responses"
        self.responses.mkdir(parents=True, exist_ok=True)
        self.metrics_path = crawl_root / "metrics.json"
        self.checkpoints_path = crawl_root / "checkpoints.json"
        self.token = token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if not self.token:
            raise ValueError("Set GITHUB_TOKEN or GH_TOKEN before crawling GitHub.")
        self.opener = opener or urllib.request.urlopen
        self.sleep = sleep
        self.min_interval = (
            float(os.environ.get("GITHUB_MIN_INTERVAL_SECONDS", "0.5"))
            if min_interval is None
            else min_interval
        )
        self._last_request = 0.0
        self.metrics = read_json(self.metrics_path, {}) or {}
        self.checkpoints = read_json(self.checkpoints_path, {}) or {}

    def _metric(self, endpoint: str) -> dict[str, Any]:
        metric = self.metrics.setdefault(
            endpoint,
            {
                "objects": 0,
                "bytes": 0,
                "requests": 0,
                "cache_hits": 0,
                "api_cost": 0,
                "overflow_calls": 0,
            },
        )
        return metric

    def _save_state(self) -> None:
        atomic_write_json(self.metrics_path, self.metrics)
        atomic_write_json(self.checkpoints_path, self.checkpoints)

    def _wait_for_pacing(self) -> None:
        delay = self.min_interval - (time.monotonic() - self._last_request)
        if delay > 0:
            self.sleep(delay)

    def _cache_paths(self, cache_key: str) -> tuple[Path, Path]:
        return (
            self.responses / f"{cache_key}.json.gz",
            self.responses / f"{cache_key}.headers.json",
        )

    def _read_cache(self, cache_key: str) -> tuple[Any, dict[str, str]] | None:
        data_path, header_path = self._cache_paths(cache_key)
        if not data_path.is_file() or not header_path.is_file():
            return None
        with gzip.open(data_path, "rb") as handle:
            data = json.loads(handle.read().decode("utf-8"))
        return data, read_json(header_path, {}) or {}

    def _save_cache(
        self, cache_key: str, data: Any, headers: Mapping[str, str], body_bytes: bytes
    ) -> None:
        data_path, header_path = self._cache_paths(cache_key)
        atomic_write_bytes(data_path, gzip.compress(body_bytes, compresslevel=6))
        safe_headers = {str(k).lower(): str(v) for k, v in headers.items()}
        safe_headers["response_sha256"] = hashlib.sha256(body_bytes).hexdigest()
        atomic_write_json(header_path, safe_headers)

    def request_json(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        endpoint: str | None = None,
        payload: Any | None = None,
        graphql: bool = False,
        max_retries: int = 8,
    ) -> tuple[Any, dict[str, str], bool]:
        if graphql:
            url = "https://api.github.com/graphql"
            body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            cache_key = endpoint_key(url, payload)
            request_data = body
        else:
            query = urllib.parse.urlencode(
                [(key, value) for key, value in (params or {}).items() if value is not None]
            )
            url = "https://api.github.com" + path
            if query:
                url += "?" + query
            cache_key = endpoint_key(url)
            request_data = None
        metric_name = endpoint or ("graphql" if graphql else path.split("/")[-1])
        metric = self._metric(metric_name)
        cached = self._read_cache(cache_key)
        cached_headers = cached[1] if cached else {}
        request_headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Authorization": f"Bearer {self.token}",
            "User-Agent": "EnterpriseRAG-infrastructure-dataset/1.0",
        }
        if cached_headers.get("etag"):
            request_headers["If-None-Match"] = cached_headers["etag"]
        request = urllib.request.Request(
            url,
            data=request_data,
            headers=request_headers,
            method="POST" if graphql else "GET",
        )
        for attempt in range(max_retries + 1):
            self._wait_for_pacing()
            try:
                self._last_request = time.monotonic()
                metric["requests"] += 1
                with self.opener(request, timeout=90) as response:
                    status = int(getattr(response, "status", 200))
                    response_body = response.read()
                    response_headers = dict(response.headers.items())
                if status == 304 and cached:
                    metric["cache_hits"] += 1
                    self._save_state()
                    return cached[0], cached_headers, True
                if status < 200 or status >= 300:
                    raise GitHubHTTPError(status, f"GitHub returned HTTP {status}")
                data = json.loads(response_body.decode("utf-8"))
                if graphql and data.get("errors"):
                    raise GraphQLResponseError(json.dumps(data["errors"], sort_keys=True))
                self._save_cache(cache_key, data, response_headers, response_body)
                metric["bytes"] += len(response_body)
                if graphql:
                    rate = data.get("data", {}).get("rateLimit", {})
                    metric["api_cost"] += int(rate.get("cost") or 0)
                if isinstance(data, list):
                    metric["objects"] += len(data)
                elif graphql:
                    metric["objects"] += len(
                        ((data.get("data") or {}).get("nodes") or [])
                    )
                else:
                    metric["objects"] += 1
                self._save_state()
                return data, {str(k).lower(): str(v) for k, v in response_headers.items()}, False
            except urllib.error.HTTPError as error:
                status = int(error.code)
                if status == 304 and cached:
                    metric["cache_hits"] += 1
                    self._save_state()
                    return cached[0], cached_headers, True
                retry_after = error.headers.get("Retry-After")
                reset = error.headers.get("X-RateLimit-Reset")
                if status not in {403, 429, 500, 502, 503, 504} or attempt >= max_retries:
                    raise GitHubHTTPError(status, f"GitHub request failed with HTTP {status}") from error
                wait = float(retry_after) if retry_after else 2**attempt
                honors_primary_reset = bool(reset and status == 403)
                if honors_primary_reset:
                    wait = max(wait, float(reset) - time.time() + 1)
                self.sleep(
                    max(wait, 1.0)
                    if honors_primary_reset
                    else min(max(wait, 1.0), 300.0)
                )
            except (
                TimeoutError,
                urllib.error.URLError,
                http.client.IncompleteRead,
                ConnectionResetError,
                GraphQLResponseError,
            ) as error:
                if isinstance(error, GraphQLResponseError) and attempt >= max_retries:
                    raise
                if attempt >= max_retries:
                    raise
                self.sleep(min(2**attempt, 120.0))
        raise RuntimeError("unreachable GitHub retry state")

    def mark_checkpoint(self, key: str, value: Mapping[str, Any]) -> None:
        self.checkpoints[key] = dict(value)
        self._save_state()


def _page_path(crawl_root: Path, repo_slug: str, stream: str, page: int) -> Path:
    return crawl_root / "pages" / repo_slug.replace("/", "__") / stream / f"page-{page:06d}.json"


def _page_file_exists(path: Path) -> bool:
    return path.is_file() or path.with_name(path.name + ".gz").is_file()


def _write_page(path: Path, data: Any, headers: Mapping[str, str], url: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, {"url": url, "headers": dict(headers), "data": data})


def _next_page_number(crawl_root: Path, repo_slug: str, stream: str) -> int:
    directory = _page_path(crawl_root, repo_slug, stream, 1).parent
    pages = list(directory.glob("page-*.json")) + list(directory.glob("page-*.json.gz"))
    numbers = [
        int(match.group(1))
        for path in pages
        if (match := re.search(r"page-(\d+)", path.name))
    ]
    return max(numbers, default=0) + 1


def _max_item_timestamp(items: Sequence[Any], field: str = "updated_at") -> str | None:
    timestamps = [
        parse_timestamp(item.get(field))
        for item in items
        if isinstance(item, Mapping) and parse_timestamp(item.get(field))
    ]
    if not timestamps:
        return None
    return max(timestamps).isoformat().replace("+00:00", "Z")


def _read_pages(crawl_root: Path, repo_slug: str, stream: str) -> Iterator[dict[str, Any]]:
    directory = crawl_root / "pages" / repo_slug.replace("/", "__") / stream
    paths = list(directory.glob("page-*.json")) + list(directory.glob("page-*.json.gz"))
    for path in sorted(paths):
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                value = json.load(handle) or {}
        else:
            value = read_json(path, {}) or {}
        if isinstance(value.get("data"), list):
            yield from value["data"]


def _created_after_cutoff(value: Mapping[str, Any]) -> bool:
    return bool(
        value.get("created_at")
        and parse_timestamp(value.get("created_at"))
        and parse_timestamp(value.get("created_at")) > CUTOFF
    )


def crawl_stream(
    client: GitHubClient,
    crawl_root: Path,
    owner: str,
    repo: str,
    stream: str,
    path: str,
    params: Mapping[str, Any],
    *,
    stop_on_cutoff: bool = False,
) -> None:
    repo_slug = f"{owner}/{repo}"
    checkpoint_key = f"{repo_slug}:{stream}"
    checkpoint = client.checkpoints.get(checkpoint_key, {})
    pagination_mode = str(checkpoint.get("pagination_mode") or "")
    sort_mode = str(checkpoint.get("sort_mode") or ("updated" if stream == "issues" else "created"))
    effective_params = dict(params)
    last_cursor = checkpoint.get("last_cursor")
    # A completed stream is durable.  Re-fetching a capped comment tail on
    # every restart can duplicate hundreds of pages and exhaust local storage.
    capped_comment_resume = False
    if checkpoint.get("complete") and not capped_comment_resume:
        return
    if capped_comment_resume:
        pagination_mode = "since"
        sort_mode = "created"
        effective_params["since"] = last_cursor
        page = 1
        storage_page = _next_page_number(crawl_root, repo_slug, stream)
    elif pagination_mode in {"updated", "since"} and sort_mode == "updated":
        effective_params.update({"sort": "updated", "direction": "asc"})
        if checkpoint.get("since"):
            effective_params["since"] = checkpoint["since"]
        page = int(checkpoint.get("next_page", 1))
        storage_page = int(checkpoint.get("storage_page", page))
    else:
        page = int(checkpoint.get("next_page", 1))
        storage_page = int(checkpoint.get("storage_page", page))
    last_items: list[Any] = []
    while True:
        page_params = dict(effective_params)
        page_params.update({"page": page, "per_page": 100})
        try:
            data, headers, _cache_hit = client.request_json(
                path, page_params, endpoint=f"rest:{stream}"
            )
        except GitHubHTTPError as error:
            # GitHub rejects page-based pagination for large REST collections.
            # Preserve the completed created-order pages, then crawl the same
            # collection in updated order and partition its tail with `since`.
            # The latter is intentionally limited to the issue identity stream;
            # other streams remain ordinary page-based REST collections.
            boundary = COMMENT_PAGE_BOUNDARY if stream in COMMENT_STREAMS else ISSUE_PAGE_BOUNDARY
            if stream not in LARGE_REST_STREAMS or error.status != 422 or page < boundary:
                raise
            if not pagination_mode:
                if stream in COMMENT_STREAMS:
                    cursor = _max_item_timestamp(last_items) or normalize_text(last_cursor)
                    if not cursor:
                        raise
                    pagination_mode = "since"
                    sort_mode = "created"
                    effective_params = dict(params)
                    effective_params["since"] = cursor
                    page = 1
                    storage_page = _next_page_number(crawl_root, repo_slug, stream)
                    client.mark_checkpoint(
                        checkpoint_key,
                        {
                            "complete": False,
                            "pagination_mode": pagination_mode,
                            "sort_mode": sort_mode,
                            "since": cursor,
                            "next_page": page,
                            "storage_page": storage_page,
                            "last_cursor": cursor,
                        },
                    )
                    continue
                pagination_mode = "updated"
                sort_mode = "updated"
                effective_params = dict(params)
                effective_params.update({"sort": "updated", "direction": "asc"})
                page = 1
                storage_page = _next_page_number(crawl_root, repo_slug, stream)
                client.mark_checkpoint(
                    checkpoint_key,
                    {
                        "complete": False,
                        "pagination_mode": pagination_mode,
                        "sort_mode": sort_mode,
                        "next_page": page,
                        "storage_page": storage_page,
                    },
                )
                continue
            cursor = _max_item_timestamp(last_items) or normalize_text(last_cursor)
            if not cursor:
                raise
            if pagination_mode == "since" and cursor == effective_params.get("since"):
                raise GitHubHTTPError(
                    422,
                    "GitHub large-dataset pagination made no timestamp progress; "
                    "manual partitioning is required",
                ) from error
            pagination_mode = "since"
            sort_mode = "updated" if stream == "issues" else sort_mode
            effective_params = dict(effective_params)
            effective_params["since"] = cursor
            page = 1
            storage_page = _next_page_number(crawl_root, repo_slug, stream)
            client.mark_checkpoint(
                checkpoint_key,
                {
                    "complete": False,
                    "pagination_mode": pagination_mode,
                    "sort_mode": sort_mode,
                    "since": cursor,
                    "next_page": page,
                    "storage_page": storage_page,
                    "last_cursor": cursor,
                },
            )
            continue
        page_url = "https://api.github.com" + path + "?" + urllib.parse.urlencode(page_params)
        page_file = _page_path(crawl_root, repo_slug, stream, storage_page)
        if not _page_file_exists(page_file):
            _write_page(page_file, data, headers, page_url)
        items = data if isinstance(data, list) else []
        last_items = list(items)
        last_cursor = _max_item_timestamp(items)
        next_link = parse_link_header(headers.get("link")).get("next")
        stop_at_cutoff = stop_on_cutoff and not pagination_mode
        if not items or (
            stop_at_cutoff
            and any(_created_after_cutoff(item) for item in items if isinstance(item, Mapping))
        ):
            client.mark_checkpoint(
                checkpoint_key,
                {
                    "complete": True,
                    "next_page": page,
                    "storage_page": storage_page,
                    **({"last_cursor": last_cursor} if last_cursor else {}),
                },
            )
            return
        if not next_link:
            if (
                stream in COMMENT_STREAMS
                and not pagination_mode
                and storage_page >= COMMENT_PAGE_BOUNDARY
                and last_cursor
            ):
                pagination_mode = "since"
                sort_mode = "created"
                effective_params = dict(params)
                effective_params["since"] = last_cursor
                page = 1
                storage_page = _next_page_number(crawl_root, repo_slug, stream)
                client.mark_checkpoint(
                    checkpoint_key,
                    {
                        "complete": False,
                        "pagination_mode": pagination_mode,
                        "sort_mode": sort_mode,
                        "since": last_cursor,
                        "next_page": page,
                        "storage_page": storage_page,
                        "last_cursor": last_cursor,
                    },
                )
                continue
            client.mark_checkpoint(
                checkpoint_key,
                {
                    "complete": True,
                    "next_page": page,
                    "storage_page": storage_page,
                    **({"last_cursor": last_cursor} if last_cursor else {}),
                    **({"pagination_mode": pagination_mode} if pagination_mode else {}),
                    **({"sort_mode": sort_mode} if pagination_mode else {}),
                },
            )
            return
        page += 1
        storage_page += 1
        client.mark_checkpoint(
            checkpoint_key,
            {
                "complete": False,
                "next_page": page,
                "storage_page": storage_page,
                **({"last_cursor": last_cursor} if last_cursor else {}),
                **({"pagination_mode": pagination_mode} if pagination_mode else {}),
                **({"sort_mode": sort_mode} if pagination_mode else {}),
                **({"since": effective_params["since"]} if effective_params.get("since") else {}),
            },
        )


def _graphql_query() -> str:
    return """
    query($ids: [ID!]!) {
      rateLimit { cost remaining resetAt }
      nodes(ids: $ids) {
        __typename
        ... on PullRequest {
          id number title body url createdAt updatedAt closedAt mergedAt state
          reviews(first: 25) {
            nodes { id body state submittedAt updatedAt author { login } }
            pageInfo { hasNextPage endCursor }
          }
          commits(first: 25) {
            nodes { commit { oid committedDate messageHeadline message author { user { login } } } }
            pageInfo { hasNextPage endCursor }
          }
          files(first: 25) {
            nodes { path additions deletions changeType }
            pageInfo { hasNextPage endCursor }
          }
        }
      }
    }
    """


def _graphql_edit_query() -> str:
    return """
    query($ids: [ID!]!) {
      rateLimit { cost remaining resetAt }
      nodes(ids: $ids) {
        __typename
        ... on Issue {
          id number title body updatedAt
          userContentEdits(first: 100) {
            nodes { editedAt diff }
            pageInfo { hasNextPage endCursor }
          }
        }
        ... on PullRequest {
          id number title body updatedAt
          userContentEdits(first: 100) {
            nodes { editedAt diff }
            pageInfo { hasNextPage endCursor }
          }
        }
      }
    }
    """


def _graphql_overflow_query(connection: str) -> str:
    return (
        _graphql_overflow_batch_query(connection, 1)
        .replace("$id0", "$id")
        .replace("$cursor0", "$cursor")
        .replace("n0: node", "node")
    )


def _graphql_overflow_batch_query(connection: str, batch_size: int) -> str:
    """Build one query for independent overflow cursors.

    Cursors are specific to a PR, so aliases let us follow several connections
    in one request without pretending that they share a cursor.
    """

    if connection not in {"userContentEdits", "reviews", "commits", "files"}:
        raise ValueError(f"Unsupported GraphQL overflow connection: {connection}")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    fragments = {
        "userContentEdits": "userContentEdits(first: 100, after: $cursor{index}) { nodes { editedAt diff } pageInfo { hasNextPage endCursor } }",
        "reviews": "reviews(first: 100, after: $cursor{index}) { nodes { id body state submittedAt updatedAt author { login } } pageInfo { hasNextPage endCursor } }",
        "commits": "commits(first: 100, after: $cursor{index}) { nodes { commit { oid committedDate messageHeadline message author { user { login } } } } pageInfo { hasNextPage endCursor } }",
        "files": "files(first: 100, after: $cursor{index}) { nodes { path additions deletions changeType } pageInfo { hasNextPage endCursor } }",
    }
    definitions = ", ".join(
        f"$id{index}: ID!, $cursor{index}: String!"
        for index in range(batch_size)
    )
    selections = []
    for index in range(batch_size):
        fragment = fragments[connection].replace("{index}", str(index))
        issue_clause = (
            f"... on Issue {{ {fragment} }}" if connection == "userContentEdits" else ""
        )
        selections.append(
            f"n{index}: node(id: $id{index}) {{ {issue_clause} ... on PullRequest {{ {fragment} }} }}"
        )
    return "query(" + definitions + ") { rateLimit { cost remaining resetAt } " + " ".join(selections) + " }"


def crawl_graphql_metadata(
    client: GitHubClient,
    crawl_root: Path,
    repo_slug: str,
    node_ids: Sequence[str],
    *,
    query: str | None = None,
    endpoint: str = "graphql:pull-request-nodes",
    optional: bool = False,
) -> None:
    if not node_ids:
        return
    result_path = crawl_root / "graphql" / repo_slug.replace("/", "__") / "records.json"
    existing = read_json(result_path, {}) or {}
    records: dict[str, Any] = existing.get("records", {})
    checkpoint_key = f"graphql:{repo_slug}:{endpoint}"
    completed = set(
        str(item)
        for item in (client.checkpoints.get(checkpoint_key, {}).get("completed_ids", []))
    )
    pending_ids = [str(node_id) for node_id in node_ids if str(node_id) not in completed]
    batch_size = 50
    index = 0
    query = query or _graphql_query()
    while index < len(pending_ids):
        batch = list(pending_ids[index : index + batch_size])
        payload = {"query": query, "variables": {"ids": batch}}
        try:
            data, _headers, _cached = client.request_json(
                "/graphql", endpoint=endpoint, payload=payload, graphql=True
            )
        except (
            GraphQLResponseError,
            GitHubHTTPError,
            TimeoutError,
            urllib.error.URLError,
            http.client.IncompleteRead,
            ConnectionResetError,
        ):
            if batch_size <= 1:
                if optional:
                    atomic_write_json(
                        crawl_root
                        / "graphql"
                        / repo_slug.replace("/", "__")
                        / f"{endpoint.replace(':', '_')}-unavailable.json",
                        {"repository": repo_slug, "endpoint": endpoint, "status": "unavailable"},
                    )
                    return
                raise
            batch_size = max(1, batch_size // 2)
            continue
        nodes = (data.get("data") or {}).get("nodes") or []
        for node in nodes:
            if isinstance(node, Mapping) and node.get("id"):
                node_id = str(node["id"])
                previous = records.get(node_id, {})
                merged = dict(previous)
                merged.update(node)
                records[node_id] = merged
        index += len(batch)
        completed.update(batch)
        api_cost = int(
            ((data.get("data") or {}).get("rateLimit") or {}).get("cost") or 0
        )
        if batch_size < 100 and api_cost <= 500:
            batch_size = min(100, batch_size * 2)
        atomic_write_json(result_path, {"repository": repo_slug, "records": records})
        client.mark_checkpoint(
            checkpoint_key,
            {"complete": index >= len(pending_ids), "completed_ids": sorted(completed)},
        )
    # Nested connections are followed only when the initial page says more data
    # exists. Independent PR cursors are batched to keep large repositories from
    # degenerating into one GraphQL request per connection.
    overflow_path = crawl_root / "graphql" / repo_slug.replace("/", "__") / "overflow.json"
    overflow = read_json(overflow_path, {}) or {}
    for connection in ("userContentEdits", "reviews", "commits", "files"):
        work: list[dict[str, str]] = []
        flush_count = 0
        for node_id, node in records.items():
            page_info = (node.get(connection) or {}).get("pageInfo") or {}
            initial_cursor = page_info.get("endCursor")
            if not page_info.get("hasNextPage") or not initial_cursor:
                continue
            key = f"{node_id}:{connection}"
            state = overflow.setdefault(key, {})
            if state.get("complete"):
                continue
            work.append(
                {
                    "key": key,
                    "node_id": node_id,
                    "cursor": str(state.get("next_cursor") or initial_cursor),
                }
            )

        def flush_overflow(*, force: bool = False) -> None:
            nonlocal flush_count
            if force or flush_count >= OVERFLOW_FLUSH_EVERY:
                atomic_write_json(overflow_path, overflow)
                flush_count = 0

        def process_batch(batch: list[dict[str, str]]) -> None:
            nonlocal flush_count
            variables: dict[str, str] = {}
            for index, entry in enumerate(batch):
                variables[f"id{index}"] = entry["node_id"]
                variables[f"cursor{index}"] = entry["cursor"]
            payload = {
                "query": _graphql_overflow_batch_query(connection, len(batch)),
                "variables": variables,
            }
            try:
                data, _headers, _cached = client.request_json(
                    "/graphql", endpoint="graphql:overflow", payload=payload, graphql=True
                )
            except (
                GraphQLResponseError,
                GitHubHTTPError,
                TimeoutError,
                urllib.error.URLError,
                http.client.IncompleteRead,
                ConnectionResetError,
            ):
                if len(batch) <= 1:
                    raise
                midpoint = len(batch) // 2
                process_batch(batch[:midpoint])
                process_batch(batch[midpoint:])
                return

            metric = client._metric("graphql:overflow")
            metric["overflow_calls"] += 1
            client._save_state()
            response_data = data.get("data") or {}
            for index, entry in enumerate(batch):
                child = (response_data.get(f"n{index}") or {}).get(connection) or {}
                state = overflow.setdefault(entry["key"], {})
                state.setdefault("nodes", []).extend(child.get("nodes") or [])
                child_info = child.get("pageInfo") or {}
                next_cursor = (
                    str(child_info.get("endCursor"))
                    if child_info.get("hasNextPage") and child_info.get("endCursor")
                    else None
                )
                state["next_cursor"] = next_cursor
                if next_cursor:
                    state["complete"] = False
                    work.append({**entry, "cursor": next_cursor})
                else:
                    state["complete"] = True
            flush_count += 1
            flush_overflow()

        while work:
            batch = work[:OVERFLOW_BATCH_SIZE]
            del work[: len(batch)]
            process_batch(batch)
        flush_overflow(force=True)


def crawl_repository(output: Path, owner: str, repo: str) -> None:
    crawl_root = output / "provenance" / "github_crawl"
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    client = GitHubClient(crawl_root, token=token)
    base = f"/repos/{owner}/{repo}"
    # The issues stream is the authoritative repository-wide issue/PR identity
    # stream.  It avoids a second complete pull-request listing.
    crawl_stream(
        client,
        crawl_root,
        owner,
        repo,
        "issues",
        f"{base}/issues",
        {"state": "all", "sort": "created", "direction": "asc"},
        stop_on_cutoff=True,
    )
    streams = (
        ("issue_comments", f"{base}/issues/comments", {"sort": "created", "direction": "asc"}, True),
        ("review_comments", f"{base}/pulls/comments", {"sort": "created", "direction": "asc"}, True),
        ("issue_events", f"{base}/issues/events", {}, False),
        ("releases", f"{base}/releases", {}, False),
        ("labels", f"{base}/labels", {}, False),
        ("milestones", f"{base}/milestones", {"state": "all", "sort": "created", "direction": "asc"}, True),
    )
    for stream, path, params, stop in streams:
        crawl_stream(client, crawl_root, owner, repo, stream, path, params, stop_on_cutoff=stop)
    issue_records = list(_read_pages(crawl_root, f"{owner}/{repo}", "issues"))
    issue_records_by_node = {
        str(item["node_id"]): item
        for item in issue_records
        if isinstance(item, Mapping) and item.get("node_id")
    }
    pr_nodes = list(
        dict.fromkeys(
            str(item["node_id"])
            for item in issue_records
            if isinstance(item, Mapping)
            and item.get("pull_request")
            and item.get("node_id")
            and at_or_before_cutoff(item.get("created_at"))
        )
    )
    crawl_graphql_metadata(client, crawl_root, f"{owner}/{repo}", pr_nodes)
    edited_nodes = [
        node_id
        for node_id, item in issue_records_by_node.items()
        if at_or_before_cutoff(item.get("created_at"))
        if parse_timestamp(item.get("updated_at"))
        and parse_timestamp(item.get("updated_at")) > CUTOFF
    ]
    crawl_graphql_metadata(
        client,
        crawl_root,
        f"{owner}/{repo}",
        edited_nodes,
        query=_graphql_edit_query(),
        endpoint="graphql:issue-edit-history",
        optional=True,
    )
    client.mark_checkpoint(
        f"{owner}/{repo}:repository",
        {"complete": True, "cutoff": CUTOFF.isoformat().replace("+00:00", "Z"), "completed_at": utc_now()},
    )


_DIFF_HUNK = re.compile(
    r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@"
)


def _reverse_unified_diff(current: str, diff: str) -> str | None:
    """Reverse one verified unified diff, returning ``None`` if it does not fit."""

    lines = current.splitlines(keepends=True)
    raw_lines = diff.splitlines(keepends=True)
    hunks: list[tuple[int, list[str], list[str]]] = []
    index = 0
    while index < len(raw_lines):
        match = _DIFF_HUNK.match(raw_lines[index].rstrip("\n"))
        if not match:
            index += 1
            continue
        old_lines: list[str] = []
        new_lines: list[str] = []
        index += 1
        while index < len(raw_lines) and not raw_lines[index].startswith("@@ "):
            line = raw_lines[index]
            if line.startswith(" "):
                old_lines.append(line[1:])
                new_lines.append(line[1:])
            elif line.startswith("-"):
                old_lines.append(line[1:])
            elif line.startswith("+"):
                new_lines.append(line[1:])
            elif line.startswith("\\"):
                pass
            else:
                return None
            index += 1
        new_start = int(match.group(3)) - 1
        hunks.append((new_start, old_lines, new_lines))
    if not hunks:
        return None
    for new_start, old_lines, new_lines in reversed(hunks):
        if new_start < 0 or lines[new_start : new_start + len(new_lines)] != new_lines:
            return None
        lines[new_start : new_start + len(new_lines)] = old_lines
    return "".join(lines)


def _reconstruct_body_at_cutoff(
    current_body: str, edits: Sequence[Mapping[str, Any]]
) -> tuple[str, str]:
    """Use edit timestamps as a conservative historical-body reconstruction.

    GitHub exposes edit diffs, but their format has changed across API versions.
    We therefore safely retain the current body when every recorded edit is at or
    before the cutoff, and omit it when a later edit cannot be reversed without
    guessing.  This prevents a post-cutoff body from becoming searchable.
    """

    post_cutoff = [
        edit
        for edit in edits
        if parse_timestamp(edit.get("editedAt"))
        and parse_timestamp(edit.get("editedAt")) > CUTOFF
    ]
    if not post_cutoff:
        return current_body, "edit-history-confirms-before-cutoff"
    reconstructed = current_body
    for edit in sorted(
        post_cutoff,
        key=lambda item: parse_timestamp(item.get("editedAt")) or CUTOFF,
        reverse=True,
    ):
        previous = edit.get("previous_body") or edit.get("previousBody")
        if previous is not None:
            reconstructed = normalize_text(previous)
            continue
        diff = edit.get("diff") or edit.get("patch")
        if not isinstance(diff, str):
            return "", "omitted-unrecoverable-post-cutoff-edit"
        reconstructed = _reverse_unified_diff(reconstructed, diff) or ""
        if not reconstructed:
            return "", "omitted-unrecoverable-post-cutoff-edit"
    return reconstructed, "reconstructed-post-cutoff-edit-history"


def _body_at_cutoff(
    record: Mapping[str, Any], node: Mapping[str, Any] | None = None
) -> tuple[str, str]:
    """Return a body only when it is safe to expose at the dataset cutoff.

    GitHub's REST object does not expose a body-edit timestamp.  The crawler
    records optional GraphQL edit history when available.  If a record was
    updated after the cutoff and no safe historical body is available, current
    content is intentionally omitted instead of leaking future text.
    """

    body = normalize_text(record.get("body"))
    node = node or {}
    edits_connection = node.get("userContentEdits") or {}
    edits = edits_connection.get("nodes") or []
    if edits or edits_connection.get("pageInfo"):
        if (edits_connection.get("pageInfo") or {}).get("hasNextPage"):
            return "", "omitted-incomplete-edit-history"
        return _reconstruct_body_at_cutoff(normalize_text(node.get("body") or body), edits)
    if at_or_before_cutoff(record.get("updated_at")):
        return body, "rest-object-before-cutoff"
    historical = record.get("body_at_cutoff")
    if historical is not None:
        return normalize_text(historical), "reconstructed-edit-history"
    return "", "omitted-unrecoverable-post-cutoff-edit"


def _state_at_cutoff(
    record: Mapping[str, Any], events: Sequence[Mapping[str, Any]] = ()
) -> str:
    """Reconstruct the issue/PR lifecycle state at the dataset cutoff."""

    state = "open"
    if at_or_before_cutoff(record.get("merged_at")):
        state = "merged"
    elif at_or_before_cutoff(record.get("closed_at")):
        state = "closed"
    for event in sorted(
        events,
        key=lambda item: parse_timestamp(item.get("created_at")) or CUTOFF,
    ):
        timestamp = parse_timestamp(event.get("created_at"))
        if not timestamp or timestamp > CUTOFF:
            continue
        event_type = event.get("event")
        if event_type == "reopened":
            state = "open"
        elif event_type == "closed" and state != "merged":
            state = "closed"
        elif event_type == "merged":
            state = "merged"
    return state


def deterministic_uuid(repository: str, node_id: str) -> str:
    digest = hashlib.sha256(f"{repository}:{node_id}".encode("utf-8")).hexdigest()[:32]
    return f"gh_{digest}"


def _graphql_for_node(
    crawl_root: Path,
    repo_slug: str,
    node_id: str,
    records: Mapping[str, Any] | None = None,
    overflow: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if records is None:
        records = read_json(
            crawl_root / "graphql" / repo_slug.replace("/", "__") / "records.json", {}
        ) or {}
    node = dict((records.get("records") or {}).get(node_id) or {})
    if overflow is None:
        overflow = read_json(
            crawl_root / "graphql" / repo_slug.replace("/", "__") / "overflow.json", {}
        ) or {}
    for connection in ("userContentEdits", "reviews", "commits", "files"):
        extra = (overflow.get(f"{node_id}:{connection}") or {}).get("nodes") or []
        if extra:
            node.setdefault(connection, {}).setdefault("nodes", []).extend(extra)
    return node


def _comment_text(comment: Mapping[str, Any]) -> str:
    author = ((comment.get("user") or {}).get("login") or (comment.get("author") or {}).get("login") or "unknown")
    created = normalize_text(comment.get("created_at") or comment.get("createdAt"))
    body = normalize_text(comment.get("body")) if at_or_before_cutoff(comment.get("updated_at") or comment.get("updatedAt") or comment.get("created_at") or comment.get("createdAt")) else "[body omitted: edited after cutoff]"
    return f"{created} {author}: {body}".strip()


def _review_text(review: Mapping[str, Any]) -> str:
    author = ((review.get("author") or {}).get("login") or "unknown")
    submitted = normalize_text(review.get("submitted_at") or review.get("submittedAt"))
    state = normalize_text(review.get("state"))
    updated = review.get("updated_at") or review.get("updatedAt") or review.get("submitted_at") or review.get("submittedAt")
    body = (
        normalize_text(review.get("body"))
        if at_or_before_cutoff(updated)
        else "[body omitted: edited after cutoff]"
    )
    return f"{submitted} {author} [{state}]: {body}".strip()


def _commit_text(commit: Mapping[str, Any]) -> str:
    nested = commit.get("commit") if isinstance(commit.get("commit"), Mapping) else commit
    date = normalize_text(nested.get("committedDate") or nested.get("committed_at"))
    headline = normalize_text(nested.get("messageHeadline") or nested.get("message"))
    oid = normalize_text(nested.get("oid") or commit.get("sha"))
    return f"{date} {oid[:12]}: {headline}".strip()


def _changed_file_text(file: Mapping[str, Any]) -> str:
    return "{} (+{}, -{}, {})".format(
        normalize_text(file.get("path")),
        normalize_text(file.get("additions")),
        normalize_text(file.get("deletions")),
        normalize_text(file.get("changeType") or file.get("status")),
    ).strip()


def _event_text(event: Mapping[str, Any]) -> tuple[int | None, str]:
    issue = event.get("issue") or {}
    number = issue.get("number") or event.get("issue_number")
    timestamp = event.get("created_at") or event.get("createdAt")
    actor = ((event.get("actor") or {}).get("login") or "unknown")
    kind = normalize_text(event.get("event") or event.get("type"))
    return (int(number) if str(number).isdigit() else None, f"{timestamp} {actor}: {kind}".strip())


def _labels_at_cutoff(
    record: Mapping[str, Any], events: Sequence[Mapping[str, Any]]
) -> list[str]:
    labels = {
        normalize_text(label.get("name"))
        for label in record.get("labels", [])
        if normalize_text(label.get("name"))
    }
    if at_or_before_cutoff(record.get("updated_at")):
        return sorted(labels)
    for event in sorted(
        events,
        key=lambda item: parse_timestamp(item.get("created_at")) or CUTOFF,
        reverse=True,
    ):
        timestamp = parse_timestamp(event.get("created_at"))
        if not timestamp or timestamp <= CUTOFF:
            continue
        label = normalize_text((event.get("label") or {}).get("name"))
        if event.get("event") == "labeled" and label:
            labels.discard(label)
        elif event.get("event") == "unlabeled" and label:
            labels.add(label)
    return sorted(labels)


def _milestone_at_cutoff(
    record: Mapping[str, Any], events: Sequence[Mapping[str, Any]]
) -> str:
    milestone = normalize_text((record.get("milestone") or {}).get("title"))
    if at_or_before_cutoff(record.get("updated_at")):
        return milestone
    for event in sorted(
        events,
        key=lambda item: parse_timestamp(item.get("created_at")) or CUTOFF,
        reverse=True,
    ):
        timestamp = parse_timestamp(event.get("created_at"))
        if not timestamp or timestamp <= CUTOFF:
            continue
        if event.get("event") == "milestoned":
            milestone = ""
        elif event.get("event") == "demilestoned":
            milestone = normalize_text((event.get("milestone") or {}).get("title"))
    return milestone


def _safe_cutoff_timestamp(value: Any) -> str:
    return normalize_text(value) if at_or_before_cutoff(value) else ""


def normalize_repository(output: Path, owner: str, repo: str) -> int:
    crawl_root = output / "provenance" / "github_crawl"
    repo_slug = f"{owner}/{repo}"
    graphql_root = crawl_root / "graphql" / repo_slug.replace("/", "__")
    graphql_records = read_json(graphql_root / "records.json", {}) or {}
    graphql_overflow = read_json(graphql_root / "overflow.json", {}) or {}
    issues: dict[int, dict[str, Any]] = {}
    for item in _read_pages(crawl_root, repo_slug, "issues"):
        if not isinstance(item, Mapping) or not str(item.get("number", "")).isdigit():
            continue
        if not at_or_before_cutoff(item.get("created_at")):
            continue
        issues[int(item["number"])] = dict(item)
    comments: dict[int, list[str]] = defaultdict(list)
    for item in _read_pages(crawl_root, repo_slug, "issue_comments"):
        url = normalize_text(item.get("issue_url"))
        number = url.rstrip("/").split("/")[-1]
        if number.isdigit() and at_or_before_cutoff(item.get("created_at")):
            comments[int(number)].append(_comment_text(item))
    review_comments: dict[int, list[str]] = defaultdict(list)
    for item in _read_pages(crawl_root, repo_slug, "review_comments"):
        url = normalize_text(item.get("pull_request_url"))
        number = url.rstrip("/").split("/")[-1]
        if number.isdigit() and at_or_before_cutoff(item.get("created_at")):
            review_comments[int(number)].append(_comment_text(item))
    events: dict[int, list[str]] = defaultdict(list)
    raw_events_by_number: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for item in _read_pages(crawl_root, repo_slug, "issue_events"):
        number, text = _event_text(item)
        if number is not None:
            raw_events_by_number[number].append(item)
            if at_or_before_cutoff(item.get("created_at")):
                events[number].append(text)
    releases = list(_read_pages(crawl_root, repo_slug, "releases"))
    output_count = 0
    for number, record in sorted(issues.items()):
        is_pr = bool(record.get("pull_request"))
        artifact_type = "pull_request" if is_pr else "issue"
        node = _graphql_for_node(
            crawl_root,
            repo_slug,
            normalize_text(record.get("node_id")),
            graphql_records,
            graphql_overflow,
        )
        body, body_status = _body_at_cutoff(record, node)
        raw_events = raw_events_by_number.get(number, [])
        reviews = [
            _review_text(item)
            for item in (node.get("reviews") or {}).get("nodes", [])
            if at_or_before_cutoff(item.get("submittedAt"))
        ]
        commits = [
            _commit_text(item)
            for item in (node.get("commits") or {}).get("nodes", [])
            if at_or_before_cutoff(((item.get("commit") or {}).get("committedDate")))
        ]
        state_at_cutoff = _state_at_cutoff(record, raw_events)
        files_status = "available"
        changed_files = [_changed_file_text(item) for item in (node.get("files") or {}).get("nodes", [])]
        if is_pr and (
            not at_or_before_cutoff(record.get("updated_at"))
            and not at_or_before_cutoff(record.get("merged_at"))
        ):
            changed_files = []
            files_status = "omitted-unrecoverable-post-cutoff-state"
        timeline = sorted(events.get(number, []) + review_comments.get(number, []))
        if comments.get(number):
            timeline.extend(comments[number])
            timeline.sort()
        title = (
            normalize_text(record.get("title"))
            if at_or_before_cutoff(record.get("updated_at"))
            else f"{artifact_type} #{number}"
        )
        document = {
            "title": title,
            "body": body,
            "conversation": comments.get(number, []),
            "reviews": reviews,
            "commit_summaries": commits,
            "changed_files": changed_files,
            "timeline": timeline,
            "labels": _labels_at_cutoff(record, raw_events),
            "milestone": _milestone_at_cutoff(record, raw_events),
            "repository": repo_slug,
            "owner": owner,
            "repo": repo,
            "artifact_type": artifact_type,
            "number": number,
            "state": state_at_cutoff,
            "state_at_cutoff": state_at_cutoff,
            "created_at": normalize_text(record.get("created_at")),
            "updated_at": _safe_cutoff_timestamp(record.get("updated_at")),
            "closed_at": _safe_cutoff_timestamp(record.get("closed_at")),
            "merged_at": _safe_cutoff_timestamp(record.get("merged_at")),
            "author": normalize_text((record.get("user") or {}).get("login")),
            "draft": bool(record.get("draft")) if is_pr else False,
            "comments_count": len(comments.get(number, [])),
            "review_comments_count": len(review_comments.get(number, [])),
            "url": normalize_text(record.get("html_url")),
            "source": "github_public",
            "provenance": [
                f"GitHub REST and GraphQL API; repository={repo_slug}",
                f"cutoff={CUTOFF.isoformat().replace('+00:00', 'Z')}",
                f"body_status={body_status}",
                f"changed_files_status={files_status}",
            ],
            "title_field_name": "title",
            "content_field_names": [
                "body",
                "conversation",
                "reviews",
                "commit_summaries",
                "changed_files",
                "timeline",
            ],
            "dataset_doc_uuid": deterministic_uuid(repo_slug, normalize_text(record.get("node_id"))),
        }
        folder = "pulls" if is_pr else "issues"
        path = output / "sources" / "github_public" / owner / repo / folder / f"{number}.json"
        atomic_write_json(path, document)
        output_count += 1
    for release in releases:
        if not at_or_before_cutoff(release.get("created_at")):
            continue
        release_id = normalize_text(release.get("id"))
        release_node_id = normalize_text(release.get("node_id") or f"release:{release_id}")
        release_body_safe = at_or_before_cutoff(release.get("updated_at") or release.get("published_at") or release.get("created_at"))
        body = normalize_text(release.get("body")) if release_body_safe else ""
        release_title_safe = at_or_before_cutoff(release.get("updated_at"))
        document = {
            "title": (
                normalize_text(release.get("name") or release.get("tag_name"))
                if release_title_safe
                else f"Release {release_id}"
            ),
            "body": body,
            "conversation": [],
            "reviews": [],
            "commit_summaries": [],
            "changed_files": [],
            "timeline": [f"{release.get('created_at')} published {release.get('tag_name', '')}".strip()],
            "labels": [],
            "milestone": "",
            "repository": repo_slug,
            "owner": owner,
            "repo": repo,
            "artifact_type": "release",
            "number": release_id,
            "state": "published",
            "state_at_cutoff": "published",
            "created_at": normalize_text(release.get("created_at")),
            "updated_at": _safe_cutoff_timestamp(release.get("updated_at")),
            "closed_at": "",
            "merged_at": "",
            "url": normalize_text(release.get("html_url")),
            "source": "github_public",
            "provenance": [
                f"GitHub REST API; cutoff={CUTOFF.isoformat().replace('+00:00', 'Z')}",
                f"body_status={'available' if release_body_safe else 'omitted-unrecoverable-post-cutoff-edit'}",
            ],
            "title_field_name": "title",
            "content_field_names": ["body", "timeline"],
            "dataset_doc_uuid": deterministic_uuid(repo_slug, release_node_id),
        }
        path = output / "sources" / "github_public" / owner / repo / "releases" / f"{release_id}.json"
        atomic_write_json(path, document)
        output_count += 1
    return output_count


def _copy_base(base: Path, output: Path) -> None:
    if output.exists():
        if not (output / "provenance" / "init_state.json").is_file():
            raise RuntimeError(f"Refusing to replace non-dataset directory: {output}")
        (output / "sources" / "github_public").mkdir(parents=True, exist_ok=True)
        (output / "provenance" / "github_crawl").mkdir(parents=True, exist_ok=True)
        # Re-running init must keep the derivative's deletion contract even if
        # a previous interrupted run left one of the replaced summaries behind.
        for relative in REMOVED_UPSTREAM_SUMMARIES:
            (output / relative).unlink(missing_ok=True)
        return
    free = shutil.disk_usage(output.parent).free
    if free < MIN_FREE_BYTES:
        raise RuntimeError(
            f"Need at least 15 GiB free for derivative staging; found {free / 1024**3:.1f} GiB."
        )
    staging = output.parent / f".{output.name}.staging"
    staging.mkdir(parents=True, exist_ok=True)
    copy_marker = staging / ".base-copy-complete"
    if not copy_marker.is_file():
        command = ["cp", "-a", "--reflink=auto", f"{base}/.", str(staging)]
        try:
            subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
        except (OSError, subprocess.CalledProcessError):
            shutil.copytree(base, staging, dirs_exist_ok=True, copy_function=shutil.copy2)
        copy_marker.touch()
    for relative in REMOVED_UPSTREAM_SUMMARIES:
        path = staging / relative
        if path.is_file():
            path.unlink()
    (staging / "sources" / "github_public").mkdir(parents=True, exist_ok=True)
    (staging / "provenance" / "github_crawl").mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        staging / "provenance" / "init_state.json",
        {"base": str(base), "cutoff": CUTOFF.isoformat().replace("+00:00", "Z"), "initialized_at": utc_now()},
    )
    copy_marker.unlink(missing_ok=True)
    os.replace(staging, output)


def build_lineage(base: Path, output: Path) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for source in sorted(path for path in base.rglob("*") if path.is_file()):
        relative = source.relative_to(base).as_posix()
        entries.append(
            {
                "path": relative,
                "size": source.stat().st_size,
                "sha256": sha256_file(source),
                "removed_from_derivative": relative in REMOVED_UPSTREAM_SUMMARIES,
            }
        )
    return {
        "base_root": str(base),
        "created_at": utc_now(),
        "file_count": len(entries),
        "entries": entries,
    }


def rebuild_indexes(output: Path) -> dict[str, Any]:
    uuid_path = output / "uuid_index.json"
    lineage = read_json(output / "provenance" / "base_lineage.json", {}) or {}
    base_root = Path(lineage.get("base_root", ""))
    base_index = read_json(base_root / "uuid_index.json", {}) if base_root else {}
    uuid_index = {
        str(document_id): str(relative)
        for document_id, relative in (base_index or {}).items()
    }
    if not uuid_index:
        uuid_index = read_json(uuid_path, {}) or {}
    public_documents = 0
    connector_counts: Counter[str] = Counter()
    for path in sorted((output / "sources").rglob("*.json")):
        relative = path.relative_to(output / "sources").as_posix()
        connector = relative.split("/", 1)[0]
        connector_counts[connector] += 1
        document = read_json(path, {}) or {}
        document_id = document.get("dataset_doc_uuid")
        if not document_id:
            continue
        if document_id in uuid_index:
            if (
                str(document_id).startswith(("gh_", "infra_"))
                and uuid_index[document_id] != relative
            ):
                raise ValueError(f"UUID collision: {document_id}")
        else:
            uuid_index[str(document_id)] = relative
        if connector == "github_public":
            public_documents += 1
    atomic_write_json(uuid_path, uuid_index)
    tree_text = _directory_tree(output / "sources") + "\n"
    atomic_write_bytes(output / "source_tree.txt", tree_text.encode("utf-8"))
    return {
        "uuid_count": len(uuid_index),
        "public_documents": public_documents,
        "connector_counts": dict(sorted(connector_counts.items())),
        "source_tree_entries": tree_text.count("\n"),
    }


def _directory_tree(base: Path) -> str:
    """Match EnterpriseRAG's directory-only source tree format."""

    def build(directory: Path, prefix: str = "") -> list[str]:
        children = sorted(path for path in directory.iterdir() if path.is_dir())
        lines: list[str] = []
        for index, child in enumerate(children):
            is_last = index == len(children) - 1
            branch = "└── " if is_last else "├── "
            lines.append(f"{prefix}{branch}{child.name}/")
            child_prefix = prefix + ("    " if is_last else "│   ")
            lines.extend(build(child, child_prefix))
        return lines

    if not base.is_dir():
        return f"{base.name}/ (not found)"
    lines = [base.name] + build(base)
    return "\n".join(lines) if len(lines) > 1 else f"{base.name}/ (empty)"


def _project_for(kind: str, index: int) -> tuple[str, str, str, tuple[int, ...]]:
    # Python hash randomization must not enter persisted dataset identity.
    stable_offset = sum(ord(char) for char in kind) % len(PROJECTS)
    return PROJECTS[(index + stable_offset) % len(PROJECTS)]


def _connector_workspace(output: Path, connector: str) -> str:
    candidates = {
        "slack": ("eng-runtime", "eng-platform", "eng-infra"),
        "linear": ("engineering", "design", "product-management"),
        "gmail": ("aditya_rao", "aisha_rahman", "alex_martinez"),
        "github": ("redwood",),
        "confluence": ("eng-infra", "eng-serving-runtime", "applied-ml-and-evals"),
        "google_drive": ("shared_drives/engineering", "shared_drives/company"),
        "jira": ("ops-requests", "internal-support"),
        "fireflies": ("misc", "all-hands"),
    }
    for candidate in candidates[connector]:
        if (output / "sources" / connector / candidate).is_dir():
            return candidate
    return ""


def _read_text_excerpt(path: Path, limit: int) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")[:limit].strip()


def _employee_names(output: Path) -> tuple[str, ...]:
    """Read real Redwood names from the preserved company scaffold."""

    path = output / "employee_directory.yaml"
    names: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if path.is_file() else ():
        match = re.match(r"^\s*-\s+name:\s*(.*)\s*$", line)
        if not match:
            continue
        value = match.group(1).strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if value:
            names.append(value)
    return tuple(dict.fromkeys(names)) or ("Redwood Infrastructure Team",)


def _generation_context(output: Path) -> dict[str, Any]:
    """Load compact, reusable context for the project-scaffolded generator."""

    rules: dict[str, str] = {}
    for connector in INTERNAL_COUNTS:
        rules[connector] = _read_text_excerpt(
            output / "sources" / connector / "agents.md", 5000
        )
    public_evidence: dict[str, dict[str, Any]] = {}
    for owner, repo, number in sorted(TARGET_ISSUES):
        for folder in ("issues", "pulls"):
            candidate = (
                output
                / "sources"
                / "github_public"
                / owner
                / repo
                / folder
                / f"{number}.json"
            )
            if not candidate.is_file():
                continue
            record = read_json(candidate, {}) or {}
            public_evidence[f"{owner}/{repo}#{number}"] = {
                "title": normalize_text(record.get("title")),
                "body": normalize_text(record.get("body"))[:1600],
                "timeline": [
                    normalize_text(item)
                    for item in (record.get("timeline") or [])[:5]
                ],
                "path": candidate.relative_to(output).as_posix(),
            }
            break
    return {
        "company_overview": _read_text_excerpt(
            output / "company_overview.md", 6000
        ),
        "initiatives": _read_text_excerpt(output / "initiatives.md", 9000),
        "project_list": _read_text_excerpt(output / "project_list.txt", 9000),
        "source_tree": _read_text_excerpt(output / "source_tree.txt", 5000),
        "employee_names": _employee_names(output),
        "connector_rules": rules,
        "public_evidence": public_evidence,
        "codebase_manifest": read_json(
            output / "codebases" / "manifest.json", {}
        ) or {},
    }


def _project_repository(
    project: tuple[str, str, str, tuple[int, ...]], index: int
) -> str:
    repositories = [item.strip() for item in project[2].split(";")]
    if len(repositories) > 1:
        issue = project[3][index % len(project[3])]
        issue_repository = {
            50026: "vllm-project/vllm",
            12736: "langfuse/langfuse",
            15208: "langfuse/langfuse",
        }.get(issue)
        if issue_repository in repositories:
            return issue_repository
    return repositories[index % len(repositories)]


def _project_context_excerpt(
    context: Mapping[str, Any], project: tuple[str, str, str, tuple[int, ...]]
) -> str:
    """Prefer scaffold lines related to this infrastructure project."""

    description_words = {
        word.casefold()
        for word in re.findall(r"[A-Za-z][A-Za-z0-9-]+", project[1])
        if len(word) >= 5
    }
    lines = str(context.get("project_list", "")).splitlines()
    selected = [
        line
        for line in lines
        if any(word in line.casefold() for word in description_words)
    ]
    if not selected:
        selected = lines[:12]
    return "\n".join(selected[:24])[:6000]


def _string_list(value: Any, fallback: Sequence[str] = ()) -> list[str]:
    if isinstance(value, list):
        return [normalize_text(item) for item in value if normalize_text(item)]
    if value not in (None, ""):
        return [normalize_text(value)]
    return list(fallback)


def _ensure_internal_contract(
    document: dict[str, Any],
    connector: str,
    index: int,
    project: tuple[str, str, str, tuple[int, ...]],
    workspace: str,
    context: Mapping[str, Any],
) -> dict[str, Any]:
    """Fill connector-native fields after either LLM or fallback generation."""

    project_id, description, _repositories, _issues = project
    repository = _project_repository(project, index)
    reference = _target_reference(project, index)
    names = tuple(context.get("employee_names") or ("Redwood Infrastructure Team",))
    owner = names[(index + sum(ord(char) for char in connector)) % len(names)]
    participants = [names[(index + offset) % len(names)] for offset in range(3)]
    day = dt.date(2026, 1, 5) + dt.timedelta(days=index * 3)
    created_at = day.isoformat()
    updated_at = (day + dt.timedelta(days=5 + index % 7)).isoformat()
    body = normalize_text(document.get("body")) or (
        f"Redwood Inference maintains {repository} as an upstream-first infrastructure dependency. "
        f"This record belongs to {project_id}: {description} Evidence: {reference}."
    )
    conversation = _string_list(
        document.get("conversation"),
        (
            f"{participants[0]}: reproduce the behavior against the pinned source tree and attach evidence.",
            f"{participants[1]}: separate upstream behavior from Redwood compatibility policy.",
            f"{participants[2]}: require rollback criteria before promotion.",
        ),
    )
    document.update(
        {
            "project": project_id,
            "repository": repository,
            "workspace": workspace,
            "artifact_type": "pull_request" if connector == "github" else connector,
            "state_at_cutoff": "active",
            "references": [reference],
            "provenance": [
                "Redwood internal synthetic record",
                "Project-scaffolded from the preserved company, source, and codebase context",
            ],
        }
    )
    if connector == "slack":
        thread_ts = str(1761955200 + index * 86400)
        document.pop("title", None)
        document.update(
            {
                "channel": workspace or "eng-runtime",
                "thread_ts": thread_ts,
                "first_message_ts": thread_ts,
                "last_message_ts": str(int(thread_ts) + 900),
                "participants": participants,
                "messages": _string_list(document.get("messages"), [body, *conversation]),
                "title_field_name": "channel",
                "content_field_names": ["messages"],
            }
        )
        document.pop("body", None)
        document.pop("title", None)
    elif connector == "linear":
        document.update(
            {
                "key": f"ENG-{90000 + index}",
                "team": workspace or "engineering",
                "title": normalize_text(document.get("title")) or f"Qualify {repository} for {project_id.replace('-', ' ')}",
                "status": "In Progress",
                "priority": "P1",
                "created_at": created_at,
                "updated_at": updated_at,
                "creator": participants[0],
                "assignee": owner,
                "description": normalize_text(document.get("description")) or body,
                "background": normalize_text(document.get("background")) or "Qualification must distinguish upstream behavior from Redwood deployment policy.",
                "objectives": _string_list(document.get("objectives"), ["Reproduce the target behavior", "Record compatibility and rollback evidence", "Prepare an upstreamable change or removal plan"]),
                "labels": ["infrastructure", "upstream-first", project_id],
                "title_field_name": "title",
                "content_field_names": ["description", "background", "objectives"],
            }
        )
    elif connector == "gmail":
        document.update(
            {
                "thread_id": f"thread-infra-{connector}-{index:03d}",
                "mailbox_owner": owner,
                "subject": normalize_text(document.get("subject")) or f"{repository} qualification decision for {project_id.replace('-', ' ')}",
                "participants_internal": participants,
                "participants_external": [],
                "message_count": 3,
                "first_email_at": f"{created_at}T09:15:00Z",
                "last_email_at": f"{updated_at}T16:30:00Z",
                "thread_type": "leadership_update",
                "messages": _string_list(document.get("messages"), [body, *conversation]),
                "title_field_name": "subject",
                "content_field_names": ["messages"],
            }
        )
        document.pop("body", None)
        document.pop("title", None)
    elif connector == "github":
        document.update(
            {
                "repo": "redwood",
                "pr_number": 90000 + index,
                "title": normalize_text(document.get("title")) or f"Add {project_id.replace('-', ' ')} qualification evidence",
                "author": owner,
                "created_at": created_at,
                "updated_at": updated_at,
                "state": "open" if index % 7 == 0 else "merged",
                "base_branch": "main",
                "head_branch": f"feature/{project_id}-{index:03d}",
                "reviewers": participants[1:],
                "labels": ["infrastructure", "upstream-first", project_id],
                "ci_status": "pass",
                "description": normalize_text(document.get("description")) or body,
                "commit_summaries": _string_list(document.get("commit_summaries"), ["Add qualification evidence and bounded downstream patch policy"]),
                "changed_files": _string_list(document.get("changed_files"), ["docs/upstream-qualification.md", "tests/qualification/test_dependency_contract.py"]),
                "conversation": conversation,
                "title_field_name": "title",
                "content_field_names": ["description", "commit_summaries", "changed_files", "conversation"],
            }
        )
    elif connector == "confluence":
        document.update(
            {
                "title": normalize_text(document.get("title")) or f"{repository} upstream qualification runbook: {project_id.replace('-', ' ')}",
                "space": workspace or "eng-infra",
                "author": owner,
                "owner_team": "engineering",
                "status": "published",
                "created_at": created_at,
                "last_updated": updated_at,
                "reviewers": participants[1:],
                "labels": ["upstream-first", "qualification", project_id],
                "content": normalize_text(document.get("content")) or f"Summary\n\n{body}\n\nProcedure\n\n1. Reproduce the behavior against the pinned source tree.\n2. Record compatibility constraints.\n3. Define contribution, rollback, and removal criteria.",
                "title_field_name": "title",
                "content_field_names": ["content"],
            }
        )
    elif connector == "google_drive":
        document.update(
            {
                "title": normalize_text(document.get("title")) or f"Working evidence: {project_id.replace('-', ' ')}",
                "owner": owner,
                "drive_area": "shared_drives",
                "path": f"shared_drives/engineering/{project_id}/{index:03d}",
                "doc_type": "doc",
                "created_at": created_at,
                "last_modified": updated_at,
                "collaborators": participants[1:],
                "team": "eng-serving-runtime" if repository == "vllm-project/vllm" else "eng-platform",
                "status": "in_review",
                "tags": ["qualification", "upstream-first", project_id],
                "linked_artifacts": [reference],
                "content": normalize_text(document.get("content")) or f"Working notes\n\n{body}\n\nOpen questions\n\n- What is the smallest upstreamable change?\n- What evidence is required before promotion?",
                "title_field_name": "title",
                "content_field_names": ["content"],
            }
        )
    elif connector == "jira":
        document.update(
            {
                "key": f"INT-{90000 + index}",
                "workflow_project": project_id,
                "project": "internal-support",
                "issue_type": "Incident",
                "summary": normalize_text(document.get("summary")) or f"Qualify {repository} behavior for {project_id.replace('-', ' ')}",
                "status": "In Progress",
                "priority": "P1",
                "created_at": created_at,
                "updated_at": updated_at,
                "reporter": participants[0],
                "assignee": owner,
                "severity": "Sev3",
                "components": ["serving-runtime" if repository == "vllm-project/vllm" else "observability"],
                "labels": ["upstream-first", "qualification"],
                "description": normalize_text(document.get("description")) or body,
                "notes": "Do not promote a downstream patch without rollback evidence and an upstream contribution or removal path.",
                "related_github_prs": [f"redwood#{90000 + index}"],
                "title_field_name": "summary",
                "content_field_names": ["description", "notes"],
            }
        )
    elif connector == "fireflies":
        document.update(
            {
                "meeting_id": f"ff-infra-{index:03d}",
                "recorded_at": f"{created_at}T15:00:00Z",
                "duration_minutes": 38 + index % 18,
                "call_type": "technical_deep_dive",
                "title": normalize_text(document.get("title")) or f"{repository} qualification review",
                "redwood_owner": owner,
                "redwood_attendees": participants,
                "customer_company": "Redwood Inference",
                "customer_attendees": [],
                "next_steps": ["Attach reproduction evidence", "Confirm rollback gate", "Decide contribution or removal path"],
                "transcription_quality": "high",
                "transcript": normalize_text(document.get("transcript")) or "\n".join([f"{participants[0]}: {body}", *conversation]),
                "title_field_name": "title",
                "content_field_names": ["transcript"],
            }
        )
    else:
        raise ValueError(f"Unsupported internal connector: {connector}")
    title_field = INTERNAL_TITLE_FIELDS.get(connector, "title")
    document["title_field_name"] = title_field
    if title_field == "title":
        document.setdefault(
            "title", f"{connector} infrastructure record {index:03d}"
        )
    else:
        document.pop("title", None)
    if connector in {"slack", "gmail"}:
        document.pop("body", None)
    return document


def _target_reference(project: tuple[str, str, str, tuple[int, ...]], index: int) -> str:
    repository = _project_repository(project, index)
    issue = project[3][index % len(project[3])]
    return f"{repository}#{issue} https://github.com/{repository}/issues/{issue}"


def _template_document(
    connector: str,
    index: int,
    project: tuple[str, str, str, tuple[int, ...]],
    workspace: str,
    context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    context = context or {}
    project_id, description, _repositories, _issues = project
    repository = _project_repository(project, index)
    reference = _target_reference(project, index)
    names = tuple(context.get("employee_names") or ("Redwood Infrastructure Team",))
    owner = names[(index + sum(ord(char) for char in connector)) % len(names)]
    participants = [names[(index + offset) % len(names)] for offset in range(3)]
    day = dt.date(2026, 1, 5) + dt.timedelta(days=index * 3)
    created_at = day.isoformat()
    updated_at = (day + dt.timedelta(days=5 + index % 7)).isoformat()
    issue = project[3][index % len(project[3])]
    evidence_key = f"{repository}#{issue}"
    public_evidence = (context.get("public_evidence") or {}).get(evidence_key, {})
    project_context = normalize_text(_project_context_excerpt(context, project))[:900]
    company_context = normalize_text(str(context.get("company_overview", "")))[:700]
    connector_context = normalize_text(
        str((context.get("connector_rules") or {}).get(connector, ""))
    )[:600]
    pinned_commit = PINNED_COMMITS.get(repository, "unknown")
    evidence_context = (
        f"Public record excerpt: {public_evidence.get('title', '')}. "
        f"{normalize_text(public_evidence.get('body'))[:900]}"
        if public_evidence
        else "The authenticated public-history record is pending crawl; use the linked target as the evidence boundary."
    )
    body = (
        f"Redwood Inference maintains {repository} as an upstream-first infrastructure dependency. "
        f"This record belongs to {project_id}: {description} "
        f"The working evidence is {reference}. {owner} qualifies the pinned upstream behavior, "
        "uses a temporary downstream patch only when an operational boundary requires it, "
        "and records contribution, removal, and rollback criteria before rollout. "
        f"Company context: {company_context} Initiative context: {project_context} "
        f"Connector guidance: {connector_context} Pinned source: {repository}@{pinned_commit}. "
        f"{evidence_context}"
    )
    conversation = [
        f"{participants[0]}: reproduce the behavior against the pinned source tree and attach a minimal evidence artifact.",
        f"{participants[1]}: separate upstream behavior, Redwood compatibility policy, and incident mitigation in the decision.",
        f"{participants[2]}: require rollback criteria and an upstream issue or contribution link before promotion.",
    ]
    common = {
        "project": project_id,
        "repository": repository,
        "artifact_type": connector,
        "state_at_cutoff": "active",
        "workspace": workspace,
        "references": [reference],
        "grounding_sources": [
            "company_overview.md",
            "initiatives.md",
            "employee_directory.yaml",
            f"sources/{connector}/agents.md",
            f"codebases/manifest.json:{repository}@{pinned_commit}",
            public_evidence.get("path", f"github_public/{repository}#{issue}"),
        ],
        "provenance": [
            "Redwood internal synthetic record",
            "Project-scaffolded from the preserved company, source, and codebase context",
        ],
    }
    if connector == "slack":
        thread_ts = str(1761955200 + index * 86400)
        return {
            **common,
            "channel": workspace or "eng-runtime",
            "thread_ts": thread_ts,
            "first_message_ts": thread_ts,
            "last_message_ts": str(int(thread_ts) + 900),
            "participants": participants,
            "messages": [body, *conversation],
            "title_field_name": "channel",
            "content_field_names": ["messages"],
        }
    if connector == "linear":
        return {
            **common,
            "key": f"ENG-{90000 + index}",
            "team": workspace or "engineering",
            "title": f"Qualify {repository} for {project_id.replace('-', ' ')}",
            "status": "In Progress",
            "priority": "P1",
            "created_at": created_at,
            "updated_at": updated_at,
            "creator": participants[0],
            "assignee": owner,
            "description": body,
            "background": "Qualification must distinguish upstream behavior from Redwood deployment policy before promotion.",
            "objectives": ["Reproduce the target behavior", "Record compatibility and rollback evidence", "Prepare an upstreamable change or removal plan"],
            "labels": ["infrastructure", "upstream-first", project_id],
            "title_field_name": "title",
            "content_field_names": ["description", "background", "objectives"],
        }
    if connector == "gmail":
        return {
            **common,
            "thread_id": f"thread-infra-{connector}-{index:03d}",
            "mailbox_owner": owner,
            "subject": f"{repository} qualification decision for {project_id.replace('-', ' ')}",
            "participants_internal": participants,
            "participants_external": [],
            "message_count": 3,
            "first_email_at": f"{created_at}T09:15:00Z",
            "last_email_at": f"{updated_at}T16:30:00Z",
            "thread_type": "leadership_update",
            "messages": [body, *conversation],
            "title_field_name": "subject",
            "content_field_names": ["messages"],
        }
    if connector == "github":
        pr_number = 90000 + index
        return {
            **common,
            "repo": "redwood",
            "pr_number": pr_number,
            "title": f"Add {project_id.replace('-', ' ')} qualification evidence",
            "author": owner,
            "created_at": created_at,
            "updated_at": updated_at,
            "state": "open" if index % 7 == 0 else "merged",
            "base_branch": "main",
            "head_branch": f"feature/{project_id}-{index:03d}",
            "reviewers": participants[1:],
            "labels": ["infrastructure", "upstream-first", project_id],
            "ci_status": "pass",
            "description": body,
            "commit_summaries": ["Add qualification evidence and bounded downstream patch policy"],
            "changed_files": ["docs/upstream-qualification.md", "tests/qualification/test_dependency_contract.py"],
            "conversation": conversation,
            "title_field_name": "title",
            "content_field_names": ["description", "commit_summaries", "changed_files", "conversation"],
        }
    if connector == "confluence":
        return {
            **common,
            "title": f"{repository} upstream qualification runbook: {project_id.replace('-', ' ')}",
            "space": workspace or "eng-infra",
            "author": owner,
            "owner_team": "engineering",
            "status": "published",
            "created_at": created_at,
            "last_updated": updated_at,
            "reviewers": participants[1:],
            "labels": ["upstream-first", "qualification", project_id],
            "content": f"Summary\n\n{body}\n\nProcedure\n\n1. Reproduce the behavior against the pinned source tree.\n2. Record operational impact and compatibility constraints.\n3. Define contribution, rollback, and removal criteria.\n\nOwners\n\n{', '.join(participants)}.",
            "title_field_name": "title",
            "content_field_names": ["content"],
        }
    if connector == "google_drive":
        drive_area = "shared_drives"
        path = f"shared_drives/engineering/{project_id}/{index:03d}"
        return {
            **common,
            "title": f"Working evidence: {project_id.replace('-', ' ')}",
            "owner": owner,
            "drive_area": drive_area,
            "path": path,
            "doc_type": "doc",
            "created_at": created_at,
            "last_modified": updated_at,
            "collaborators": participants[1:],
            "team": "eng-serving-runtime" if repository == "vllm-project/vllm" else "eng-platform",
            "status": "in_review",
            "tags": ["qualification", "upstream-first", project_id],
            "linked_artifacts": [reference],
            "content": f"Working notes\n\n{body}\n\nOpen questions\n\n- What is the smallest upstreamable change?\n- What evidence is required before promotion?\n- When can a temporary downstream patch be removed?",
            "title_field_name": "title",
            "content_field_names": ["content"],
        }
    if connector == "jira":
        return {
            **common,
            "key": f"INT-{90000 + index}",
            "project": "internal-support",
            "issue_type": "Incident",
            "summary": f"Qualify {repository} behavior for {project_id.replace('-', ' ')}",
            "status": "In Progress",
            "priority": "P1",
            "created_at": created_at,
            "updated_at": updated_at,
            "reporter": participants[0],
            "assignee": owner,
            "severity": "Sev3",
            "components": ["serving-runtime" if repository == "vllm-project/vllm" else "observability"],
            "labels": ["upstream-first", "qualification"],
            "description": body,
            "notes": "Do not promote a downstream patch without rollback evidence and an upstream contribution or removal path.",
            "related_github_prs": [f"redwood#{90000 + index}"],
            "title_field_name": "summary",
            "content_field_names": ["description", "notes"],
        }
    if connector == "fireflies":
        return {
            **common,
            "meeting_id": f"ff-infra-{index:03d}",
            "recorded_at": f"{created_at}T15:00:00Z",
            "duration_minutes": 38 + index % 18,
            "call_type": "technical_deep_dive",
            "title": f"{repository} qualification review",
            "redwood_owner": owner,
            "redwood_attendees": participants,
            "customer_company": "Redwood Inference",
            "customer_attendees": [],
            "next_steps": ["Attach reproduction evidence", "Confirm rollback gate", "Decide contribution or removal path"],
            "transcription_quality": "high",
            "transcript": "\n".join([f"{participants[0]}: {body}", *conversation]),
            "title_field_name": "title",
            "content_field_names": ["transcript"],
        }
    raise ValueError(f"Unsupported internal connector: {connector}")


def _llm_document(
    output: Path,
    connector: str,
    index: int,
    project: tuple[str, str, str, tuple[int, ...]],
    workspace: str,
    context: Mapping[str, Any],
) -> dict[str, Any]:
    from src.llm import Message, get_cheap_llm, get_llm, run_auto_conversation
    from src.tools.runner import ToolRunner
    from src.utils import extract_json_from_response, validate_no_nested_dicts

    project_id, description, repository, issues = project
    evidence: list[str] = []
    for issue_index, issue in enumerate(issues):
        owner_repo = _project_repository(project, issue_index)
        for folder in ("issues", "pulls"):
            candidate = output / "sources" / "github_public" / owner_repo.split("/", 1)[0] / owner_repo.split("/", 1)[1] / folder / f"{issue}.json"
            if candidate.is_file():
                public = read_json(candidate, {}) or {}
                evidence.append(json.dumps({"title": public.get("title"), "body": normalize_text(public.get("body"))[:1200], "timeline": public.get("timeline", [])[:4]}, ensure_ascii=True))
                break
    model_kind = connector in {"confluence", "google_drive", "fireflies"} or index % 5 == 0
    llm = get_llm(quiet=True, reasoning_level="low") if model_kind else get_cheap_llm(quiet=True, reasoning_level="low")
    scaffold_excerpt = _project_context_excerpt(context, project)
    connector_rules = str((context.get("connector_rules") or {}).get(connector, ""))[:5000]
    title_field = INTERNAL_TITLE_FIELDS.get(connector, "title")
    prompt = f"""
Create one coherent JSON document for a Redwood enterprise infrastructure workflow corpus.
Artifact connector: {connector}; workspace: {workspace}; sequence: {index}.
Project: {project_id}. Project description: {description}.
Upstream dependency: {repository}. Related pre-cutoff issue numbers: {list(issues)}.
Public pre-cutoff evidence excerpts: {evidence or ['The authenticated public-history crawl has not been normalized yet.']}
Preserved company scaffold excerpt:
{context.get('company_overview', '')[:2500]}
Related company initiatives:
{context.get('initiatives', '')[:3000]}
Available Redwood participants:
{', '.join(tuple(context.get('employee_names') or ())[:40])}
Related initiative/project lines:
{scaffold_excerpt}
Pinned source metadata:
{json.dumps(context.get('codebase_manifest', {}), ensure_ascii=True)[:4000]}
Connector schema and writing rules:
{connector_rules}
Redwood is upstream-first: qualify upstream behavior, use temporary downstream patches only
when necessary, and state contribution/removal/rollback criteria. Do not invent a post-cutoff
fix or claim a GitHub event that is not supplied. Return a flat JSON object with string or list
of string values only. The canonical title field for this connector is {title_field}; include
that field and use it in title_field_name. Include the connector's content fields, references,
repository, artifact_type, project, and state_at_cutoff.
""".strip()
    response = run_auto_conversation(
        llm,
        ToolRunner(),
        [Message(role="system", content="Return only valid JSON."), Message(role="user", content=prompt)],
        quiet=True,
    )
    parsed = json.loads(extract_json_from_response(response))
    error = validate_no_nested_dicts(parsed)
    if error:
        raise ValueError(f"LLM generated nested data for {connector}/{index}: {error}")
    return parsed


def generate_internal(output: Path, template_fallback: bool = False) -> dict[str, Any]:
    generated: list[dict[str, Any]] = []
    models: Counter[str] = Counter()
    context = _generation_context(output)
    for connector, count in INTERNAL_COUNTS.items():
        workspace = _connector_workspace(output, connector)
        for index in range(count):
            project = _project_for(connector, index)
            try:
                document = _template_document(connector, index, project, workspace, context) if template_fallback else _llm_document(output, connector, index, project, workspace, context)
                models["template-fallback" if template_fallback else ("primary" if connector in {"confluence", "google_drive", "fireflies"} or index % 5 == 0 else "cheap")] += 1
            except Exception:
                if not template_fallback:
                    raise
                document = _template_document(connector, index, project, workspace, context)
                models["template-fallback"] += 1
            document = _ensure_internal_contract(
                document, connector, index, project, workspace, context
            )
            project_id = str(
                document.get("workflow_project", document.get("project", "unknown"))
            )
            document["dataset_doc_uuid"] = (
                "infra_"
                + hashlib.sha256(
                    f"{connector}:{project_id}:{index}".encode()
                ).hexdigest()[:32]
            )
            path = output / "sources" / connector / workspace / f"infra-v1-{connector}-{index:03d}.json"
            atomic_write_json(path, document)
            generated.append({"connector": connector, "project": document.get("workflow_project", document.get("project")), "path": path.relative_to(output).as_posix(), "uuid": document["dataset_doc_uuid"]})
    manifest = {
        "count": len(generated),
        "expected_count": sum(INTERNAL_COUNTS.values()),
        "counts_by_connector": dict(INTERNAL_COUNTS),
        "models": dict(models),
        "cutoff": CUTOFF.isoformat().replace("+00:00", "Z"),
        "documents": generated,
    }
    atomic_write_json(output / "provenance" / "internal_generation_manifest.json", manifest)
    project_manifest: dict[str, Any] = {}
    for project_id, description, repository, issues in PROJECTS:
        project_manifest[project_id] = {
            "description": description,
            "repositories": [item.strip() for item in repository.split(";")],
            "target_issues": list(issues),
            "document_count": sum(1 for item in generated if item.get("project") == project_id),
            "method": "project-scaffolded; primary model for durable artifacts and cheap model for routine records" if not template_fallback else "project-scaffolded deterministic offline fallback",
        }
    atomic_write_json(output / "provenance" / "internal_generation_projects.json", project_manifest)
    rebuild_indexes(output)
    return manifest


def init_dataset(base: Path, output: Path) -> None:
    _copy_base(base, output)
    lineage_path = output / "provenance" / "base_lineage.json"
    if not lineage_path.is_file():
        atomic_write_json(lineage_path, build_lineage(base, output))
    rebuild_indexes(output)
    atomic_write_json(
        output / "provenance" / "dataset_state.json",
        {"phase": "init", "base": str(base), "cutoff": CUTOFF.isoformat().replace("+00:00", "Z"), "updated_at": utc_now()},
    )


def _verify_lineage(output: Path) -> tuple[int, list[str]]:
    lineage = read_json(output / "provenance" / "base_lineage.json", {}) or {}
    mismatches: list[str] = []
    checked = 0
    base = Path(lineage.get("base_root", ""))
    for entry in lineage.get("entries", []):
        relative = entry["path"]
        if entry.get("removed_from_derivative") or relative in DERIVED_INDEX_FILES:
            continue
        path = output / relative
        if not path.is_file() or path.stat().st_size != entry.get("size") or sha256_file(path) != entry.get("sha256"):
            mismatches.append(relative)
        checked += 1
    if base and not base.exists():
        mismatches.append(f"missing-base:{base}")
    return checked, mismatches


def validate_dataset(output: Path, allow_incomplete: bool = False) -> dict[str, Any]:
    if not output.is_dir():
        raise FileNotFoundError(output)
    checked, lineage_mismatches = _verify_lineage(output)
    errors = [f"base-lineage:{path}" for path in lineage_mismatches]
    lineage = read_json(output / "provenance" / "base_lineage.json", {}) or {}
    base_root = Path(lineage.get("base_root", ""))
    base_uuid_index = read_json(base_root / "uuid_index.json", {}) if base_root else {}
    derivative_uuid_index = read_json(output / "uuid_index.json", {}) or {}
    for document_id, relative in (base_uuid_index or {}).items():
        if derivative_uuid_index.get(document_id) != relative:
            errors.append(f"original-uuid-changed:{document_id}")
    uuids: dict[str, str] = {}
    public_count = 0
    internal_count_actual = 0
    internal_connector_counts: Counter[str] = Counter()
    connector_counts: Counter[str] = Counter()
    employee_names = set(_employee_names(output))
    generated_internal: list[tuple[str, Path, Mapping[str, Any]]] = []
    target_linked_internal: list[str] = []
    public_target_comment_checks: list[dict[str, Any]] = []
    missing_cross_references: list[str] = []
    for path in sorted((output / "sources").rglob("*.json")):
        relative = path.relative_to(output).as_posix()
        connector = relative.split("/")[1] if relative.startswith("sources/") else "unknown"
        connector_counts[connector] += 1
        document = read_json(path, None)
        if not isinstance(document, Mapping):
            errors.append(f"invalid-json:{relative}")
            continue
        is_public = connector == "github_public"
        is_generated_internal = path.name.startswith("infra-v1-")
        if is_public:
            required_fields = (
                "title",
                "title_field_name",
                "content_field_names",
                "dataset_doc_uuid",
            )
        elif is_generated_internal:
            required_fields = (
                "title_field_name",
                "content_field_names",
                "dataset_doc_uuid",
            )
        else:
            required_fields = ()
        for field in required_fields:
            if field not in document:
                errors.append(f"missing-{field}:{relative}")
        if is_generated_internal:
            internal_connector_counts[connector] += 1
            generated_internal.append((relative, path, document))
            expected_title_field = INTERNAL_TITLE_FIELDS.get(connector, "title")
            if document.get("title_field_name") != expected_title_field:
                errors.append(
                    f"invalid-title-field:{relative}:{document.get('title_field_name')}"
                )
            if expected_title_field not in document:
                errors.append(f"missing-title-value:{relative}:{expected_title_field}")
        document_id = str(document.get("dataset_doc_uuid", ""))
        if (
            document_id.startswith(("gh_", "infra_"))
            and document_id in uuids
            and uuids[document_id] != relative
        ):
            errors.append(f"duplicate-uuid:{document_id}")
        if document_id:
            uuids[document_id] = relative
        if connector == "github_public":
            public_count += 1
            for field in ("repository", "artifact_type", "number", "state_at_cutoff"):
                if field not in document:
                    errors.append(f"missing-public-{field}:{relative}")
            for field in ("created_at", "updated_at", "closed_at", "merged_at"):
                value = document.get(field)
                if value and parse_timestamp(value) and parse_timestamp(value) > CUTOFF:
                    errors.append(f"post-cutoff-{field}:{relative}")
        if path.name.startswith("infra-v1-"):
            internal_count_actual += 1
        if (is_public or is_generated_internal) and (
            not isinstance(document.get("content_field_names"), list)
            or not all(
            isinstance(field, str) for field in document["content_field_names"]
            )
        ):
            errors.append(f"invalid-content-fields:{relative}")
        if is_public or is_generated_internal:
            if not normalize_text(document.get("dataset_doc_uuid")):
                errors.append(f"empty-dataset-doc-uuid:{relative}")
            for field in document.get("content_field_names", []):
                if field not in document:
                    errors.append(f"missing-content-field:{relative}:{field}")
            title_field = document.get("title_field_name")
            if title_field and not normalize_text(document.get(title_field)):
                errors.append(f"empty-title-value:{relative}:{title_field}")
        if is_generated_internal:
            repository = normalize_text(document.get("repository"))
            if repository not in {"vllm-project/vllm", "langfuse/langfuse"}:
                errors.append(f"invalid-internal-repository:{relative}:{repository}")
            references = document.get("references")
            if not isinstance(references, list) or not references:
                errors.append(f"missing-internal-references:{relative}")
            elif not any("github.com/" in normalize_text(item) for item in references):
                errors.append(f"missing-github-reference:{relative}")
            valid_reference = any(
                re.search(
                    r"(?:vllm-project/vllm|langfuse/langfuse)#(?:50026|49980|34752|48627|12736|15208)\b",
                    normalize_text(item),
                )
                for item in references or []
            )
            if not valid_reference:
                errors.append(f"invalid-internal-issue-reference:{relative}")
            else:
                target_linked_internal.append(relative)
                for reference in references:
                    match = re.search(
                        r"(vllm-project/vllm|langfuse/langfuse)#(50026|49980|34752|48627|12736|15208)\b",
                        normalize_text(reference),
                    )
                    if not match:
                        continue
                    reference_owner, reference_repo = match.group(1).split("/", 1)
                    reference_number = match.group(2)
                    issue_path = (
                        output
                        / "sources"
                        / "github_public"
                        / reference_owner
                        / reference_repo
                        / "issues"
                        / f"{reference_number}.json"
                    )
                    pull_path = issue_path.parent.parent / "pulls" / f"{reference_number}.json"
                    if not issue_path.is_file() and not pull_path.is_file():
                        missing_cross_references.append(
                            f"{relative}:{match.group(1)}#{reference_number}"
                        )
            for field in (
                "created_at",
                "updated_at",
                "last_updated",
                "last_modified",
                "recorded_at",
                "first_email_at",
                "last_email_at",
            ):
                value = document.get(field)
                if not value:
                    continue
                parsed = parse_timestamp(value)
                if parsed is None:
                    errors.append(f"invalid-internal-date:{relative}:{field}")
                elif parsed > CUTOFF:
                    errors.append(f"post-cutoff-internal-date:{relative}:{field}")
            if (
                parse_timestamp(document.get("created_at"))
                and parse_timestamp(document.get("updated_at"))
                and parse_timestamp(document.get("updated_at"))
                < parse_timestamp(document.get("created_at"))
            ):
                errors.append(f"internal-date-order:{relative}")
            person_fields = (
                "assignee",
                "creator",
                "reporter",
                "owner",
                "redwood_owner",
                "mailbox_owner",
                "author",
            )
            for field in person_fields:
                value = normalize_text(document.get(field))
                if value and value not in employee_names:
                    errors.append(f"unknown-internal-person:{relative}:{field}")
            person_list_fields = (
                "participants",
                "participants_internal",
                "redwood_attendees",
                "reviewers",
                "collaborators",
            )
            for field in person_list_fields:
                values = document.get(field) or []
                if not isinstance(values, list):
                    errors.append(f"invalid-internal-people-list:{relative}:{field}")
                    continue
                for value in values:
                    if normalize_text(value) not in employee_names:
                        errors.append(f"unknown-internal-person:{relative}:{field}")
    for relative in REMOVED_UPSTREAM_SUMMARIES:
        if (output / relative).exists():
            errors.append(f"removed-summary-still-present:{relative}")
    internal_manifest = read_json(output / "provenance" / "internal_generation_manifest.json", {}) or {}
    internal_count = int(internal_manifest.get("count", 0))
    if internal_count_actual != internal_count:
        errors.append(f"internal-files:{internal_count_actual}/{internal_count}")
    if dict(sorted(internal_connector_counts.items())) != dict(sorted(INTERNAL_COUNTS.items())):
        errors.append(
            "internal-connector-counts:"
            + json.dumps(dict(sorted(internal_connector_counts.items())), sort_keys=True)
        )
    if internal_count != sum(INTERNAL_COUNTS.values()):
        errors.append(f"internal-count:{internal_count}/{sum(INTERNAL_COUNTS.values())}")
    missing_targets = []
    for owner, repo, number in TARGET_ISSUES:
        path = output / "sources" / "github_public" / owner / repo / "issues" / f"{number}.json"
        if not path.is_file():
            path = output / "sources" / "github_public" / owner / repo / "pulls" / f"{number}.json"
        if not path.is_file():
            missing_targets.append(f"{owner}/{repo}#{number}")
            continue
        document = read_json(path, {}) or {}
        crawl_root = output / "provenance" / "github_crawl"
        expected_comments: list[str] = []
        expected_review_comments: list[str] = []
        for item in _read_pages(crawl_root, f"{owner}/{repo}", "issue_comments"):
            item_url = normalize_text(item.get("issue_url"))
            if item_url.rstrip("/").endswith(f"/{number}") and at_or_before_cutoff(item.get("created_at")):
                expected_comments.append(_comment_text(item))
        for item in _read_pages(crawl_root, f"{owner}/{repo}", "review_comments"):
            item_url = normalize_text(item.get("pull_request_url"))
            if item_url.rstrip("/").endswith(f"/{number}") and at_or_before_cutoff(item.get("created_at")):
                expected_review_comments.append(_comment_text(item))
        actual_comments = document.get("conversation") or []
        actual_timeline = document.get("timeline") or []
        if expected_comments and (
            len(actual_comments) != len(expected_comments)
            or any(item not in actual_comments for item in expected_comments)
        ):
            errors.append(f"target-comments-incomplete:{owner}/{repo}#{number}")
        if expected_review_comments and (
            int(document.get("review_comments_count", 0)) != len(expected_review_comments)
            or any(item not in actual_timeline for item in expected_review_comments)
        ):
            errors.append(f"target-review-comments-incomplete:{owner}/{repo}#{number}")
        public_target_comment_checks.append(
            {
                "target": f"{owner}/{repo}#{number}",
                "document": path.relative_to(output).as_posix(),
                "comments_expected": len(expected_comments),
                "comments_indexed": len(actual_comments),
                "review_comments_expected": len(expected_review_comments),
                "review_comments_indexed": int(document.get("review_comments_count", 0)),
            }
        )
    incomplete: list[str] = []
    if missing_targets:
        incomplete.append("missing-targets:" + ",".join(sorted(missing_targets)))
    if missing_targets and not allow_incomplete:
        errors.append("missing-targets:" + ",".join(sorted(missing_targets)))
    if missing_cross_references and not allow_incomplete:
        errors.append(
            "missing-cross-references:" + ",".join(sorted(missing_cross_references))
        )
    if missing_cross_references:
        incomplete.append(
            "missing-cross-references:" + ",".join(sorted(missing_cross_references))
        )
    coherence_sample: list[str] = []
    sample_size = max(1, (len(generated_internal) + 9) // 10)
    sampled_internal = sorted(
        generated_internal,
        key=lambda item: hashlib.sha256(item[0].encode("utf-8")).digest(),
    )[:sample_size]
    for relative, _path, document in sampled_internal:
        coherence_sample.append(relative)
        text = json.dumps(document, ensure_ascii=True).casefold()
        repository = normalize_text(document.get("repository")).casefold()
        project = normalize_text(
            document.get("workflow_project") or document.get("project")
        ).casefold()
        if repository not in text or project not in text or "upstream" not in text:
            errors.append(f"internal-coherence-sample:{relative}")
    for relative, _path, document in generated_internal:
        if relative not in target_linked_internal:
            continue
        text = json.dumps(document, ensure_ascii=True).casefold()
        if "rollback" not in text or "contribution" not in text:
            errors.append(f"target-internal-coherence:{relative}")
    result = {
        "valid": not errors,
        "errors": errors,
        "incomplete": incomplete,
        "lineage_files_checked": checked,
        "public_documents": public_count,
        "internal_documents": internal_count,
        "internal_documents_on_disk": internal_count_actual,
        "connector_counts": dict(sorted(connector_counts.items())),
        "uuid_count": len(uuids),
        "cutoff": CUTOFF.isoformat().replace("+00:00", "Z"),
        "internal_connector_counts": dict(sorted(internal_connector_counts.items())),
        "internal_validation": {
            "employee_names_checked": len(employee_names),
            "target_linked_documents_checked": len(target_linked_internal),
            "deterministic_sample_fraction": 0.10,
            "deterministic_sample_count": len(coherence_sample),
            "deterministic_sample_documents": coherence_sample,
        },
        "target_comment_checks": public_target_comment_checks,
        "validated_at": utc_now(),
    }
    atomic_write_json(output / "provenance" / "validation.json", result)
    if errors and not allow_incomplete:
        raise RuntimeError("Dataset validation failed: " + "; ".join(errors[:12]))
    return result


def write_manifest(output: Path, phase: str) -> dict[str, Any]:
    indexes = rebuild_indexes(output)
    lineage = read_json(output / "provenance" / "base_lineage.json", {}) or {}
    internal_generation = read_json(
        output / "provenance" / "internal_generation_manifest.json", {}
    ) or {}
    lineage_entries = lineage.get("entries", [])
    lineage_digest = hashlib.sha256(
        "".join(
            f"{entry.get('path')}:{entry.get('size')}:{entry.get('sha256')}\n"
            for entry in lineage_entries
        ).encode("utf-8")
    ).hexdigest()
    crawl_metrics = read_json(
        output / "provenance" / "github_crawl" / "metrics.json", {}
    ) or {}
    validation = read_json(output / "provenance" / "validation.json", {}) or {}
    crawl_efficiency = {
        "requests": sum(int(value.get("requests", 0)) for value in crawl_metrics.values()),
        "objects": sum(int(value.get("objects", 0)) for value in crawl_metrics.values()),
        "bytes": sum(int(value.get("bytes", 0)) for value in crawl_metrics.values()),
        "cache_hits": sum(int(value.get("cache_hits", 0)) for value in crawl_metrics.values()),
        "api_cost": sum(int(value.get("api_cost", 0)) for value in crawl_metrics.values()),
        "overflow_calls": sum(int(value.get("overflow_calls", 0)) for value in crawl_metrics.values()),
        "by_endpoint": crawl_metrics,
    }
    manifest = {
        "dataset": "EnterpriseRAG-Bench/generated_data_infra_v1",
        "derivative_of": "EnterpriseRAG-Bench/generated_data",
        "phase": phase,
        "cutoff": CUTOFF.isoformat().replace("+00:00", "Z"),
        "pinned_commits": PINNED_COMMITS,
        "removed_handwritten_summaries": list(REMOVED_UPSTREAM_SUMMARIES),
        "public_repositories": [f"{owner}/{repo}" for owner, repo in PUBLIC_REPOSITORIES],
        "internal_counts": INTERNAL_COUNTS,
        "artifact_counts": {
            "connector_documents": indexes.get("connector_counts", {}),
            "public_github_documents": indexes.get("public_documents", 0),
            "internal_documents": internal_generation.get("count", 0),
            "total_source_tree_entries": indexes.get("source_tree_entries", 0),
        },
        "base_hashes": {
            "lineage_manifest": "provenance/base_lineage.json",
            "lineage_file_count": len(lineage_entries),
            "lineage_digest": lineage_digest,
        },
        "indexes": indexes,
        "crawl_metrics": crawl_metrics,
        "crawl_efficiency": crawl_efficiency,
        "validation": {
            "valid": validation.get("valid"),
            "errors": validation.get("errors", []),
            "incomplete": validation.get("incomplete", []),
            "lineage_files_checked": validation.get("lineage_files_checked", 0),
            "target_comment_checks": validation.get("target_comment_checks", []),
            "internal_validation": validation.get("internal_validation", {}),
        },
        "omitted_or_unrecoverable_fields": [
            "body when GitHub edit history cannot reconstruct a post-cutoff edit",
            "changed_files for open PRs whose post-cutoff state cannot be reconstructed",
            "full patches are never indexed",
        ],
        "license_and_provenance": [
            "Public GitHub metadata is retained for research provenance under the repositories' public terms.",
            "Redwood records are synthetic derivatives of the preserved EnterpriseRAG-Bench company scaffold.",
            "provenance/github_crawl is archival metadata and is excluded from corpus indexing.",
        ],
        "generation": {
            "models": internal_generation.get("models", {}),
            "prompt_version": "infra-v1-project-scaffolded",
            "prompt_inputs": [
                "company_overview.md",
                "initiatives.md",
                "project_list.txt",
                "employee_directory.yaml",
                "sources/*/agents.md",
                "codebases/manifest.json",
                "cutoff-filtered github_public records",
            ],
            "prompt_policy": [
                "Primary model: project manifests, ADRs, design reviews, postmortems, and meetings.",
                "Cheap model: routine threads/tickets and first-pass validation.",
                "The generation manifest records whether the run used configured models or the deterministic offline fallback; no credentials are persisted.",
            ],
        },
        "generated_at": utc_now(),
    }
    atomic_write_json(output / "manifest.json", manifest)
    return manifest


def phase_init(args: argparse.Namespace) -> None:
    init_dataset(Path(args.base).resolve(), Path(args.output).resolve())
    write_manifest(Path(args.output).resolve(), "init")


def phase_crawl(args: argparse.Namespace) -> None:
    output = Path(args.output).resolve()
    repositories = PUBLIC_REPOSITORIES
    if args.repository:
        owner, repo = args.repository.split("/", 1)
        repositories = ((owner, repo),)
    for owner, repo in repositories:
        crawl_repository(output, owner, repo)
    atomic_write_json(output / "provenance" / "dataset_state.json", {"phase": "crawl", "updated_at": utc_now(), "cutoff": CUTOFF.isoformat().replace("+00:00", "Z")})
    write_manifest(output, "crawl")


def phase_normalize(args: argparse.Namespace) -> None:
    output = Path(args.output).resolve()
    repositories = PUBLIC_REPOSITORIES
    if args.repository:
        owner, repo = args.repository.split("/", 1)
        repositories = ((owner, repo),)
    count = sum(normalize_repository(output, owner, repo) for owner, repo in repositories)
    atomic_write_json(output / "provenance" / "dataset_state.json", {"phase": "normalize", "public_documents_created": count, "updated_at": utc_now(), "cutoff": CUTOFF.isoformat().replace("+00:00", "Z")})
    write_manifest(output, "normalize")


def phase_generate_internal(args: argparse.Namespace) -> None:
    output = Path(args.output).resolve()
    generate_internal(output, template_fallback=args.template_fallback)
    atomic_write_json(output / "provenance" / "dataset_state.json", {"phase": "generate-internal", "updated_at": utc_now(), "cutoff": CUTOFF.isoformat().replace("+00:00", "Z")})
    write_manifest(output, "generate-internal")


def phase_validate(args: argparse.Namespace) -> None:
    output = Path(args.output).resolve()
    result = validate_dataset(output, allow_incomplete=args.allow_incomplete)
    atomic_write_json(
        output / "provenance" / "dataset_state.json",
        {
            "phase": "validate",
            "valid": result["valid"],
            "updated_at": utc_now(),
            "cutoff": CUTOFF.isoformat().replace("+00:00", "Z"),
        },
    )
    write_manifest(output, "validate")
    if not result["valid"]:
        raise RuntimeError("Dataset validation failed")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("init", "crawl", "normalize", "generate-internal", "validate", "all"))
    parser.add_argument("--base", default=str(DEFAULT_BASE))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--repository", help="Only process owner/repository during crawl or normalize")
    parser.add_argument("--template-fallback", action="store_true", help="Use deterministic scaffolded documents when LLM credentials are unavailable")
    parser.add_argument("--allow-incomplete", action="store_true", help="Report validation gaps without failing; useful before authenticated crawl")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.phase == "init":
        phase_init(args)
    elif args.phase == "crawl":
        phase_crawl(args)
    elif args.phase == "normalize":
        phase_normalize(args)
    elif args.phase == "generate-internal":
        phase_generate_internal(args)
    elif args.phase == "validate":
        phase_validate(args)
    else:
        phase_init(args)
        phase_crawl(args)
        phase_normalize(args)
        phase_generate_internal(args)
        phase_validate(args)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit("Interrupted; checkpoints are durable and the phase can be resumed.")
