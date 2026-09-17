#!/usr/bin/env python3
"""Benchmark the qst_0250 shell search against a Chronos/PostgreSQL prototype.

The query-side prototype stores one row per JSON file in a branchable Chronos table, with
the original JSON and a jq-compatible, flattened string projection. A custom
PostgreSQL pg_trgm GIN index serves the broad text predicates; SQL then splits
the projected text into lines and applies the final per-file regex and 20-line
cap. The sole filesystem baseline is the exact command captured in the qst_0250 run.

Example (from the repository root):

  uv run --project chronos python chronos/bench/fs/agentic_search_qst0250.py \
    --source-root EnterpriseRAG-Bench/generated_data_infra_v1 \
    --events EnterpriseRAG-Bench/agent_search_benchmark/runs/lunamax-100-v1/qst_0250.events.jsonl \
    --out-dir EnterpriseRAG-Bench/agent_search_benchmark/runs/lunamax-100-v1/chronosfs-qst0250-pgtrgm-v1 \
    --dsn 'postgresql://user@/bench?host=%2Ftmp%2Fpgsock&port=55432'

The database must be disposable/empty. The script refuses to overwrite its
logical table if it already exists. After one run, ``--reuse-results`` can
repeat the SQL query and compare it with the captured output without rebuilding
the index.
"""

from __future__ import annotations

import argparse
import json
import shlex
import statistics
import subprocess
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg
from psycopg import sql

from chronos_core.branching import ChronosBranchContext


TABLE = "qst0250_files"
ITEM_ID = "item_240"
PREFILTERS = (
    r"(spike|burst|surge)",
    r"(bill|overage|charge)",
    r"(same|group|coalesc|aggregate|repeat|subsequent|additional)",
    r"(prepaid|prepay|pre-paid|credit)",
    r"(dedicated|gpu|hosting)",
)
FINAL_REGEX = (
    r"((spike|burst|surge).*(same|group|coalesc|aggregate|repeat|subsequent|additional).*(bill|overage|charge|event))"
    r"|((bill|overage|charge|event).*(same|group|coalesc|aggregate|repeat|subsequent|additional).*(spike|burst|surge))"
)
QUERY = f"""
SELECT doc.path, hit.line, hit.line_no
FROM {TABLE} AS doc
CROSS JOIN LATERAL (
    SELECT line, line_no
    FROM regexp_split_to_table(doc.search_text, E'\\n')
         WITH ORDINALITY AS candidate_lines(line, line_no)
    WHERE line ~* '{FINAL_REGEX}'
    ORDER BY line_no
    LIMIT 20
) AS hit
WHERE doc.path LIKE 'sources/%.json'
  AND doc.content ~* '{PREFILTERS[0]}'
  AND doc.content ~* '{PREFILTERS[1]}'
  AND doc.content ~* '{PREFILTERS[2]}'
  AND doc.content ~* '{PREFILTERS[3]}'
  AND doc.content ~* '{PREFILTERS[4]}'
ORDER BY doc.path, hit.line_no
""".strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--item-id", default=ITEM_ID)
    parser.add_argument("--query-repeats", type=int, default=3)
    parser.add_argument(
        "--reuse-results",
        type=Path,
        help="reuse an existing results.json with an already-ingested Chronos table",
    )
    return parser.parse_args()


def find_recorded_command(
    events_path: Path, item_id: str
) -> tuple[list[str], float, str]:
    started: dict[str, Any] | None = None
    completed: dict[str, Any] | None = None
    with events_path.open("r", encoding="utf-8") as events:
        for raw_line in events:
            event = json.loads(raw_line)
            detail = event.get("event", {})
            item = detail.get("item", {})
            if item.get("id") != item_id or item.get("type") != "command_execution":
                continue
            if detail.get("type") == "item.started":
                started = event
            elif detail.get("type") == "item.completed":
                completed = event
    if started is None or completed is None:
        raise RuntimeError(f"could not find a completed command item {item_id!r}")
    command = started["event"]["item"]["command"]
    argv = shlex.split(command)
    if len(argv) < 3 or Path(argv[0]).name != "bash":
        raise RuntimeError(f"unexpected captured command: {command[:200]!r}")
    elapsed = (completed["monotonic_ns"] - started["monotonic_ns"]) / 1e9
    output = completed.get("event", {}).get("item", {}).get("aggregated_output", "")
    return argv, elapsed, output


def result_map_from_text(text: str) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    current_path: str | None = None
    current_lines: list[str] = []

    def finish() -> None:
        nonlocal current_path, current_lines
        while current_lines and current_lines[-1] == "":
            current_lines.pop()
        if current_path is not None:
            result[current_path] = current_lines
        current_path = None
        current_lines = []

    for line in text.split("\n"):
        if line.startswith("===== ") and line.endswith(" ====="):
            finish()
            current_path = line[6:-6]
        elif current_path is not None:
            current_lines.append(line)
    finish()
    return result


def flatten_strings(value: Any) -> Iterable[str]:
    """Match jq's recursive ``.. | strings`` traversal order."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from flatten_strings(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from flatten_strings(item)


def search_projection(raw_text: str) -> tuple[str, bool]:
    try:
        document = json.loads(raw_text)
    except json.JSONDecodeError:
        # jq emits no string rows on malformed JSON; retain raw JSON for the
        # first-stage grep predicates and make the final stage empty.
        return "", False
    flattened = "\n".join(part.replace("\\n", "\n") for part in flatten_strings(document))
    # PostgreSQL text cannot contain NUL. The corpus is expected to be clean;
    # replace a decoded JSON \u0000 rather than failing the whole COPY.
    return flattened.replace("\x00", "\ufffd"), True


def iter_rg_paths(source_root: Path) -> Iterable[str]:
    process = subprocess.Popen(
        ["rg", "--files", "--null", "-g", "*.json", "sources"],
        cwd=source_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    pending = b""
    while True:
        chunk = process.stdout.read(1024 * 1024)
        if not chunk:
            break
        pending += chunk
        while b"\0" in pending:
            raw_path, pending = pending.split(b"\0", 1)
            if raw_path:
                yield raw_path.decode("utf-8", errors="surrogateescape")
    if pending:
        yield pending.decode("utf-8", errors="surrogateescape")
    stderr = process.stderr.read() if process.stderr else b""
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"rg --files failed ({return_code}): {stderr.decode(errors='replace')}")


def copy_text_files(dsn: str, source_root: Path) -> dict[str, Any]:
    with psycopg.connect(dsn) as connection:
        existing = connection.execute(
            "SELECT to_regclass(%s)", (f"public.{TABLE}",)
        ).fetchone()[0]
        if existing is not None:
            raise RuntimeError(f"table {TABLE!r} already exists; use a fresh database")
        connection.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        connection.execute(
            f"CREATE UNLOGGED TABLE {TABLE} ("
            "path text PRIMARY KEY, content text NOT NULL, search_text text NOT NULL)"
        )
        connection.commit()

        started = time.perf_counter()
        count = 0
        bytes_read = 0
        malformed_json = 0
        skipped_binary = 0
        nul_replacements = 0
        with connection.cursor() as cursor:
            with cursor.copy(
                f"COPY {TABLE} (path, content, search_text) FROM STDIN"
            ) as copy:
                for relative_path in iter_rg_paths(source_root):
                    data = (source_root / relative_path).read_bytes()
                    bytes_read += len(data)
                    # ripgrep's default behavior skips binary files.
                    if b"\0" in data:
                        skipped_binary += 1
                        continue
                    raw_text = data.decode("utf-8", errors="replace")
                    search_text, valid_json = search_projection(raw_text)
                    if not valid_json:
                        malformed_json += 1
                    nul_replacements += search_text.count("\ufffd")
                    copy.write_row((relative_path, raw_text, search_text))
                    count += 1
        connection.commit()
    return {
        "seconds": time.perf_counter() - started,
        "files": count,
        "source_bytes": bytes_read,
        "malformed_json": malformed_json,
        "skipped_binary": skipped_binary,
        "nul_replacements": nul_replacements,
    }


def create_chronos_projection(
    dsn: str, output_dir: Path
) -> tuple[ChronosBranchContext, str, float, float, str]:
    context = ChronosBranchContext.connect(
        dsn,
        backend="interval",
        # This prototype builds its own PostgreSQL GIN/pattern indexes on the
        # physical branch table. Disable generic source-index copying; this is
        # also compatible with the local PostgreSQL branch build's hidden rowid.
        interval_create_secondary_indexes=False,
        interval_create_writer_segment_index=False,
    )
    started = time.perf_counter()
    context.register_table(TABLE, ["path"])
    elapsed = time.perf_counter() - started

    index_started = time.perf_counter()
    with psycopg.connect(dsn) as connection:
        physical = connection.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = %s",
            (TABLE,),
        ).fetchone()[0]
        physical_id = sql.Identifier(physical)
        connection.execute(
            sql.SQL("CREATE INDEX qst0250_files_content_trgm "
                    "ON {} USING GIN (content gin_trgm_ops)").format(physical_id)
        )
        connection.execute(
            sql.SQL("CREATE INDEX qst0250_files_path_prefix "
                    "ON {} (path text_pattern_ops)").format(physical_id)
        )
        connection.execute(sql.SQL("ANALYZE {}").format(physical_id))
        connection.commit()
        (table_bytes, index_bytes, total_bytes) = connection.execute(
            "SELECT pg_table_size(%s::regclass), pg_indexes_size(%s::regclass), "
            "pg_total_relation_size(%s::regclass)",
            (physical, physical, physical),
        ).fetchone()
    index_elapsed = time.perf_counter() - index_started
    index_sizes = (
        f"logical table: {TABLE}\nphysical table: {physical}\n"
        f"table bytes: {table_bytes}\nindex bytes: {index_bytes}\n"
        f"total bytes: {total_bytes}\n"
    )
    (output_dir / "chronos_physical_table.txt").write_text(index_sizes, encoding="utf-8")
    return context, physical, elapsed, index_elapsed, index_sizes


def connect_existing_projection(
    dsn: str, previous: dict[str, Any], output_dir: Path
) -> tuple[ChronosBranchContext, str, str]:
    context = ChronosBranchContext.connect(
        dsn,
        backend="interval",
        interval_create_secondary_indexes=False,
        interval_create_writer_segment_index=False,
    )
    physical = str(previous["physical_table"])
    with psycopg.connect(dsn) as connection:
        (table_bytes, index_bytes, total_bytes) = connection.execute(
            "SELECT pg_table_size(%s::regclass), pg_indexes_size(%s::regclass), "
            "pg_total_relation_size(%s::regclass)",
            (physical, physical, physical),
        ).fetchone()
    index_sizes = (
        f"logical table: {TABLE}\nphysical table: {physical}\n"
        f"table bytes: {table_bytes}\nindex bytes: {index_bytes}\n"
        f"total bytes: {total_bytes}\n"
    )
    (output_dir / "chronos_physical_table.txt").write_text(index_sizes, encoding="utf-8")
    return context, physical, index_sizes


def render_result_map(result: dict[str, list[str]]) -> str:
    chunks: list[str] = []
    for path in sorted(result):
        chunks.append(f"\n===== {path} =====\n")
        chunks.extend(line + "\n" for line in result[path])
    return "".join(chunks)


def compare_results(left: dict[str, list[str]], right: dict[str, list[str]]) -> dict[str, Any]:
    all_paths = set(left) | set(right)
    mismatches = [path for path in sorted(all_paths) if left.get(path) != right.get(path)]
    return {
        "equivalent": not mismatches,
        "left_documents": len(left),
        "right_documents": len(right),
        "mismatching_documents": len(mismatches),
        "mismatch_examples": [
            {
                "path": path,
                "left_lines": left.get(path, [])[:5],
                "right_lines": right.get(path, [])[:5],
            }
            for path in mismatches[:5]
        ],
    }


def main() -> None:
    args = parse_args()
    source_root = args.source_root.resolve()
    events_path = args.events.resolve()
    output_dir = args.out_dir.resolve()
    if not (source_root / "sources").is_dir():
        raise SystemExit(f"source root has no sources/: {source_root}")
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise SystemExit(f"output directory is not empty: {output_dir}")

    command_argv, recorded_seconds, recorded_output = find_recorded_command(
        events_path, args.item_id
    )
    if not recorded_output:
        raise RuntimeError("the completed qst_0250 command has no captured stdout")
    if args.query_repeats < 1:
        raise SystemExit("repeat counts must be positive")
    (output_dir / "native_agent_command_recorded.out").write_text(
        recorded_output, encoding="utf-8"
    )
    (output_dir / "native_agent_command_recorded.json").write_text(
        json.dumps(
            {
                "command": command_argv,
                "seconds": recorded_seconds,
                "source": str(events_path),
                "item_id": args.item_id,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    chronos_context: ChronosBranchContext | None = None
    if args.reuse_results is not None:
        previous = json.loads(args.reuse_results.resolve().read_text(encoding="utf-8"))
        ingestion = previous["corpus"]
        registration_seconds = previous["timings"]["chronos_register_copy_seconds"]
        index_build_seconds = previous["timings"]["index_build_and_analyze_seconds"]
        chronos_context, physical_table, index_sizes = connect_existing_projection(
            args.dsn, previous, output_dir
        )
    else:
        ingestion = copy_text_files(args.dsn, source_root)
    try:
        if args.reuse_results is None:
            (
                chronos_context,
                physical_table,
                registration_seconds,
                index_build_seconds,
                index_sizes,
            ) = create_chronos_projection(args.dsn, output_dir)
        session_started = time.perf_counter()
        session = chronos_context.checkout("main")
        checkout_seconds = time.perf_counter() - session_started

        query_seconds: list[float] = []
        sql_result: dict[str, list[str]] = {}
        result_rows = 0
        for repeat in range(args.query_repeats):
            started = time.perf_counter()
            rows = session.query(QUERY)
            query_seconds.append(time.perf_counter() - started)
            grouped: dict[str, list[str]] = defaultdict(list)
            for row in rows:
                grouped[str(row["path"])].append(str(row["line"]))
            if repeat == 0:
                sql_result = dict(grouped)
                result_rows = len(rows)
        (output_dir / "chronos_sql.out").write_text(
            render_result_map(sql_result), encoding="utf-8"
        )

        explain_rows = session.explain(QUERY)
        (output_dir / "chronos_explain.json").write_text(
            json.dumps(explain_rows, indent=2, default=str) + "\n", encoding="utf-8"
        )

        recorded_result = result_map_from_text(recorded_output)
        recorded_comparison = compare_results(recorded_result, sql_result)
        timings = {
            "captured_agent_command_seconds": recorded_seconds,
            "copy_ingest_seconds": ingestion["seconds"],
            "chronos_register_copy_seconds": registration_seconds,
            "index_build_and_analyze_seconds": index_build_seconds,
            "chronos_checkout_seconds": checkout_seconds,
            "chronos_sql_query_seconds": query_seconds,
            "chronos_sql_query_median_seconds": statistics.median(query_seconds),
        }
        result = {
            "experiment": "qst_0250 Chronos branch-backed single-row search prototype",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "query_item_id": args.item_id,
            "comparison_policy": "The sole filesystem baseline is the exact unmodified command, duration, and output captured for this item in the qst_0250 event log.",
            "source_root": str(source_root),
            "table": TABLE,
            "physical_table": physical_table,
            "corpus": ingestion,
            "timings": timings,
            "results": {
                "sql_documents": len(sql_result),
                "sql_lines": result_rows,
                "recorded_agent_command_documents": len(recorded_result),
                "agent_command_equivalence": recorded_comparison,
            },
            "storage_note": index_sizes,
            "environment": {},
            "sql": QUERY,
        }
        with psycopg.connect(args.dsn) as environment_connection:
            result["environment"]["postgres_version"] = environment_connection.execute(
                "SELECT version()"
            ).fetchone()[0]
        result["environment"]["ripgrep_version"] = subprocess.run(
            ["rg", "--version"], capture_output=True, text=True, check=False
        ).stdout.splitlines()[0]
        (output_dir / "results.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

        median_sql = statistics.median(query_seconds)
        setup_seconds = ingestion["seconds"] + registration_seconds + index_build_seconds
        speedup_agent = recorded_seconds / median_sql if median_sql else None
        pg_version = result["environment"]["postgres_version"].splitlines()[0]
        rg_version = result["environment"]["ripgrep_version"]
        report = [
            "# qst_0250 filesystem search benchmark",
            "",
            "This prototype ingests each visible `sources/**/*.json` file as one row in a Chronos interval table. It keeps raw JSON for the shell command's five whole-file predicates and a flattened string column equivalent to `jq -r '.. | strings'` for the final line regex. A PostgreSQL `pg_trgm` GIN index and a path-prefix B-tree index are attached to Chronos' physical table. The query is executed through a Chronos `main` branch session, so interval visibility is included.",
            "This is a query-side projection prototype, not a FUSE-mounted replacement: it isolates the SQL translation/index path under the requested one-row-per-file assumption.",
            "",
            "## Results",
            "",
            f"- Files ingested: {ingestion['files']:,} ({ingestion['source_bytes'] / (1024**3):.2f} GiB raw JSON).",
            f"- Exact agent command captured in the qst_0250 event log: {recorded_seconds:.3f}s; the command was not modified or rerun.",
            f"- Chronos/PostgreSQL query (median of {len(query_seconds)} runs): {median_sql:.3f}s; samples: {', '.join(f'{s:.3f}' for s in query_seconds)}.",
            f"- End-to-end speedup vs the exact logged command: {speedup_agent:.1f}x." if speedup_agent is not None else "- End-to-end speedup vs the exact logged command: n/a.",
            f"- Results match recorded agent output: **{recorded_comparison['equivalent']}** ({recorded_comparison['left_documents']} vs {recorded_comparison['right_documents']} files).",
            "",
            "## Setup cost and caveats",
            "",
            f"- COPY ingest: {ingestion['seconds']:.1f}s; Chronos table registration/copy: {registration_seconds:.1f}s; GIN/path index build plus analyze: {index_build_seconds:.1f}s; setup total: {setup_seconds:.1f}s.",
            "- This is an end-to-end comparison of the exact logged shell command with indexed SQL, not an isolated measurement of filesystem I/O: Chronos changes the execution strategy and uses prebuilt indexes.",
            f"- Environment: {pg_version}; {rg_version}. SQL timings are repeated queries after ingestion and index creation.",
            "- The recorded duration and output are the successful historical capture from the event log; this harness does not execute the baseline command.",
            "- The GIN index is created directly on the Chronos physical table because the current generic Chronos index API only exposes ordinary column indexes; index creation/rebuild must become branch/schema lifecycle-aware before production use.",
            "- File order is not compared: this command prints every matching file and only caps matching lines at 20 per file. The per-file line order and content are compared.",
            "",
            "## Artifacts",
            "",
            "- `native_agent_command_recorded.out` / `.json`: exact successful command output and timing captured in the agent event log.",
            "- `chronos_sql.out`: indexed Chronos SQL output.",
            "- `chronos_explain.json`: branch-rewritten PostgreSQL plan.",
            "- `results.json`: machine-readable timings, sizes, and equivalence checks.",
            "",
        ]
        (output_dir / "README.md").write_text("\n".join(report), encoding="utf-8")
    finally:
        if chronos_context is not None:
            chronos_context.close()


if __name__ == "__main__":
    main()
