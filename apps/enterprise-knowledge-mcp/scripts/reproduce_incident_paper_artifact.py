#!/usr/bin/env python3
"""Verify released files and regenerate incident-swarm tables/plots offline."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
LABELS = {"chronos": "Chronos", "native-branching": "Uncoordinated", "native-branching-lock": "Serialized"}
COLORS = {"chronos": "#9467bd", "native-branching": "#d95f02", "native-branching-lock": "#2678b2"}


def verify(root: Path) -> int:
    manifest = json.loads((root / "release-manifest.json").read_text())
    for item in manifest["files"]:
        path = root / item["path"]
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError("Manifest path escapes release directory")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != item["sha256"] or path.stat().st_size != item["bytes"]:
            raise ValueError(f"Bad checksum (run git lfs pull if this is a pointer): {path}")
    return len(manifest["files"])


def rows_from_results(root: Path):
    with (root / "replay/sweep-results.csv").open() as stream:
        original = list(csv.DictReader(stream))
    rows = []
    for item in original:
        system = item["system"]
        trial_name = Path(item["output"]).name
        trial = root / "replay" / system / trial_name
        config = json.loads((trial / "config.json").read_text())
        requested = int(config["workers"])
        completed = int(item["successful_workers"])
        result_file = trial / "results.json"
        final = result_file.exists()
        if final:
            result = json.loads(result_file.read_text())["results"][0]
            if completed != result["successful_workers"]:
                raise ValueError(f"Aggregate/per-trial completion mismatch: {trial}")
            if abs(float(item["elapsed_seconds"]) - result["elapsed_seconds"]) > 1e-5:
                raise ValueError(f"Aggregate/per-trial runtime mismatch: {trial}")
            if int(item["verifier_violations"]) != result["verifier"]["violation_count"]:
                raise ValueError(f"Aggregate/per-trial verifier mismatch: {trial}")
            if int(item["incomplete_bundles"]) != len(result["final_state"]["incomplete_bundles"]):
                raise ValueError(f"Aggregate/per-trial bundle mismatch: {trial}")
            if int(result["workers"]) != requested:
                raise ValueError(f"Config/per-trial worker count mismatch: {trial}")
        rows.append({"system": system, "requested_workers": requested,
                     "completed_workers": completed, "completion_percent": 100 * completed / requested,
                     "has_final_result": final, "status": item["status"],
                     "elapsed_seconds": float(item["elapsed_seconds"]),
                     "verifier_violations": int(item["verifier_violations"]) if item["verifier_violations"] else None,
                     "incomplete_bundles": int(item["incomplete_bundles"]) if item["incomplete_bundles"] else None})
    return rows


def render(rows, output: Path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    for metric, ylabel, filename in [
        ("elapsed_seconds", "Elapsed replay time (s)", "runtime"),
        ("completion_percent", "Workers completed (%)", "completion"),
        ("verifier_violations", "Verifier observations of violations", "violations"),
    ]:
        fig, ax = plt.subplots(figsize=(5.2, 2.9), layout="constrained")
        for system, label in LABELS.items():
            subset = sorted((row for row in rows if row["system"] == system), key=lambda row: row["requested_workers"])
            finalized = [row for row in subset if row["has_final_result"]]
            ax.plot([row["requested_workers"] for row in finalized], [row[metric] for row in finalized],
                    "o-", label=label, color=COLORS[system], markersize=4)
            for row in subset:
                if not row["has_final_result"] and row[metric] is not None:
                    ax.scatter(row["requested_workers"], row[metric], marker="x", color=COLORS[system])
                    ax.annotate("partial", (row["requested_workers"], row[metric]),
                                xytext=(-34, -18) if metric == "completion_percent" else (-28, 8),
                                textcoords="offset points", fontsize=8, color=COLORS[system])
        ax.set_xscale("log", base=2)
        counts = sorted({row["requested_workers"] for row in rows})
        ax.set_xticks(counts, labels=[str(n) for n in counts])
        ax.set_xlabel("Concurrent workers")
        ax.set_ylabel(ylabel)
        if metric == "elapsed_seconds":
            ax.set_yscale("log")
        elif metric == "completion_percent":
            ax.set_ylim(0, 108)
        ax.grid(alpha=.25)
        ax.legend(fontsize=8, ncol=3, loc="lower center", bbox_to_anchor=(.5, 1.01))
        for suffix in ["pdf", "png"]:
            fig.savefig(output / f"{filename}.{suffix}", dpi=180)
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, default=APP / ".results/incident-response-swarm-v1")
    parser.add_argument("--output", type=Path, default=APP / ".results/incident-response-swarm-v1/analysis")
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    count = verify(args.artifact)
    rows = rows_from_results(args.artifact)
    print(f"Verified {count} files and {len(rows)} trial summaries")
    if not args.verify_only:
        args.output.mkdir(parents=True, exist_ok=True)
        with (args.output / "summary.csv").open("w") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        render(rows, args.output)


if __name__ == "__main__":
    main()
