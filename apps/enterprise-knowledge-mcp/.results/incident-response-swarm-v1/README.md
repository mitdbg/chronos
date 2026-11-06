# Incident-response swarm

Captured sessions and replay outputs for the paper's incident-response swarm
(Figure 7).

| Path | Contents |
| --- | --- |
| `captures/raw/` | Eight redacted Codex session event streams |
| `captures/cli/` | CLI events, prompts and capture records |
| `traces/` | Normalized replay traces with recorded model delays |
| `incident-inputs.jsonl` | Incident inputs |
| `replay/` | Aggregate tables and per-trial configurations, results and logs |
| `analysis/` | Generated summary tables and figures |
| `release-manifest.json` | Checksums and capture provenance |

See [REPRODUCING.md](REPRODUCING.md) for verification, figure generation and
benchmark replay commands.
