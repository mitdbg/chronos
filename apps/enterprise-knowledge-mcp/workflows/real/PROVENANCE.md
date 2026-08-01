# Workload provenance

These workloads were produced by running Codex with the prompts in `prompts/`
against a live enterprise-knowledge MCP server on July 27, 2026. The portable
JSONL traces were extracted from the corresponding Codex session rollouts;
they were not written as benchmark scripts.

| Workflow | Codex session | Trace events | Memory writes | Rollout SHA-256 |
|---|---|---:|---:|---|
| Create department and team | `019fa1d1-eca1-7352-a12a-0e54b7379da4` | 34 | 2 | `e0b6e347ef1484a8e9c388caca5aa16ba7513e270dab932e3e8f7a5515d5a625` |
| Add team member | `019fa1d8-835d-7291-aea3-579e6a84b2be` | 17 | 1 | `f3f0265c0451c14223531a9aa1c29d02654eaac6579241cd49acdd7c055006e4` |
| Debug streaming handshake | `019fa1dd-5dd9-7820-ae2d-db0e45af0882` | 9 | 1 | `b430cf72ec056ab12b95097ba485741977f393b49c2b7462ad44b42a51dd87b2` |
| Debug predictive headroom | `019fa1dd-5dd6-7ee1-9bd2-d2e8751cd69b` | 11 | 1 | `420bd929781ad3defa32d8546685617511b4fba00812d17910f62d48e8b74b2c` |
| Debug throttling race | `019fa1dd-5dd6-70b0-a11f-5879b04ef105` | 9 | 1 | `9080067d40d54de0b8588760c08a6a5176d93960d5411efb253f665d1d4a5a52` |
| Ten EnterpriseRAG questions | `019fa1e0-6d00-7b80-aad5-c668deeaaefe` | 71 | 0 | `8c3794ebb4d93eee91c3c9e6fe3ca9ecd8d20288324240001e3f4b4cd32ce8cb` |

The source rollouts are under
`~/.codex/sessions/2026/07/27/rollout-<timestamp>-<session>.jsonl`. The
repository also retains Codex's emitted event stream, stderr, and final response
for each successful run under `runs/20260727/`.

The combined trace contains 151 actions: 141 MCP calls, seven shell commands,
and three file patches. It preserves timestamp order across the three debugging
sessions that Codex ran concurrently. Its SHA-256 is
`b5571f911aa084bfdc83511adda47844e6db5c7669e2979fb7f0c93829c06d46`.

Diagnostic and cancelled attempts remain in `runs/20260727/` to document two
implementation defects discovered while capturing the workload. They are not
part of any normalized or benchmarked trace.
