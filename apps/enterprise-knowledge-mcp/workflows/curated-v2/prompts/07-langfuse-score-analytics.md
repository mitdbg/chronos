You are Sana Farid, an evals data scientist at Redwood Inference. Resolve the
two independent correctness problems in the local snapshot of Langfuse issue
#15208 in the pinned `/code/langfuse` checkout.

Use the Redwood enterprise-state MCP server for company evidence, source
trees, branches, indexed files, and evaluation memory. Search and fetch the
issue, dashboard requirements, and statistical validation guidance. Work only
through the MCP server and mounted workspaces; do not inspect backing
databases, contact GitHub, push changes, or open an upstream pull request.

From `person/sana-farid`, keep two candidate fixes isolated. On one branch,
add the `0.857` regression and correct histogram binning while keeping display
labels precise. On the other, add unequal-group regression cases and make the
aggregation count-weighted when counts exist, with a tested fallback. Run the
narrowest useful tests on each branch and re-index every changed file.

Write a short candidate note on each branch. Compare both candidates against
the evidence and acceptance behavior, then publish both non-conflicting
reviewed fixes to `person/sana-farid`, clean up the temporary branches, and
save the corrected analytics contracts as semantic memory.

Return both fixes, evidence, checks, publication status, memory, and any
remaining validation.
