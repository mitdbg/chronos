# Applications for Multi-Store Branching

**Status:** Research motivation; not an API specification

This document motivates Chronos multi-store branching: one branch spans a
relational database and a POSIX filesystem. The key requirement is that code,
artifacts, generated files, and relational state can be forked, mutated,
checked, diffed, merged, or discarded together.

The examples below are included only when both stores matter. If an application
only needs versioned files, Git or object-store snapshots may be enough. If it
only needs versioned rows, relational Chronos may be enough. Multi-store
branching is justified when correctness depends on the relationship between
files and database state.

## Why Existing Solutions Fall Short

### Git Alone

Git versions files well, but it does not version live relational state. Many
agent and data workflows store essential state in SQL tables: labels, task
queues, graph nodes, experiment metrics, rollout scores, source provenance, and
schema changes. If files are branched but SQL state is shared, a speculative run
can pollute global state or read inconsistent metadata.

### Database Transactions Alone

Database transactions isolate relational changes, but they do not isolate
filesystem side effects from scripts, compilers, notebook execution, data
exporters, or MCP file tools. A transaction rollback cannot remove generated
files, changed configs, corrupted caches, or model artifacts written to disk.

### Containers Alone

Containers can isolate processes and filesystems, but they do not provide
branch-native database semantics. A container may point to a shared database, or
it may clone a database volume at high cost. Containers also do not naturally
provide branch diff, checkpoint, merge, and branch lineage across relational and
filesystem state.

### Copying Whole Workspaces

Copying the repository and dumping/restoring the database for every branch is
simple but too expensive for wide or deep speculation. It also makes merge and
diff hard because the system loses structured lineage across stores.

### Ad Hoc Run Directories

Many pipelines write to `runs/123` while storing metadata in shared tables.
This helps with artifact organization but not isolation. The same code can
still update shared database rows, consume shared task queues, or write outside
the intended run directory.

## Data Science Experiments

### Motivation

Data science workflows combine source files, generated artifacts, and database
state:

- Python/R notebooks and scripts.
- Feature engineering code.
- Model config files.
- Generated plots and reports.
- Model checkpoints.
- Feature tables.
- Experiment metadata.
- Evaluation metrics.
- Dataset splits and run registries.

A single experiment often changes both the filesystem and database. For
example, an agent may edit `features.py`, generate a new parquet file, register
a feature set in SQL, train a model, and write evaluation metrics to a table.
Those effects need to be reviewed as one unit.

### Why Existing Solutions Are Not Enough

Git can version `features.py`, but not the feature table rows and metrics the
script wrote. SQL transactions can rollback feature rows, but not generated
parquet files or model checkpoints. MLflow-style run tracking can record
artifacts, but usually does not give a branch-local SQL database view that can
be queried and mutated by arbitrary analysis code.

### Multi-Store Branch Use

Create one branch per experiment:

```text
branch exp_feature_v2:
  filesystem:
    src/features.py
    outputs/features_v2.parquet
    reports/metrics.html
  relational:
    feature_sets
    experiment_runs
    metrics
    validation_failures
```

Run training and validation inside the branch. If the experiment fails, discard
the branch. If it passes, merge code, artifacts, and relational metadata
together.

## Data Annotation

### Motivation

Annotation systems often store task state in a relational database while the
source material and exports live as files:

- Images, PDFs, videos, or audio clips.
- OCR outputs and document chunks.
- Annotation JSON exports.
- Label tables.
- Assignment queues.
- Annotator decisions.
- Review/adjudication state.
- Quality-control scores.

Agents may pre-label data, humans may review it, and adjudicators may resolve
conflicts. Each batch can affect both exported files and SQL label state.

### Why Existing Solutions Are Not Enough

Versioning annotation export files does not isolate the task queue or label
tables. A database-only branch does not isolate generated crops, OCR cache
files, or JSONL annotation dumps. Separate staging tables help, but they force
each tool to understand staging conventions and do not protect filesystem
side effects.

### Multi-Store Branch Use

Create a branch for each annotation batch or agent proposal:

```text
branch annotator_batch_17:
  filesystem:
    exports/batch_17.jsonl
    crops/*.png
    review_notes.md
  relational:
    annotation_tasks
    labels
    adjudication_decisions
    quality_scores
```

Review the branch diff. Merge accepted labels and files. Delete rejected or
low-quality branches without cleanup scripts.

## Agent Sandboxing

### Motivation

Coding and research agents do not only edit files. They also mutate persistent
state:

- Code and config files.
- Generated tests, reports, and logs.
- Build outputs.
- Long-term memory tables.
- Knowledge graph nodes and edges.
- Source indexes and provenance.
- Task state and tool traces.

When an agent is exposed through MCP to Claude, Codex, or another client, file
operations may be out-of-band POSIX reads/writes. The sandbox must therefore be
the mounted filesystem itself, not a special file API.

### Why Existing Solutions Are Not Enough

A container can isolate files but usually still points at shared application
databases. A database transaction can isolate SQL but cannot rollback shell
commands that wrote files. Git worktrees isolate source code but not generated
artifacts or relational memory. MCP file roots protect path access, but they do
not solve database branching.

### Multi-Store Branch Use

Create one branch per agent run:

```text
branch codex_run_42:
  filesystem:
    source edits
    generated tests
    logs
    reports
  relational:
    graph memory
    task state
    tool traces
    source indexes
```

Expose only the branch mount to MCP file and shell tools. Run validation.
Merge if checks pass; otherwise delete the branch.

## Speculative Execution

### Motivation

Speculative systems explore many alternatives before choosing one:

- Multiple implementation plans.
- Alternative data-cleaning strategies.
- Competing extraction prompts.
- Different schema migration paths.
- Different generated reports.

Each alternative may produce files and mutate relational state. The system must
compare alternatives and keep only the best.

### Why Existing Solutions Are Not Enough

Ad hoc temp directories do not isolate SQL state. SQL savepoints do not isolate
files. Copying the whole workspace for each speculation becomes expensive as
branch width grows. Without branch lineage, comparing alternatives requires
custom bookkeeping.

### Multi-Store Branch Use

Fork one branch per hypothesis:

```text
main
  -> plan_a
  -> plan_b
  -> plan_c
```

Each plan can run arbitrary code, generate artifacts, and update scoring tables.
The orchestrator evaluates diffs and metrics, merges the winning branch, and
discards the rest.

## Simulation

### Motivation

Simulation workflows often have a hybrid state model:

- Filesystem configs and scenario definitions.
- Simulation binaries or scripts.
- Log files and snapshots.
- Relational entity tables.
- Event histories.
- Measurement tables.
- Scenario metadata.

Changing a config file without corresponding database changes can create an
invalid scenario. Likewise, database state without matching artifacts may be
impossible to reproduce.

### Why Existing Solutions Are Not Enough

Containers isolate runtime execution but do not provide cheap branch trees over
database state. Database snapshots isolate rows but not simulation outputs or
config files. Copying directories and databases for every scenario is too slow
for large parameter sweeps.

### Multi-Store Branch Use

Create branches for scenarios:

```text
branch scenario_high_demand:
  filesystem:
    configs/demand.yaml
    logs/run.log
    snapshots/final_state.bin
  relational:
    entities
    events
    metrics
    scenario_runs
```

Run simulations independently. Compare metrics and artifacts across branches.
Merge selected scenarios into a curated result set.

## Monte Carlo Tree Search

### Motivation

MCTS and related tree-search methods create many states from prior states:

- Tree nodes.
- Visit counts.
- Reward estimates.
- Rollout traces.
- Environment state.
- Generated code or plans.
- Per-node artifacts.

Each expanded node is naturally a branch from a previous node. Rollouts may
mutate both files and relational state.

### Why Existing Solutions Are Not Enough

Keeping all state in memory loses durability and makes long runs hard to
resume. Storing tree state in SQL while writing artifacts to normal directories
requires custom cleanup and replay logic. Copying full workspaces per node is
too expensive for wide search. Git cannot represent relational environment
state.

### Multi-Store Branch Use

Map tree nodes to branches:

```text
node_0
  -> node_1
     -> node_4
     -> node_5
  -> node_2
  -> node_3
```

Each node branch contains:

```text
filesystem:
  generated plan/code
  rollout logs
  evaluator outputs
relational:
  node table
  reward estimates
  environment state
  action history
```

Expanding a node is `create_branch(child, from_branch=parent)`. Failed rollouts
are deleted. Promising branches can be checkpointed, revisited, and merged into
the search's canonical state.

## Auto Research

### Motivation

Automated research systems combine source artifacts and structured knowledge:

- PDFs, web captures, extracted text.
- Notes and generated reports.
- Analysis scripts.
- Citation graphs.
- Claims and evidence tables.
- Entity and relation extraction outputs.
- Confidence scores.
- Review decisions.

Different research hypotheses can lead to different extraction outputs,
different graph structure, and different generated reports.

### Why Existing Solutions Are Not Enough

Versioning notes in files does not isolate the citation graph or claims table.
Database branching alone does not isolate downloaded papers, extracted text, or
generated reports. Using separate project directories and staging databases
requires every tool to understand a custom run layout.

### Multi-Store Branch Use

Create branches per hypothesis or extraction strategy:

```text
branch hypothesis_transformer_scaling:
  filesystem:
    papers/*.pdf
    extracts/*.txt
    reports/draft.md
  relational:
    papers
    citations
    claims
    evidence_links
    confidence_scores
```

Review branch diffs to see both graph changes and report changes. Merge only
high-confidence claims and accepted artifacts.

## ETL and Data Pipeline Development

### Motivation

Pipeline changes affect code, generated files, and database tables:

- Pipeline source code.
- YAML/JSON configs.
- Staging outputs.
- Error reports.
- Transformed relational tables.
- Validation results.
- Schema migrations.

Testing a pipeline rewrite against real data should not mutate canonical
tables or overwrite trusted artifacts.

### Why Existing Solutions Are Not Enough

Feature flags and staging tables require every pipeline component to be
branch-aware. Database transactions often cannot wrap long-running jobs, and
they cannot rollback file outputs. Git branches do not isolate staging tables.

### Multi-Store Branch Use

Run the new pipeline in a branch:

```text
branch pipeline_rewrite:
  filesystem:
    pipelines/clean.py
    configs/prod_like.yaml
    validation/report.html
  relational:
    staging_customers
    transformed_orders
    validation_failures
    schema_versions
```

Compare output tables and generated validation reports. Merge only when the
branch passes quality checks.

## Benchmarking and Evaluation

### Motivation

Evaluation systems store both files and relational metrics:

- Benchmark harness code.
- Prompt templates.
- Model output logs.
- Traces and result files.
- Task tables.
- Score tables.
- Run metadata.
- Aggregated metrics.

Changing evaluator code without corresponding score-table isolation can
invalidate results.

### Why Existing Solutions Are Not Enough

Run directories preserve output files but score tables may still be shared.
Database-only isolation misses model output logs and trace files. Git branches
cannot isolate run registries and metrics.

### Multi-Store Branch Use

Create branches per evaluator or model variant:

```text
branch eval_prompt_v3:
  filesystem:
    prompts/system.md
    traces/*.jsonl
    figures/*.png
  relational:
    tasks
    model_outputs
    scores
    aggregate_metrics
```

Compare full evaluation branches and merge the selected benchmark results.

## Knowledge Graph Construction

### Motivation

Knowledge graph construction is a natural multi-store workload:

- Source documents.
- Extracted spans.
- Audit reports.
- Entity and edge tables.
- Provenance records.
- Embedding metadata.
- Review decisions.

Extraction and linking strategies can modify source-derived files and graph
tables together.

### Why Existing Solutions Are Not Enough

Relational branching can isolate graph rows, but not generated extraction
files, reports, or local source caches. File-only branching cannot isolate
graph tables. Separate staging graphs require custom merge logic and do not
generalize to arbitrary file artifacts.

### Multi-Store Branch Use

Create a branch for each extraction strategy:

```text
branch linker_ablation:
  filesystem:
    extracts/*.jsonl
    audits/linker_report.md
  relational:
    graph_nodes
    graph_edges
    provenance
    review_queue
```

Diff graph changes and audit artifacts together. Merge accepted nodes, edges,
and reports as one coherent update.
