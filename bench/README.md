# Branching Backend Benchmarks

This folder contains benchmarks for the bolt-on branching physical backends.

Run a quick smoke benchmark:

```bash
PYTHONPATH=packages/janus-core/src \
python bench/branching_backends.py --quick
```

Run a broader benchmark:

```bash
PYTHONPATH=packages/janus-core/src \
python3 bench/branching_backends.py \
  --dataset-sizes 100000 \
  --depths 1,4,8 \
  --read-ops 5000 \
  --write-ops 5000 \
  --branch-mutations 100
```

For each dataset size and depth, the benchmark first loads the data, registers
tables, and creates indexes outside the timed section. It then measures branch
creation while building one branch chain of the requested depth. Reads,
creation while building one branch chain of the requested depth. After each
branch is created, the benchmark applies branch-local updates, inserts, and
deletes before creating the next child branch. Reads, updates, inserts, and
deletes are then measured on the terminal branch of that mutated chain. Finally,
branch deletion is measured by deleting the chain in reverse.

The benchmark writes:

- `results.csv`
- `results.json`
- `summary.png`
- one matplotlib PNG chart per benchmark metric
- `README.md` linking the generated plots

By default, output goes under `.benchmarks/branching-<timestamp>/`.
