# Installation

Version 0.2.0a1 is a local experimental release candidate. These instructions
do not assume it has been uploaded to PyPI.

## Install a wheel

Create a virtual environment and install a wheel matching its Python and platform:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install /path/to/chronos_core-0.2.0a1-<python>-<abi>-<platform>.whl
python -c "from chronos_core import _native_interval as n; print(n.has_filesystem, n.has_duckdb, n.has_s3)"
```

Candidate scripts target Linux x86-64. Raw linux_x86_64 wheels require compatible
system libraries. Auditwheel-repaired wheels bundle redistributable dependencies
and carry the resulting manylinux tag. Do not manually relabel raw wheels.
Other platforms require separate source-build validation.

## Source build

On Ubuntu, install the relational build dependencies:

```sh
sudo apt-get install build-essential python3-dev libsqlite3-dev libpq-dev libboost-dev
python -m pip install ./packages/chronos-core
```

Pip creates an isolated build environment. Downloads include libpg_query 17-6.2.2
and nlohmann/json 3.11.3 when absent locally. CMake FetchContent source overrides
can supply pre-fetched dependencies for controlled builds.

## Optional features

ChronosFS needs Linux and libfuse3. S3 also needs Boost.System, CURL, OpenSSL,
and pugixml. Build all features with:

```sh
sudo apt-get install libfuse3-dev libboost-system-dev libcurl4-openssl-dev libssl-dev libpugixml-dev
python -m pip install './packages/chronos-core[duckdb,qdrant,mcp]' \
  -Ccmake.define.CHRONOS_WITH_FILESYSTEM=ON \
  -Ccmake.define.CHRONOS_WITH_DUCKDB=ON \
  -Ccmake.define.CHRONOS_WITH_S3=ON
```

DuckDB downloads the pinned 1.5.4 Linux x86-64 library. Elsewhere supply
`-Ccmake.define.CHRONOS_DUCKDB_ROOT=/path/to/library-and-header`. Installing a
Python extra does not enable a missing compiled driver.

Direct ChronosFS API access needs no mount. FUSE mounting additionally needs
/dev/fuse and permissions. A relational build reports an actionable error when
asked to construct a filesystem store.

See [contributor instructions](../CONTRIBUTING.md). Test installed artifacts in
isolated environments: PYTHONPATH alone cannot establish which C++ binary runs.
