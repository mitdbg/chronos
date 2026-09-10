# Preparing an experimental release locally

No command below uploads an artifact or changes repository visibility. Keep the
release local until the source review, history review, and test report have been
approved.

Build from the package's source distribution to catch missing source files.
Use a directory outside the checkout for build output.

```sh
export CHRONOS_RELEASE_DIR=$(mktemp -d /tmp/chronos-release.XXXXXX)
python -m pip install build
python -m build packages/chronos-core --outdir "$CHRONOS_RELEASE_DIR" \
  -Cbuild-dir="$CHRONOS_RELEASE_DIR/build" \
  -Ccmake.define.CHRONOS_WITH_FILESYSTEM=ON \
  -Ccmake.define.CHRONOS_WITH_DUCKDB=ON \
  -Ccmake.define.CHRONOS_WITH_S3=ON
```

Also build the default relational-only configuration and test it in a fresh
environment. Do not use `--system-site-packages`; a previously installed editable
package can load an older compiled extension even when the new wheel is present.

Record the Git revision and uncommitted diff, compiler, Python version, feature
flags, dependency versions, and test commands alongside each artifact. Keep test
logs and report failures and skips explicitly. An artifact built from a dirty
tree is a candidate, not a reproducible tagged release.

For a Linux wheel intended for another machine, inspect it with `auditwheel
show` and repair it with `auditwheel repair` in the intended manylinux build
environment. Include required dependency licenses in the repaired wheel and
test the result in a clean container. A wheel built on one Linux distribution
is not automatically portable. Do not change its platform tag by hand.

Before any eventual public release, review all reachable Git history for
credentials, private datasets, experiment traces, and license obligations. A
clean working tree does not remove old files from history. Secret scanning does
not establish that historical data is suitable for publication. Resolve findings
before pushing, tagging a public release, or changing repository visibility.

The PostgreSQL implementation has a separate repository and PostgreSQL license.
Build its local image with `docker/chronos/Dockerfile` and run its smoke test;
do not push the fork to its upstream `postgres/postgres` remote.
