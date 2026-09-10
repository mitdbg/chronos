"""Check the installed artifact's feature flags and disabled-feature errors."""
import pytest
from chronos_core import _native_interval as native


def test_feature_flags_match_bindings():
    assert native.has_filesystem == hasattr(native, "NativeChronosFSStore")
    assert native.has_s3 == hasattr(native, "NativeChronosLakeServer")


@pytest.mark.skipif(native.has_duckdb, reason="DuckDB enabled in this build")
def test_disabled_duckdb_reports_build_option(tmp_path):
    with pytest.raises(RuntimeError, match="CHRONOS_WITH_DUCKDB"):
        native.NativeSqlConnection(f"duckdb:///{tmp_path / 'disabled.duckdb'}")


@pytest.mark.skipif(native.has_filesystem, reason="filesystem enabled in this build")
def test_disabled_filesystem_reports_build_option(tmp_path):
    from chronos_core.workspace import ChronosFSStore
    from chronos_core.workspace.chronosfs.store import ChronosFSError
    with pytest.raises(ChronosFSError, match="CHRONOS_WITH_FILESYSTEM"):
        ChronosFSStore.connect(f"sqlite:///{tmp_path / 'disabled.sqlite'}")
