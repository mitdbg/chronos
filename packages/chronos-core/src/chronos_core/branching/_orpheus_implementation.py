from __future__ import annotations

"""Portable OrpheusDB implementation model used by the Chronos backend.

The upstream OrpheusDB implementation is a Python 2/PostgreSQL CLI. Its core
storage model is still small and useful:

* ``<dataset>_datatable`` stores immutable records with a synthetic ``rid``.
* ``<dataset>_indextable`` stores version membership. In split-by-rlist it
  maps ``vid -> rlist``; in split-by-vlist it maps ``rid -> vlist``.
* ``<dataset>_versiontable`` stores version metadata and parent/child links.

Chronos uses that model behind ``ChronosBranchContext`` instead of shelling out
to the CLI. The PostgreSQL backend defaults to the Orpheus r-list variant:
integer ``rid`` in the datatable, integer ``vid`` in the versiontable,
``vid`` plus integer-array ``rlist`` in the indextable, and integer-array
parent/children fields in the version table.
"""

from dataclasses import dataclass


UPSTREAM_REPOSITORY = "https://github.com/orpheus-db/implementation"
UPSTREAM_COMMIT = "ef535bf9dcff6b97252fb6b27614c7d262d05ea6"

PUBLIC_SCHEMA = "public."
DATATABLE_SUFFIX = "_datatable"
INDEXTABLE_SUFFIX = "_indextable"
VERSIONTABLE_SUFFIX = "_versiontable"


@dataclass(frozen=True)
class OrpheusDatasetTables:
    """Physical tables corresponding to one Chronos logical table."""

    datatable: str
    indextable: str
    versiontable: str


def orpheus_dataset_tables(prefix: str) -> OrpheusDatasetTables:
    """Return Orpheus-style table names for a sanitized dataset prefix."""

    return OrpheusDatasetTables(
        datatable=f"{prefix}{DATATABLE_SUFFIX}",
        indextable=f"{prefix}{INDEXTABLE_SUFFIX}",
        versiontable=f"{prefix}{VERSIONTABLE_SUFFIX}",
    )
