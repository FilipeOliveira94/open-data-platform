"""The tables of each daily drop, in the order they are written."""

from __future__ import annotations

from open_data_platform.simulators.bioguardia import FINANCE_SOURCE, OPS_SOURCE
from open_data_platform.simulators.bioguardia.spec import TableSpec
from open_data_platform.simulators.bioguardia.tables_master import MASTER_TABLES
from open_data_platform.simulators.bioguardia.tables_monitoring import MONITORING_TABLES
from open_data_platform.simulators.bioguardia.tables_ops import OPS_TABLES
from open_data_platform.simulators.bioguardia.tables_other import (
    FINANCE_TABLES,
    OTHER_OPS_TABLES,
)

# Track points come before patrols: the patrol row reuses the track's distance.
OPS_CATALOG: tuple[TableSpec, ...] = (
    *MASTER_TABLES,
    *OPS_TABLES,
    *MONITORING_TABLES,
    *OTHER_OPS_TABLES,
)
FINANCE_CATALOG: tuple[TableSpec, ...] = FINANCE_TABLES
CATALOGS: dict[str, tuple[TableSpec, ...]] = {
    OPS_SOURCE: OPS_CATALOG,
    FINANCE_SOURCE: FINANCE_CATALOG,
}
SOURCE_SYSTEMS = {
    OPS_SOURCE: "BioGuardia Operations Hub",
    FINANCE_SOURCE: "BioGuardia Finance",
}


def all_tables() -> tuple[TableSpec, ...]:
    return OPS_CATALOG + FINANCE_CATALOG
