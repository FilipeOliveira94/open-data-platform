"""Silver rules for ``ibge_localidades``."""

from __future__ import annotations

from open_data_platform.lakehouse.silver.common import DedupPolicy, TimestampStyle

SOURCE_ID = "ibge_localidades"

POLICIES: dict[str, DedupPolicy] = {
    "municipios": DedupPolicy(timestamp_style=TimestampStyle.ISO),
}
