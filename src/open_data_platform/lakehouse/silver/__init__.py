"""Silver layer: typed, deduplicated tables built from Bronze.

Bronze stays one generic path. Silver is split by ``source_id`` so each
source can declare its own cast, JSON expansion, and survivor rules.
Airflow (Phase 3) will call these entrypoints; DAGs must not reimplement
the transforms.
"""

from open_data_platform.lakehouse.silver.common import (
    DedupPolicy,
    JsonExpansion,
    OutputColumn,
    SilverError,
    TimestampStyle,
    date_sql,
    json_array,
    json_path_double,
    json_path_string,
    json_path_timestamp,
    json_typed,
    render_silver_sql,
    silver_table_name,
    timestamp_sql,
)
from open_data_platform.lakehouse.write_mode import WriteMode, WriteModeError

__all__ = [
    "DedupPolicy",
    "JsonExpansion",
    "OutputColumn",
    "SilverError",
    "TimestampStyle",
    "WriteMode",
    "WriteModeError",
    "date_sql",
    "json_array",
    "json_path_double",
    "json_path_string",
    "json_path_timestamp",
    "json_typed",
    "render_silver_sql",
    "silver_table_name",
    "timestamp_sql",
]
