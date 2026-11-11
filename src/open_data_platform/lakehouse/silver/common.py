"""Shared Silver typing, deduplication SQL, and table naming.

Source modules supply a ``DedupPolicy`` per dataset. This module does not
talk to MinIO or Spark.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from open_data_platform.lakehouse.bronze import METADATA_COLUMNS, bronze_table_name
from open_data_platform.lakehouse.write_mode import (
    CURRENT_WRITE_MODE,
    WriteMode,
    WriteModeError,
    render_append_only_sql,
    render_incremental_merge_sql,
    require_implemented,
)

if TYPE_CHECKING:
    from open_data_platform.contracts.model import ContractField, DataContract

_SEGMENT_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")

ICEBERG_TYPES: dict[str, str] = {
    "string": "STRING",
    "json": "STRING",
    "integer": "BIGINT",
    "number": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
}

DEFAULT_ORDER: tuple[str, ...] = (
    "_ingestion_ts",
    "execution_id",
    "raw_object_key",
    "record_index",
)


class SilverError(ValueError):
    """Silver SQL cannot be built for the contract or policy."""


class TimestampStyle(Enum):
    """How Bronze string timestamps are parsed for one source."""

    OLIST_CSV = "olist_csv"
    ISO = "iso"


@dataclass(frozen=True, slots=True)
class OutputColumn:
    """One Silver column: DDL type and SELECT expression over Bronze names."""

    name: str
    iceberg_type: str
    select_sql: str


@dataclass(frozen=True, slots=True)
class JsonExpansion:
    """Replace one Bronze ``json`` STRING column with typed Silver columns.

    The source field is omitted from the Silver table. ``columns`` are added
    in order. Expressions may reference the Bronze column by its quoted name.
    """

    source_field: str
    columns: tuple[OutputColumn, ...]


@dataclass(frozen=True, slots=True)
class DedupPolicy:
    """Survivor rules for one Silver dataset.

    ``partition_keys`` defaults to the contract primary key. An empty tuple
    with ``partition_payload_fields=True`` deduplicates identical payload
    rows (geolocation). ``order_by`` lists columns available after the typed
    select; later columns in ``DEFAULT_ORDER`` are appended when missing.
    ``json_expansions`` flatten selected Bronze JSON strings into columns.
    ``write_mode`` defaults to full overwrite (the only mode implemented).
    """

    partition_keys: tuple[str, ...] | None = None
    partition_payload_fields: bool = False
    order_by: tuple[str, ...] = ()
    timestamp_style: TimestampStyle = TimestampStyle.OLIST_CSV
    json_expansions: tuple[JsonExpansion, ...] = ()
    write_mode: WriteMode = CURRENT_WRITE_MODE


def require_bucket_name(bucket: str) -> str:
    """Reject bucket names that cannot be embedded in an ``s3a://`` path."""
    if _BUCKET_RE.fullmatch(bucket) is None:
        msg = f"bucket name is not a simple S3 identifier: {bucket!r}"
        raise SilverError(msg)
    return bucket


def silver_table_name(source_id: str, dataset: str) -> str:
    """Iceberg table name for one contract. The namespace is ``silver``."""
    _require_segment("source_id", source_id)
    _require_segment("dataset", dataset)
    return f"{source_id}__{dataset}"


def silver_columns(contract: DataContract) -> tuple[str, ...]:
    """Default column names before JSON expansions (metadata + payload)."""
    columns = default_output_columns(contract, TimestampStyle.ISO)
    return tuple(col.name for col in columns)


def default_output_columns(
    contract: DataContract,
    style: TimestampStyle,
) -> tuple[OutputColumn, ...]:
    """Metadata and contract/envelope columns with default casts."""
    columns: list[OutputColumn] = []
    for name in METADATA_COLUMNS:
        columns.append(
            OutputColumn(
                name=name,
                iceberg_type="STRING",
                select_sql=_quote_ident(name),
            )
        )
    for name in _payload_column_names(contract):
        field_def = _field_by_name(contract, name)
        columns.append(
            OutputColumn(
                name=name,
                iceberg_type=iceberg_type(field_def.field_type),
                select_sql=_cast_expr(_quote_ident(name), field_def, style),
            )
        )
    return tuple(columns)


def resolve_output_columns(
    contract: DataContract,
    policy: DedupPolicy,
) -> tuple[OutputColumn, ...]:
    """Apply JSON expansions on top of the default Silver columns."""
    replaced = {item.source_field: item for item in policy.json_expansions}
    payload_names = set(_payload_column_names(contract))
    for source_field in replaced:
        if source_field not in payload_names:
            msg = (
                f"{contract.source_id}.{contract.dataset}: "
                f"json expansion source {source_field!r} is not a payload column"
            )
            raise SilverError(msg)
    out: list[OutputColumn] = []
    for column in default_output_columns(contract, policy.timestamp_style):
        expansion = replaced.get(column.name)
        if expansion is None:
            out.append(column)
            continue
        out.extend(expansion.columns)
    return tuple(out)


def iceberg_type(field_type: str) -> str:
    """Map a contract logical type to an Iceberg / Spark SQL type name."""
    mapped = ICEBERG_TYPES.get(field_type)
    if mapped is None:
        msg = f"unsupported field type: {field_type!r}"
        raise SilverError(msg)
    return mapped


def dataset_sql_name(contract: DataContract) -> str:
    """File name for one dataset Spark script."""
    return f"{silver_table_name(contract.source_id, contract.dataset)}.sql"


def timestamp_sql(expr: str, style: TimestampStyle) -> str:
    """Cast a Spark SQL expression to TIMESTAMP using the given style."""
    if style is TimestampStyle.OLIST_CSV:
        return (
            "COALESCE("
            f"TRY_TO_TIMESTAMP({expr}, 'yyyy-MM-dd HH:mm:ss'), "
            f"TRY_TO_TIMESTAMP({expr})"
            ")"
        )
    return (
        "COALESCE("
        f"TRY_TO_TIMESTAMP({expr}), "
        f"TRY_TO_TIMESTAMP(regexp_replace({expr}, 'Z$', ''), "
        "\"yyyy-MM-dd'T'HH:mm:ss.SSS\"), "
        f"TRY_TO_TIMESTAMP(regexp_replace({expr}, 'Z$', ''), "
        "\"yyyy-MM-dd'T'HH:mm:ss\")"
        ")"
    )


def date_sql(expr: str) -> str:
    """Cast a Spark SQL expression to DATE (no try_to_date on this runtime)."""
    return (
        "COALESCE("
        f"TRY_CAST({expr} AS DATE), "
        f"TO_DATE({expr}), "
        f"TO_DATE({expr}, 'yyyy-MM-dd')"
        ")"
    )


def json_path_string(bronze_column: str, json_path: str) -> str:
    """``get_json_object`` over a Bronze STRING column (quoted name or expr)."""
    return f"get_json_object({bronze_column}, '{json_path}')"


def json_path_double(bronze_column: str, json_path: str) -> str:
    """Parse a JSON object number field as DOUBLE."""
    return f"TRY_CAST({json_path_string(bronze_column, json_path)} AS DOUBLE)"


def json_path_timestamp(
    bronze_column: str,
    json_path: str,
    style: TimestampStyle = TimestampStyle.ISO,
) -> str:
    """Parse a JSON object timestamp field."""
    return timestamp_sql(json_path_string(bronze_column, json_path), style)


def json_array(bronze_column: str, element_type_sql: str) -> str:
    """Parse a Bronze JSON array STRING into a Spark ARRAY type."""
    return f"from_json({bronze_column}, 'ARRAY<{element_type_sql}>')"


def json_typed(bronze_column: str, spark_type_sql: str) -> str:
    """Parse a Bronze JSON STRING with an arbitrary Spark type DDL string."""
    return f"from_json({bronze_column}, '{spark_type_sql}')"


def render_silver_sql(
    contract: DataContract,
    warehouse_bucket: str,
    policy: DedupPolicy,
) -> str:
    """Spark SQL that creates one Silver table and overwrites it from Bronze.

    Only ``WriteMode.FULL_OVERWRITE`` is implemented: drop/recreate + overwrite.
    Incremental modes branch to placeholders in ``lakehouse.write_mode``.
    """
    context = f"{contract.source_id}.{contract.dataset}"
    try:
        if policy.write_mode is WriteMode.INCREMENTAL_MERGE:
            return render_incremental_merge_sql(context=context)
        if policy.write_mode is WriteMode.APPEND_ONLY:
            return render_append_only_sql(context=context)
        require_implemented(policy.write_mode, context=context)
    except WriteModeError as exc:
        raise SilverError(str(exc)) from exc
    bucket = require_bucket_name(warehouse_bucket)
    source_id = _require_segment("source_id", contract.source_id)
    dataset = _require_segment("dataset", contract.dataset)
    table = silver_table_name(source_id, dataset)
    bronze = bronze_table_name(source_id, dataset)
    quoted_silver = _quote_ident(table)
    quoted_bronze = _quote_ident(bronze)
    columns = resolve_output_columns(contract, policy)
    column_defs = ",\n".join(
        f"    {_quote_ident(col.name)} {col.iceberg_type}" for col in columns
    )
    location = f"s3a://{bucket}/silver/{source_id}/{dataset}"
    typed_select = ",\n".join(
        f"  {col.select_sql} AS {_quote_ident(col.name)}" for col in columns
    )
    partition = _partition_columns(contract, policy)
    order_sql = _order_by_sql(policy)
    pk_filter = _primary_key_filter(partition)
    names = tuple(col.name for col in columns)
    column_list = ", ".join(_quote_ident(name) for name in names)
    outer_select = ",\n".join(f"  {_quote_ident(name)}" for name in names)
    inner_select = ",\n".join(f"    {_quote_ident(name)}" for name in names)
    lines = [
        f"-- {source_id}.{dataset}",
        f"-- write_mode={policy.write_mode.value} (full load + overwrite)",
        "CREATE NAMESPACE IF NOT EXISTS silver;",
        "",
        f"DROP TABLE IF EXISTS silver.{quoted_silver};",
        f"CREATE TABLE silver.{quoted_silver} (",
        column_defs,
        ") USING iceberg",
        f"LOCATION '{location}'",
        "TBLPROPERTIES ('format-version' = '2');",
        "",
        f"INSERT OVERWRITE silver.{quoted_silver} ({column_list})",
        "SELECT",
        outer_select,
        "FROM (",
        "  SELECT",
        inner_select + ",",
        "    ROW_NUMBER() OVER (",
        f"      PARTITION BY {partition}",
        f"      ORDER BY {order_sql}",
        "    ) AS _silver_rn",
        "  FROM (",
        "    SELECT",
        typed_select,
        f"    FROM bronze.{quoted_bronze}",
        "  ) typed",
        f"  WHERE {pk_filter}",
        ") ranked",
        "WHERE _silver_rn = 1;",
        "",
        f"SELECT COUNT(*) AS row_count FROM silver.{quoted_silver};",
    ]
    return "\n".join(lines) + "\n"


def render_source_scripts(
    entries: Sequence[tuple[DataContract, DedupPolicy]],
    warehouse_bucket: str,
) -> tuple[tuple[str, str], ...]:
    """One ``(filename, sql)`` pair per dataset."""
    return tuple(
        (
            dataset_sql_name(contract),
            render_silver_sql(contract, warehouse_bucket, policy),
        )
        for contract, policy in entries
    )


def _cast_expr(quoted: str, field: ContractField, style: TimestampStyle) -> str:
    kind = field.field_type
    if kind in {"string", "json"}:
        return quoted
    if kind == "integer":
        return f"TRY_CAST({quoted} AS BIGINT)"
    if kind == "number":
        return f"TRY_CAST({quoted} AS DOUBLE)"
    if kind == "boolean":
        return (
            "CASE "
            f"WHEN lower({quoted}) IN ('true') THEN true "
            f"WHEN lower({quoted}) IN ('false') THEN false "
            "ELSE CAST(NULL AS BOOLEAN) END"
        )
    if kind == "date":
        return date_sql(quoted)
    if kind == "timestamp":
        return timestamp_sql(quoted, style)
    msg = f"unsupported field type: {kind!r}"
    raise SilverError(msg)


def partition_key_names(
    contract: DataContract,
    policy: DedupPolicy,
) -> tuple[str, ...]:
    """Columns Silver groups by: the full payload, or the primary key.

    Reconciliation counts distinct values of this same list on Bronze.
    """
    if policy.partition_payload_fields:
        names = _payload_column_names(contract)
        if not names:
            msg = f"{contract.source_id}.{contract.dataset}: no payload columns"
            raise SilverError(msg)
        return names
    keys = (
        contract.primary_key if policy.partition_keys is None else policy.partition_keys
    )
    if not keys:
        msg = (
            f"{contract.source_id}.{contract.dataset}: "
            "empty primary key needs partition_payload_fields"
        )
        raise SilverError(msg)
    payload = set(_payload_column_names(contract))
    for key in keys:
        if key not in payload:
            msg = (
                f"{contract.source_id}.{contract.dataset}: "
                f"partition key {key!r} is not a payload column"
            )
            raise SilverError(msg)
    return keys


def _partition_columns(contract: DataContract, policy: DedupPolicy) -> str:
    return ", ".join(
        _quote_ident(name) for name in partition_key_names(contract, policy)
    )


def _primary_key_filter(partition_sql: str) -> str:
    parts = [part.strip() for part in partition_sql.split(",")]
    return " AND ".join(f"{part} IS NOT NULL" for part in parts)


def _order_by_sql(policy: DedupPolicy) -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for name in (*policy.order_by, *DEFAULT_ORDER):
        if name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    chunks: list[str] = []
    for name in ordered:
        if name == "record_index":
            chunks.append("CAST(`record_index` AS BIGINT) DESC")
        else:
            chunks.append(f"{_quote_ident(name)} DESC")
    return ", ".join(chunks)


def _payload_column_names(contract: DataContract) -> tuple[str, ...]:
    names = [field.name for field in contract.fields]
    locator = contract.record_locator
    for field_def in contract.envelope_fields:
        if locator is not None and field_def.name == locator:
            continue
        names.append(field_def.name)
    return tuple(names)


def _field_by_name(contract: DataContract, name: str) -> ContractField:
    for field_def in (*contract.fields, *contract.envelope_fields):
        if field_def.name == name:
            return field_def
    msg = f"{contract.source_id}.{contract.dataset}: unknown column {name!r}"
    raise SilverError(msg)


def _require_segment(name: str, value: str) -> str:
    if _SEGMENT_RE.fullmatch(value) is None:
        msg = f"{name} must be a raw path segment: {value!r}"
        raise SilverError(msg)
    return value


def _quote_ident(name: str) -> str:
    if name == "" or "`" in name or "\x00" in name:
        msg = f"identifier cannot be quoted: {name!r}"
        raise SilverError(msg)
    return f"`{name}`"


def quote_ident(name: str) -> str:
    """Public wrapper for Spark identifier quoting."""
    return _quote_ident(name)
