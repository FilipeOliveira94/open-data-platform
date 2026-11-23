"""Read Iceberg table metadata from the JDBC catalog (P3-08).

The Bronze step checks that a table it is about to ``MERGE`` into has the
``dt`` identity partition (ADR-0026). A table created before P3-08 has no
partition spec, and its ``MERGE`` would still scan all history. The check
reads the catalog row (``iceberg_tables.metadata_location``) and the current
metadata JSON from object storage; it needs no query engine.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

from open_data_platform.lakehouse.promotion_registry import state_transaction

if TYPE_CHECKING:
    from botocore.client import BaseClient

CATALOG_NAME = "local"


class CatalogMetadataError(RuntimeError):
    """The catalog row or metadata file of a table could not be read."""


def identity_partition_columns(metadata: Mapping[str, object]) -> tuple[str, ...]:
    """Column names of the identity fields of the table's default spec."""
    spec_id = metadata.get("default-spec-id")
    columns = {
        int(column["id"]): str(column["name"])
        for schema in _list(metadata.get("schemas"))
        if schema.get("schema-id") == metadata.get("current-schema-id")
        for column in _list(schema.get("fields"))
    }
    for spec in _list(metadata.get("partition-specs")):
        if spec.get("spec-id") != spec_id:
            continue
        return tuple(
            columns.get(int(item["source-id"]), "")
            for item in _list(spec.get("fields"))
            if item.get("transform") == "identity"
        )
    return ()


def table_partition_columns(
    client: BaseClient,
    env: Mapping[str, str],
    namespace: str,
    table: str,
) -> tuple[str, ...] | None:
    """Identity partition columns of a catalog table, or ``None`` if absent."""
    with state_transaction(env, "iceberg catalog") as conn:
        row = conn.execute(
            "SELECT metadata_location FROM iceberg_tables "
            "WHERE catalog_name = %s AND table_namespace = %s AND table_name = %s",
            (CATALOG_NAME, namespace, table),
        ).fetchone()
    if row is None:
        return None
    location = str(row[0])
    scheme, _, rest = location.partition("://")
    bucket, _, key = rest.partition("/")
    if scheme not in {"s3", "s3a"} or not bucket or not key:
        msg = f"{namespace}.{table}: metadata location is not an S3 path"
        raise CatalogMetadataError(msg)
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        metadata = json.loads(body)
    except (OSError, ValueError, KeyError) as exc:
        msg = f"{namespace}.{table}: metadata unreadable ({type(exc).__name__})"
        raise CatalogMetadataError(msg) from exc
    return identity_partition_columns(metadata)


def _list(value: object) -> list[dict]:
    return (
        [item for item in value if isinstance(item, dict)]
        if isinstance(value, list)
        else []
    )
