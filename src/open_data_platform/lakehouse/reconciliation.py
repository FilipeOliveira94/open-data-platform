"""Live count checks derived from a run, not from a pinned snapshot.

Bronze checks compare Trino to the row counts read from raw objects in that
promotion. Silver checks compare Trino Silver to distinct Bronze keys (or
distinct payload rows) using the same partition list as the Spark writer.
Shape checks compare a column rule to the Silver table itself.

The expected side of every check is ``ok`` or the raw-derived count. Snapshot
sizes such as DummyJSON 194 are not stored here.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

from open_data_platform.contracts.model import DataContract
from open_data_platform.lakehouse.bronze import bronze_table_name
from open_data_platform.lakehouse.silver.common import (
    DedupPolicy,
    partition_key_names,
    silver_table_name,
)

_SIMPLE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_COUNT_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*\.[a-z0-9][a-z0-9_-]*$")

CATALOG = "iceberg"


class ReconciliationError(ValueError):
    """A count file or a live check comparison is not usable."""


def format_counts(counts: Sequence[tuple[str, int]]) -> str:
    """Serialize ``source.dataset`` counts as ``name<TAB>count`` lines."""
    lines: list[str] = []
    for name, count in counts:
        _require_count_name(name)
        _require_count(count)
        lines.append(f"{name}\t{count}\n")
    return "".join(lines)


def parse_counts(text: str) -> dict[str, int]:
    """Parse a count file written by :func:`format_counts`."""
    parsed: dict[str, int] = {}
    for line_no, line in enumerate(text.splitlines(), start=1):
        if line.strip() == "":
            continue
        parts = line.split("\t")
        if len(parts) != 2:
            msg = f"line {line_no}: expected name and count"
            raise ReconciliationError(msg)
        name, raw = parts
        _require_count_name(name)
        if not raw.isdecimal():
            msg = f"line {line_no}: count must be a non-negative integer"
            raise ReconciliationError(msg)
        if name in parsed:
            msg = f"line {line_no}: duplicate count for {name}"
            raise ReconciliationError(msg)
        parsed[name] = int(raw)
    return parsed


def require_expected(expected: str, actual: str, *, check: str) -> None:
    """Fail with the check name and both sides when a live value drifts."""
    if actual != expected:
        msg = f"{check}: expected {expected} got {actual}"
        raise ReconciliationError(msg)


def bronze_count_checks(
    counts: Mapping[str, int],
    contracts: Sequence[DataContract],
) -> tuple[tuple[str, str], ...]:
    """Trino ``COUNT(*)`` checks whose expected side is the raw read."""
    expected_names = {
        f"{contract.source_id}.{contract.dataset}": contract for contract in contracts
    }
    missing = sorted(set(expected_names) - set(counts))
    extra = sorted(set(counts) - set(expected_names))
    if missing or extra:
        msg = (
            f"count file does not match the contracts: missing={missing} extra={extra}"
        )
        raise ReconciliationError(msg)
    checks: list[tuple[str, str]] = []
    for name in sorted(expected_names):
        contract = expected_names[name]
        table = _qualified(
            "bronze",
            bronze_table_name(contract.source_id, contract.dataset),
        )
        sql = f"SELECT COUNT(*) AS row_count FROM {table}"
        checks.append((str(counts[name]), _one_line(sql)))
    return tuple(checks)


def silver_live_checks(
    entries: Sequence[tuple[DataContract, DedupPolicy]],
) -> tuple[tuple[str, str], ...]:
    """Relational Silver checks plus shape rules. Expected value is ``ok``."""
    relational = tuple(
        ("ok", silver_relation_sql(contract, policy)) for contract, policy in entries
    )
    return relational + shape_checks()


def silver_relation_sql(contract: DataContract, policy: DedupPolicy) -> str:
    """One Trino statement: ``ok``, or both counts when Silver drifts from Bronze."""
    keys = partition_key_names(contract, policy)
    quoted_keys = ", ".join(trino_ident(name) for name in keys)
    present = " AND ".join(f"{trino_ident(name)} IS NOT NULL" for name in keys)
    bronze = _qualified(
        "bronze",
        bronze_table_name(contract.source_id, contract.dataset),
    )
    silver = _qualified(
        "silver",
        silver_table_name(contract.source_id, contract.dataset),
    )
    label = f"{contract.source_id}.{contract.dataset}"
    sql = (
        "SELECT CASE "
        "WHEN silver_count = bronze_groups THEN 'ok' "
        "ELSE 'mismatch "
        f"{label} silver=' || CAST(silver_count AS varchar) "
        "|| ' bronze=' || CAST(bronze_groups AS varchar) "
        "END "
        "FROM ("
        f"SELECT (SELECT COUNT(*) FROM {silver}) AS silver_count, "
        "(SELECT COUNT(*) FROM ("
        f"SELECT 1 FROM {bronze} WHERE {present} GROUP BY {quoted_keys}"
        ")) AS bronze_groups)"
    )
    return _one_line(sql)


def shape_checks() -> tuple[tuple[str, str], ...]:
    """Column rules whose expected count is the Silver table, not a snapshot size."""
    products = _qualified("silver", "dummyjson_products__products")
    clickstream = _qualified("silver", "demo_storefront__clickstream")
    expanded = (
        "SELECT CASE "
        "WHEN total = expanded THEN 'ok' "
        "ELSE 'mismatch dummyjson_products.products expanded=' "
        "|| CAST(expanded AS varchar) || ' total=' || CAST(total AS varchar) "
        "END FROM ("
        f"SELECT (SELECT COUNT(*) FROM {products}) AS total, "
        f"(SELECT COUNT(*) FROM {products} WHERE dimensions_width IS NOT NULL "
        "AND meta_barcode IS NOT NULL AND cardinality(tags) >= 1) AS expanded)"
    )
    product_on_cart = (
        "SELECT CASE "
        "WHEN missing = 0 THEN 'ok' "
        "ELSE 'mismatch demo_storefront.clickstream "
        "cart_or_purchase_without_product=' || CAST(missing AS varchar) "
        "END FROM ("
        f"SELECT COUNT(*) AS missing FROM {clickstream} "
        "WHERE event_type IN ('add_to_cart', 'purchase') AND product_id IS NULL)"
    )
    return (
        ("ok", _one_line(expanded)),
        ("ok", _one_line(product_on_cart)),
    )


def trino_ident(name: str) -> str:
    """Quote a Trino identifier when it is not a simple name."""
    if _SIMPLE_IDENT.fullmatch(name):
        return name
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _qualified(namespace: str, table: str) -> str:
    return f"{CATALOG}.{namespace}.{table}"


def _one_line(sql: str) -> str:
    return " ".join(sql.split())


def _require_count_name(name: str) -> None:
    if _COUNT_NAME.fullmatch(name) is None:
        msg = f"count name must be source.dataset: {name!r}"
        raise ReconciliationError(msg)


def _require_count(count: int) -> None:
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        msg = f"count must be a non-negative integer: {count!r}"
        raise ReconciliationError(msg)
