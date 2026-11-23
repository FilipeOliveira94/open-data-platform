"""Live count checks derived from a run, not from a pinned snapshot.

Bronze checks compare Trino to the row total that the promotion registry
holds for each dataset a refresh wrote (ADR-0024): every registered object's
rows, so the Bronze table equals the sum of what was promoted into it.
Silver checks compare Trino Silver to distinct Bronze keys (or distinct
payload rows) using the same partition list as the Spark writer.
Shape checks compare a column rule to the Silver table itself.

The expected side of every check is ``ok`` or the raw-derived count. Snapshot
sizes such as DummyJSON 194 are not stored here.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

from open_data_platform.contracts.model import QUARANTINE_EXCLUDE, DataContract
from open_data_platform.lakehouse.bronze import QUARANTINE_COLUMN, bronze_table_name
from open_data_platform.lakehouse.silver.common import (
    AUDIT_SCHEMA,
    AUDIT_TABLE,
    QUARANTINED_COLUMN,
    DedupPolicy,
    partition_key_names,
    quality_metrics,
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
    """Trino ``COUNT(*)`` checks for the datasets a refresh wrote.

    The expected side is the registry row total of each dataset. A refresh
    that wrote no table has an empty count file and no checks.
    """
    expected_names = {
        f"{contract.source_id}.{contract.dataset}": contract for contract in contracts
    }
    extra = sorted(set(counts) - set(expected_names))
    if extra:
        msg = f"count file names unknown datasets: {extra}"
        raise ReconciliationError(msg)
    checks: list[tuple[str, str]] = []
    for name in sorted(counts):
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
    quality = tuple(
        check
        for contract, policy in entries
        for check in silver_quality_checks(contract, policy)
    )
    rebuilt = {(contract.source_id, contract.dataset) for contract, _p in entries}
    return relational + quality + shape_checks(rebuilt)


def silver_quality_checks(
    contract: DataContract, policy: DedupPolicy
) -> tuple[tuple[str, str], ...]:
    """P3-07 checks: counts recorded by the latest write, and exclusion held.

    The latest run that wrote quality counts for the dataset recorded every
    metric ``quality_metrics`` names. A ``quarantine_exclude`` table holds no
    row with ``_quarantined = true``.
    """
    label = f"{contract.source_id}.{contract.dataset}"
    audit = _qualified(AUDIT_SCHEMA, AUDIT_TABLE)
    expected = len(quality_metrics(contract, policy))
    scope = f"source_id = '{contract.source_id}' AND dataset = '{contract.dataset}'"
    recorded = (
        "SELECT CASE "
        f"WHEN recorded = {expected} THEN 'ok' "
        f"ELSE 'mismatch {label} quality_counts=' || CAST(recorded AS varchar) "
        f"|| ' expected={expected}' END "
        f"FROM (SELECT COUNT(*) AS recorded FROM {audit} WHERE {scope} "
        "AND run_label = ("
        f"SELECT max_by(run_label, recorded_at) FROM {audit} WHERE {scope}))"
    )
    checks = [("ok", _one_line(recorded))]
    if contract.on_invalid == QUARANTINE_EXCLUDE:
        silver = _qualified(
            "silver", silver_table_name(contract.source_id, contract.dataset)
        )
        excluded = (
            "SELECT CASE WHEN kept = 0 THEN 'ok' "
            f"ELSE 'mismatch {label} quarantined_in_silver=' "
            "|| CAST(kept AS varchar) END "
            f"FROM (SELECT COUNT(*) AS kept FROM {silver} "
            f"WHERE {trino_ident(QUARANTINED_COLUMN)})"
        )
        checks.append(("ok", _one_line(excluded)))
    return tuple(checks)


def silver_relation_sql(contract: DataContract, policy: DedupPolicy) -> str:
    """One Trino statement: ``ok``, or both counts when Silver drifts from Bronze."""
    keys = partition_key_names(contract, policy)
    quoted_keys = ", ".join(trino_ident(name) for name in keys)
    present = " AND ".join(f"{trino_ident(name)} IS NOT NULL" for name in keys)
    if contract.on_invalid == QUARANTINE_EXCLUDE:
        # Silver drops a quarantined row before it picks a survivor (P3-07).
        present += f" AND {trino_ident(QUARANTINE_COLUMN)} IS NULL"
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
        f"SELECT 1 FROM {_bronze_scope(contract.update_pattern, bronze)} "
        f"WHERE {present} GROUP BY {quoted_keys}"
        ")) AS bronze_groups)"
    )
    return _one_line(sql)


def _bronze_scope(update_pattern: str, bronze: str) -> str:
    """The Bronze rows the Silver writer reads, for the same update pattern.

    Mirrors ``silver.common._bronze_selection``: the latest complete execution
    for ``snapshot`` and ``full_refresh``, every complete execution for
    ``append_only``, and every row for ``incremental_window``.
    """
    completed = '"_execution_completed_ts"'
    if update_pattern == "incremental_window":
        return bronze
    if update_pattern == "append_only":
        return f"(SELECT * FROM {bronze} WHERE {completed} IS NOT NULL) scoped"
    if update_pattern in {"snapshot", "full_refresh"}:
        return (
            f"{bronze} b JOIN ("
            f'SELECT "dt", "execution_id" FROM {bronze} '
            f"WHERE {completed} IS NOT NULL "
            f"ORDER BY from_iso8601_timestamp({completed}) DESC, "
            '"dt" DESC, "execution_id" DESC LIMIT 1'
            ') latest ON b."dt" = latest."dt" '
            'AND b."execution_id" = latest."execution_id"'
        )
    msg = f"unsupported update_pattern: {update_pattern!r}"
    raise ReconciliationError(msg)


def shape_checks(
    datasets: set[tuple[str, str]] | None = None,
) -> tuple[tuple[str, str], ...]:
    """Column rules whose expected count is the Silver table, not a snapshot size.

    ``datasets`` keeps only the rules of those Silver tables; ``None`` keeps all.
    """
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
    rules = (
        (("dummyjson_products", "products"), ("ok", _one_line(expanded))),
        (("demo_storefront", "clickstream"), ("ok", _one_line(product_on_cart))),
    )
    return tuple(
        check for owner, check in rules if datasets is None or owner in datasets
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
