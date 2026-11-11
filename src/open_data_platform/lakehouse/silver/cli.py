"""CLI for Silver SQL generation. Spark runs the scripts separately."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from open_data_platform.contracts.load import load_contracts
from open_data_platform.lakehouse.reconciliation import (
    ReconciliationError,
    silver_live_checks,
)
from open_data_platform.lakehouse.silver import (
    demo_storefront,
    dummyjson_products,
    ibge_localidades,
    olist_brazilian_ecommerce,
    olist_marketing_funnel,
    postgres_oltp,
)
from open_data_platform.lakehouse.silver.common import (
    DedupPolicy,
    SilverError,
    render_source_scripts,
    require_bucket_name,
)

if TYPE_CHECKING:
    from open_data_platform.contracts.model import DataContract

SOURCE_MODULES: Mapping[str, Mapping[str, DedupPolicy]] = {
    olist_brazilian_ecommerce.SOURCE_ID: olist_brazilian_ecommerce.POLICIES,
    olist_marketing_funnel.SOURCE_ID: olist_marketing_funnel.POLICIES,
    postgres_oltp.SOURCE_ID: postgres_oltp.POLICIES,
    dummyjson_products.SOURCE_ID: dummyjson_products.POLICIES,
    demo_storefront.SOURCE_ID: demo_storefront.POLICIES,
    ibge_localidades.SOURCE_ID: ibge_localidades.POLICIES,
}


def policy_for(contract: DataContract) -> DedupPolicy:
    """Return the DedupPolicy registered for one contract."""
    policies = SOURCE_MODULES.get(contract.source_id)
    if policies is None:
        msg = f"no Silver module for source_id={contract.source_id!r}"
        raise SilverError(msg)
    policy = policies.get(contract.dataset)
    if policy is None:
        msg = f"no Silver policy for {contract.source_id}.{contract.dataset}"
        raise SilverError(msg)
    return policy


def selected_entries(
    *,
    source_id: str | None,
    datasets: Sequence[str],
    all_sources: bool,
) -> tuple[tuple[DataContract, DedupPolicy], ...]:
    """Resolve contracts and policies for a CLI selection."""
    loaded = {(item.source_id, item.dataset): item for item in load_contracts()}
    if all_sources:
        if source_id or datasets:
            msg = "--all does not take --source or --dataset"
            raise SilverError(msg)
        identities = tuple(
            sorted(
                (source, dataset)
                for source, policies in SOURCE_MODULES.items()
                for dataset in policies
            )
        )
    elif source_id:
        if source_id not in SOURCE_MODULES:
            msg = f"unknown source_id: {source_id}"
            raise SilverError(msg)
        if datasets:
            identities = tuple((source_id, name) for name in datasets)
        else:
            identities = tuple(
                (source_id, name) for name in sorted(SOURCE_MODULES[source_id])
            )
    elif datasets:
        msg = "--dataset requires --source"
        raise SilverError(msg)
    else:
        msg = "pass --source or --all"
        raise SilverError(msg)

    entries: list[tuple[DataContract, DedupPolicy]] = []
    for identity in identities:
        contract = loaded.get(identity)
        if contract is None:
            msg = f"unknown dataset: {identity[0]}/{identity[1]}"
            raise SilverError(msg)
        entries.append((contract, policy_for(contract)))
    return tuple(entries)


def write_scripts(
    destination: Path,
    scripts: tuple[tuple[str, str], ...],
) -> None:
    """Write one SQL file per dataset; remove stale scripts in the directory."""
    if destination.exists() and not destination.is_dir():
        msg = "--sql-out must be a directory"
        raise SilverError(msg)
    destination.mkdir(parents=True, exist_ok=True)
    expected = {name for name, _sql in scripts}
    for child in destination.glob("*.sql"):
        if child.name not in expected:
            child.unlink()
    for name, sql in scripts:
        (destination / name).write_text(sql, encoding="utf-8")


def live_checks() -> tuple[tuple[str, str], ...]:
    """Trino statements for the full-data Silver live run.

    Each expected value is ``ok``. The statement compares Silver to Bronze
    keys, or a shape rule to the Silver table.
    """
    entries = selected_entries(source_id=None, datasets=(), all_sources=True)
    return silver_live_checks(entries)


def main(argv: list[str] | None = None) -> int:
    """Write Silver Spark SQL for one source or every source."""
    parser = argparse.ArgumentParser(
        description=(
            "Build Silver Spark SQL from Bronze tables. "
            "Spark is a separate step. Airflow should call this entrypoint. "
            "Use --all for the full live run against real Bronze tables."
        ),
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="source_id whose Silver module should run.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Dataset name within --source. Repeatable. Default: all for the source.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Build Silver SQL for every registered source and dataset.",
    )
    parser.add_argument(
        "--sql-out",
        type=str,
        default=None,
        help="Directory for one Spark SQL file per dataset.",
    )
    parser.add_argument(
        "--print-live-checks",
        action="store_true",
        help=(
            "Print ok and Trino SQL pairs that compare Silver to Bronze "
            "for the full live run."
        ),
    )
    args = parser.parse_args(argv)
    try:
        if args.print_live_checks:
            if args.source or args.dataset or args.sql_out or args.all:
                msg = "--print-live-checks does not take other arguments"
                raise SilverError(msg)
            for expected, statement in live_checks():
                print(f"{expected}\t{statement}")
            return 0
        if args.sql_out is None:
            msg = "--sql-out is required"
            raise SilverError(msg)
        warehouse = require_bucket_name(os.environ["ODP_WAREHOUSE_BUCKET"])
        entries = selected_entries(
            source_id=args.source,
            datasets=args.dataset,
            all_sources=args.all,
        )
        scripts = render_source_scripts(entries, warehouse)
        write_scripts(Path(args.sql_out), scripts)
        for contract, _policy in entries:
            print(f"{contract.source_id}.{contract.dataset}")
    except KeyError as exc:
        print(f"missing environment variable: {exc}", file=sys.stderr)
        return 1
    except (SilverError, ReconciliationError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
