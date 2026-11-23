"""Incremental lakehouse refresh: Bronze, Silver, and the Gold gate (ADR-0024).

``medallion_build`` calls these functions. Each one returns ``True`` when it
wrote a table and ``False`` when there was nothing to do, so the caller can
report the step as skipped.

- Bronze stages only new raw objects (``bronze.promote_bronze``), runs one
  Spark script per changed dataset, then registers the objects. The
  registry write marks the dataset pending for Silver in the same
  transaction. Staging objects of the run are deleted after registration.
- Silver rebuilds the datasets pending for it, one Spark script each, then
  clears the flag and marks Gold pending.
- Gold (quarantine staging and ``dbt build``) runs only while Gold is pending.
- The quality gate (P3-16): a write marks its check pending in the registry.
  Silver does not build a dataset whose Bronze check is open, and Gold does
  not build while any Bronze or Silver check is open. An open check is
  pending or failed; the check step runs it again on the next refresh even
  when no new raw object arrives.

Execution metadata (P3-08): each dataset a step writes gets a
``run_log.DatasetRun`` with its objects, rows read and written, duration,
and the measured scan and commit counts of its Spark script, recorded even
when the step fails. Before an incremental Bronze ``MERGE`` the table must
have the ``dt`` partition spec (ADR-0026); a table created before P3-08
fails the step until ``make live-rebuild`` recreates it.

``full_rebuild`` rebuilds every dataset from all complete raw objects and
every Silver table, and runs Gold. It is for a contract version change or
recovery and is never the scheduled path.

Output names datasets, files, and counts. It never prints row values.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from open_data_platform.lakehouse.bronze import (
    PARTITION_COLUMN,
    BronzeError,
    DatasetPromotion,
    bronze_table_name,
    promote_bronze,
)
from open_data_platform.lakehouse.promotion_registry import (
    BRONZE,
    CHECK_LAYERS,
    GOLD,
    GOLD_DATASET,
    SILVER,
    PromotionRegistry,
    RefreshRecord,
)
from open_data_platform.lakehouse.run_log import (
    DATASET_FAILED,
    DATASET_RAN,
    DatasetRun,
    RunLog,
)
from open_data_platform.lakehouse.spark_metrics import SparkMetrics, TableIO
from open_data_platform.lakehouse.write_mode import WriteMode

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from open_data_platform.contracts.model import DataContract

# Run one Spark SQL file and return what it measured; raise when Spark fails.
# A runner that measures nothing may return ``None``.
SparkRunner = Callable[[Path], SparkMetrics | None]
# Identity partition columns of ``bronze.<table>``, or ``None`` when absent.
PartitionReader = Callable[[str], tuple[str, ...] | None]

COUNTS_FILE = "bronze-source-counts.tsv"
SILVER_DATASETS_FILE = "silver-datasets.txt"


class RefreshError(RuntimeError):
    """A refresh step failed. The message names the step and dataset."""


@dataclass(frozen=True, slots=True)
class BronzeRefresh:
    """What one Bronze step did, per dataset."""

    datasets: tuple[DatasetPromotion, ...]
    table_rows: dict[str, int]
    seconds: float

    @property
    def wrote(self) -> bool:
        return bool(self.table_rows)


def refresh_bronze(
    client: BaseClient,
    *,
    contracts: Sequence[DataContract],
    registry: PromotionRegistry,
    run_label: str,
    work: Path,
    raw_bucket: str,
    warehouse_bucket: str,
    run_spark: SparkRunner,
    full_rebuild: bool = False,
    run_log: RunLog | None = None,
    partition_columns: PartitionReader | None = None,
) -> BronzeRefresh:
    """Promote new raw objects, write the changed tables, and register them.

    The registry write follows the Spark commit of each dataset. A Spark
    failure stops the step: earlier datasets stay registered, the failing
    dataset keeps its previous snapshot and stays unregistered, and a retry
    stages it again. The per-dataset counts are recorded in
    ``lakehouse_state.bronze_refreshes`` even when the step fails.
    """
    started = time.monotonic()
    try:
        promotion = promote_bronze(
            client,
            raw_bucket=raw_bucket,
            warehouse_bucket=warehouse_bucket,
            contracts=contracts,
            run_label=run_label,
            registry=registry,
            full_rebuild=full_rebuild,
            work_dir=work,
        )
    except BronzeError as exc:
        raise RefreshError(f"bronze promotion failed: {exc}") from exc
    sql_dir = work / "bronze-sql"
    _write_scripts(sql_dir, promotion.scripts)
    table_rows: dict[str, int] = {}
    records: list[RefreshRecord] = []
    runs: list[DatasetRun] = []
    try:
        for entry in promotion.datasets:
            total: int | None = None
            metrics = SparkMetrics()
            spark_started = time.monotonic()
            if entry.script is not None:
                try:
                    if entry.mode is not WriteMode.FULL_OVERWRITE:
                        _require_bronze_partition(entry, partition_columns)
                    print(f"spark bronze: {entry.script}", flush=True)
                    metrics = run_spark(sql_dir / entry.script) or SparkMetrics()
                except Exception as exc:
                    records.append(_record(entry, None, failed=True))
                    runs.append(
                        _bronze_run(
                            entry,
                            _failure_metrics(exc),
                            time.monotonic() - spark_started,
                            None,
                            failed=True,
                        )
                    )
                    raise
            if entry.objects:
                total = registry.record_promotion(
                    entry.source_id,
                    entry.dataset,
                    entry.objects,
                    run_label=run_label,
                    replace_dataset=entry.mode is WriteMode.FULL_OVERWRITE,
                )
                if entry.script is not None:
                    table_rows[entry.name] = total
            if entry.staging_key is not None:
                client.delete_object(Bucket=warehouse_bucket, Key=entry.staging_key)
            records.append(_record(entry, total))
            if entry.script is not None or entry.objects:
                runs.append(
                    _bronze_run(entry, metrics, time.monotonic() - spark_started, total)
                )
    finally:
        registry.record_refresh(run_label, records)
        if run_log is not None:
            run_log.record_datasets(run_label, runs)
        _write_counts(work / COUNTS_FILE, table_rows)
    seconds = time.monotonic() - started
    for entry in promotion.datasets:
        print(
            f"bronze {entry.name}: "
            f"mode={entry.mode.value if entry.script else 'none'} "
            f"listed={entry.objects_listed} incomplete={entry.objects_incomplete} "
            f"promoted={len(entry.objects)} replaced={len(entry.replaced_keys)} "
            f"rows_appended={entry.rows_staged} "
            f"table_rows={table_rows.get(entry.name, '-')}",
            flush=True,
        )
    print(
        f"bronze tables written: {len(table_rows)} "
        f"objects promoted: {sum(len(e.objects) for e in promotion.datasets)} "
        f"rows appended: {sum(e.rows_staged for e in promotion.datasets)} "
        f"seconds: {seconds:.1f}",
        flush=True,
    )
    return BronzeRefresh(
        datasets=promotion.datasets, table_rows=table_rows, seconds=seconds
    )


def refresh_silver(
    *,
    registry: PromotionRegistry,
    run_label: str,
    work: Path,
    warehouse_bucket: str,
    run_spark: SparkRunner,
    full_rebuild: bool = False,
    allow_volume_drop: frozenset[tuple[str, str]] = frozenset(),
    run_log: RunLog | None = None,
) -> tuple[tuple[str, str], ...]:
    """Rebuild the Silver tables pending for Silver. Return the datasets.

    Each dataset is one ``CREATE OR REPLACE`` (P3-11). Its pending flag is
    cleared only after its Spark script succeeds, in the same transaction
    that marks Gold pending, so a failure leaves it pending for the next run.
    A volume guard failure (P3-07) is such a failure. ``allow_volume_drop``
    names ``snapshot`` or ``full_refresh`` datasets whose guard this run
    skips, to accept a real shrink of the source.
    """
    from open_data_platform.lakehouse.silver import cli as silver
    from open_data_platform.lakehouse.silver.common import (
        SilverError,
        render_source_scripts,
    )

    pending = registry.pending(SILVER)
    available = silver.selected_entries(source_id=None, datasets=(), all_sources=True)
    guarded = {
        (contract.source_id, contract.dataset)
        for contract, _policy in available
        if contract.min_row_ratio is not None
    }
    unknown = sorted(f"{s}.{d}" for s, d in allow_volume_drop - guarded)
    if unknown:
        msg = f"--allow-volume-drop names no guarded dataset: {', '.join(unknown)}"
        raise RefreshError(msg)
    if full_rebuild:
        entries = available
    else:
        entries = tuple(
            entry
            for entry in available
            if (entry[0].source_id, entry[0].dataset) in pending
        )
    unchecked = registry.open_checks(BRONZE)
    held = sorted(
        f"{contract.source_id}.{contract.dataset} ({unchecked[identity].status})"
        for contract, _policy in entries
        if (identity := (contract.source_id, contract.dataset)) in unchecked
    )
    if held:
        msg = f"silver blocked: bronze check not passed for {', '.join(held)}"
        raise RefreshError(msg)
    sql_dir = work / "silver-sql"
    done: list[tuple[str, str]] = []
    try:
        scripts = render_source_scripts(
            entries,
            warehouse_bucket,
            run_label=run_label,
            allow_volume_drop=allow_volume_drop,
        )
    except SilverError as exc:
        raise RefreshError(f"silver render failed: {exc}") from exc
    _write_scripts(sql_dir, scripts)
    for (contract, _policy), (name, _sql) in zip(entries, scripts, strict=True):
        if (contract.source_id, contract.dataset) in allow_volume_drop:
            print(f"silver volume guard skipped: {name}", flush=True)
        print(f"spark silver: {name}", flush=True)
        identity = (contract.source_id, contract.dataset)
        started = time.monotonic()
        try:
            metrics = run_spark(sql_dir / name) or SparkMetrics()
        except Exception as exc:
            if run_log is not None:
                run_log.record_datasets(
                    run_label,
                    [
                        _silver_run(
                            identity,
                            _failure_metrics(exc),
                            time.monotonic() - started,
                            failed=True,
                        )
                    ],
                )
            raise
        registry.complete(
            SILVER,
            identity,
            pending.get(identity),
            run_label=run_label,
            mark_next=GOLD,
        )
        if run_log is not None:
            run_log.record_datasets(
                run_label,
                [_silver_run(identity, metrics, time.monotonic() - started)],
            )
        done.append(identity)
    (work / SILVER_DATASETS_FILE).write_text(
        "".join(f"{source}/{dataset}\n" for source, dataset in done),
        encoding="utf-8",
    )
    print(f"silver tables written: {len(done)}", flush=True)
    return tuple(done)


def gold_mark(registry: PromotionRegistry) -> datetime | None:
    """The Gold pending mark, or ``None`` when Gold is up to date."""
    return registry.pending(GOLD).get(GOLD_DATASET)


def open_checks_summary(registry: PromotionRegistry) -> tuple[str, ...]:
    """``layer source.dataset (status)`` for every open check, sorted."""
    return tuple(
        sorted(
            f"{layer} {source}.{dataset} ({check.status})"
            for layer in CHECK_LAYERS
            for (source, dataset), check in registry.open_checks(layer).items()
        )
    )


def require_gold_inputs_checked(registry: PromotionRegistry) -> None:
    """Raise when Gold would build on a Bronze or Silver check that is open."""
    held = open_checks_summary(registry)
    if held:
        msg = f"gold blocked: check not passed for {', '.join(held)}"
        raise RefreshError(msg)


def complete_gold(
    registry: PromotionRegistry, run_label: str, marked: datetime | None
) -> None:
    """Clear the Gold flag read before ``dbt build`` started, if unchanged."""
    registry.complete(GOLD, GOLD_DATASET, marked, run_label=run_label, mark_next=None)


def read_dbt_runs(path: Path) -> list[DatasetRun]:
    """Gold ``DatasetRun`` rows from a dbt ``run_results.json``, one per model.

    ``source_id`` is the model schema and ``dataset`` the model name. dbt
    writes the file even when the build fails, so failed models are kept.
    ``rows_written`` is the adapter's ``rows_affected`` when it reports one,
    else ``None``. Tests and skipped nodes are not datasets. A missing or
    unreadable file gives no rows.
    """
    import json

    try:
        results = json.loads(path.read_text(encoding="utf-8")).get("results") or ()
    except (OSError, ValueError, AttributeError):
        return []
    runs: list[DatasetRun] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        unique_id = str(item.get("unique_id", ""))
        status = str(item.get("status", ""))
        if not unique_id.startswith("model.") or status not in {"success", "error"}:
            continue
        relation = str(item.get("relation_name") or "")
        parts = [part.strip('"`') for part in relation.split(".")]
        if len(parts) < 2 or not parts[-2] or not parts[-1]:
            continue
        rows = (item.get("adapter_response") or {}).get("rows_affected")
        runs.append(
            DatasetRun(
                layer=GOLD,
                source_id=parts[-2],
                dataset=parts[-1],
                status=DATASET_RAN if status == "success" else DATASET_FAILED,
                mode="dbt",
                seconds=float(item.get("execution_time") or 0.0),
                rows_written=rows if isinstance(rows, int) and rows >= 0 else None,
            )
        )
    return runs


def read_silver_datasets(work: Path) -> tuple[tuple[str, str], ...] | None:
    """Datasets the Silver step of this run rebuilt, or ``None`` without a file."""
    path = work / SILVER_DATASETS_FILE
    if not path.is_file():
        return None
    found: list[tuple[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        source, _, dataset = line.partition("/")
        if source and dataset:
            found.append((source, dataset))
    return tuple(found)


def _require_bronze_partition(
    entry: DatasetPromotion, partition_columns: PartitionReader | None
) -> None:
    """Fail before an incremental ``MERGE`` into a table without ``dt``."""
    if partition_columns is None:
        return
    table = bronze_table_name(entry.source_id, entry.dataset)
    columns = partition_columns(table)
    if columns is not None and PARTITION_COLUMN not in columns:
        msg = (
            f"bronze.{table} has no {PARTITION_COLUMN} partition spec; "
            "run make live-rebuild once to recreate it (ADR-0026)"
        )
        raise RefreshError(msg)


def _failure_metrics(exc: BaseException) -> SparkMetrics:
    metrics = getattr(exc, "metrics", None)
    return metrics if isinstance(metrics, SparkMetrics) else SparkMetrics()


def _tables(metrics: SparkMetrics) -> tuple[TableIO, ...]:
    return tuple(metrics.tables[name] for name in sorted(metrics.tables))


def _bronze_run(
    entry: DatasetPromotion,
    metrics: SparkMetrics,
    seconds: float,
    total: int | None,
    *,
    failed: bool = False,
) -> DatasetRun:
    """Objects and rows staged, rows the commit added, the registry total."""
    table = metrics.get(f"bronze.{bronze_table_name(entry.source_id, entry.dataset)}")
    return DatasetRun(
        layer=BRONZE,
        source_id=entry.source_id,
        dataset=entry.dataset,
        status=DATASET_FAILED if failed else DATASET_RAN,
        mode=entry.mode.value if entry.script is not None else "none",
        seconds=seconds,
        objects_read=len(entry.objects),
        rows_read=entry.rows_staged,
        rows_written=None if table is None else table.added_rows,
        table_rows=total,
        tables=_tables(metrics),
    )


def _silver_run(
    identity: tuple[str, str],
    metrics: SparkMetrics,
    seconds: float,
    *,
    failed: bool = False,
) -> DatasetRun:
    """Rows scanned from the Bronze table, rows the Silver commit wrote."""
    from open_data_platform.lakehouse.silver.common import silver_table_name

    source_id, dataset = identity
    bronze = metrics.get(f"bronze.{bronze_table_name(source_id, dataset)}")
    silver = metrics.get(f"silver.{silver_table_name(source_id, dataset)}")
    return DatasetRun(
        layer=SILVER,
        source_id=source_id,
        dataset=dataset,
        status=DATASET_FAILED if failed else DATASET_RAN,
        mode=WriteMode.FULL_OVERWRITE.value,
        seconds=seconds,
        rows_read=None if bronze is None else bronze.rows_scanned,
        rows_written=None if silver is None else silver.added_rows,
        table_rows=None if silver is None else silver.total_rows,
        tables=_tables(metrics),
    )


def _record(
    entry: DatasetPromotion, total: int | None, *, failed: bool = False
) -> RefreshRecord:
    mode = entry.mode.value if entry.script is not None else "none"
    return RefreshRecord(
        source_id=entry.source_id,
        dataset=entry.dataset,
        mode=f"{mode}:failed" if failed else mode,
        objects_listed=entry.objects_listed,
        objects_incomplete=entry.objects_incomplete,
        objects_promoted=len(entry.objects),
        rows_appended=entry.rows_staged,
        table_rows=total,
    )


def _write_scripts(destination: Path, scripts: Sequence[tuple[str, str]]) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    expected = {name for name, _sql in scripts}
    for child in destination.glob("*.sql"):
        if child.name not in expected:
            child.unlink()
    for name, sql in scripts:
        (destination / name).write_text(sql, encoding="utf-8")


def _write_counts(path: Path, table_rows: dict[str, int]) -> None:
    from open_data_platform.lakehouse.reconciliation import format_counts

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_counts(sorted(table_rows.items())), encoding="utf-8")
