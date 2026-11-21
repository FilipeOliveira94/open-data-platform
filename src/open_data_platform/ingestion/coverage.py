"""Window coverage ledger and gap report for incremental extracts (P3-06).

ADR-0025. Each successful incremental extract records, per dataset, the
window it covered in ``ingestion_state.extract_coverage`` in the catalog
database (``ICEBERG_CATALOG_DB``, the same PostgreSQL server as the
promotion registry of ADR-0024). One row per ``(source_id, dataset,
execution_id)``: a cleared and re-run Airflow task keeps its execution id
and overwrites its own row; a new run of the same window adds a row.

``frequency`` names the cadence that produced the window: ``daily`` (one
day), ``weekly`` (seven days), or ``manual`` (any window, outside the
schedule). A ``CHECK`` keeps it consistent with the window length.

The gap report rebuilds the expected windows of one cadence from its first
window up to the last recorded one and compares them with the ledger:

- a *gap* is an expected window that lacks at least one dataset;
- an *overlap* is two different recorded windows of the same cadence that
  intersect. The same window recorded by several executions is a re-run,
  counted but not an overlap. Daily and weekly windows overlap by design and
  are never compared with each other;
- windows that start before the first expected window (a backfill before the
  DAG ``start_date``) are listed apart and are not errors.

Statements are parameterized. Window bounds, ids, and counts are values,
never SQL text. No row values are stored.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol

from open_data_platform.ingestion.pipeline_log import (
    configure_cli_logging,
    log_failure,
    log_stage,
)
from open_data_platform.lakehouse.promotion_registry import RegistryError, catalog_db

if TYPE_CHECKING:
    import psycopg

SCHEMA = "ingestion_state"
TABLE = f"{SCHEMA}.extract_coverage"
DAILY = "daily"
WEEKLY = "weekly"
MANUAL = "manual"
FREQUENCIES = (DAILY, WEEKLY, MANUAL)

# Serializes the DDL when the daily and weekly extracts start together.
_DDL_LOCK_KEY = 0x0D9_C0BE
DDL = (
    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
    f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        source_id text NOT NULL,
        dataset text NOT NULL,
        execution_id text NOT NULL,
        frequency text NOT NULL CHECK (frequency IN ('daily', 'weekly', 'manual')),
        window_start timestamptz NOT NULL,
        window_end timestamptz NOT NULL,
        dag_id text,
        airflow_run_id text,
        row_count bigint NOT NULL CHECK (row_count >= 0),
        recorded_at timestamptz NOT NULL,
        PRIMARY KEY (source_id, dataset, execution_id),
        CHECK (window_end > window_start),
        CHECK (
            frequency = 'manual'
            OR (frequency = 'daily' AND window_end - window_start = interval '1 day')
            OR (frequency = 'weekly' AND window_end - window_start = interval '7 days')
        )
    )""",
    f"""CREATE INDEX IF NOT EXISTS extract_coverage_frequency
        ON {TABLE} (source_id, frequency, window_start)""",
)

UPSERT_SQL = (
    f"INSERT INTO {TABLE} (source_id, dataset, execution_id, frequency, "
    "window_start, window_end, dag_id, airflow_run_id, row_count, recorded_at) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
    "ON CONFLICT (source_id, dataset, execution_id) DO UPDATE SET "
    "frequency = EXCLUDED.frequency, window_start = EXCLUDED.window_start, "
    "window_end = EXCLUDED.window_end, dag_id = EXCLUDED.dag_id, "
    "airflow_run_id = EXCLUDED.airflow_run_id, row_count = EXCLUDED.row_count, "
    "recorded_at = EXCLUDED.recorded_at"
)

SELECT_SQL = (
    f"SELECT dataset, execution_id, window_start, window_end, dag_id, "
    f"airflow_run_id, row_count FROM {TABLE} "
    "WHERE source_id = %s AND frequency = %s ORDER BY window_start, dataset"
)


class CoverageError(ValueError):
    """The ledger cannot be written or read. Names the step, not values."""


@dataclass(frozen=True, slots=True)
class Cadence:
    """The expected windows of one scheduled frequency.

    ``first_start`` is the start of the first window the DAG covers; each
    window is ``length`` long and the next starts where it ends.
    """

    first_start: datetime
    length: timedelta


# Must match the DAG files (a unit test reads their START_DATE):
# - ``ingest_postgres_oltp_daily``: START_DATE 2026-11-19, window = interval.
# - ``ingest_postgres_oltp_weekly``: START_DATE 2026-11-16 (a Monday), window
#   = interval shifted back one day, so the first is [2026-11-15, 2026-11-22).
CADENCES: dict[str, Cadence] = {
    DAILY: Cadence(datetime(2026, 11, 19, tzinfo=UTC), timedelta(days=1)),
    WEEKLY: Cadence(datetime(2026, 11, 15, tzinfo=UTC), timedelta(days=7)),
}


@dataclass(frozen=True, slots=True)
class CoverageRecord:
    """One dataset of one successful extract execution."""

    source_id: str
    dataset: str
    execution_id: str
    frequency: str
    window_start: datetime
    window_end: datetime
    row_count: int
    dag_id: str | None = None
    airflow_run_id: str | None = None


class CoverageSink(Protocol):
    """Where an extract records the windows it covered."""

    def record(self, records: Sequence[CoverageRecord]) -> None:
        """Store all records in one transaction."""
        ...


def validate_frequency(
    frequency: str, window_start: datetime, window_end: datetime
) -> str:
    """Return ``frequency`` when it is known and matches the window length."""
    if frequency not in FREQUENCIES:
        msg = f"unknown frequency: {frequency!r}"
        raise CoverageError(msg)
    cadence = CADENCES.get(frequency)
    if cadence is not None and window_end - window_start != cadence.length:
        msg = f"a {frequency} window must be {cadence.length.days} day(s) long"
        raise CoverageError(msg)
    return frequency


def upsert_parameters(
    records: Sequence[CoverageRecord], recorded_at: datetime
) -> list[tuple[object, ...]]:
    """Return the ``UPSERT_SQL`` parameter rows for ``records``."""
    rows: list[tuple[object, ...]] = []
    for item in records:
        validate_frequency(item.frequency, item.window_start, item.window_end)
        if item.row_count < 0:
            msg = f"negative row count for dataset {item.dataset}"
            raise CoverageError(msg)
        rows.append(
            (
                item.source_id,
                item.dataset,
                item.execution_id,
                item.frequency,
                item.window_start,
                item.window_end,
                item.dag_id or None,
                item.airflow_run_id or None,
                item.row_count,
                recorded_at,
            )
        )
    return rows


class PostgresCoverageLedger:
    """Ledger in the catalog database. One short transaction per call."""

    def __init__(self, env: Mapping[str, str]) -> None:
        self._env = dict(env)
        try:
            self._dbname = catalog_db(env)
        except RegistryError as exc:
            raise CoverageError(str(exc)) from exc

    @contextmanager
    def _connect(self) -> Iterator[psycopg.Connection]:
        import psycopg

        env = self._env
        try:
            conn = psycopg.connect(
                host=env.get("POSTGRES_HOST", "postgres"),
                port=int(env.get("POSTGRES_PORT", "5432")),
                user=env["POSTGRES_USER"],
                password=env["POSTGRES_PASSWORD"],
                dbname=self._dbname,
            )
        except psycopg.Error as exc:
            msg = "cannot connect to the coverage ledger"
            raise CoverageError(msg) from exc
        try:
            with conn, conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (_DDL_LOCK_KEY,))
                for statement in DDL:
                    conn.execute(statement)
                yield conn
        except psycopg.Error as exc:
            msg = f"coverage ledger statement failed: {type(exc).__name__}"
            raise CoverageError(msg) from exc

    def record(self, records: Sequence[CoverageRecord]) -> None:
        if not records:
            return
        rows = upsert_parameters(records, datetime.now(UTC))
        with self._connect() as conn, conn.cursor() as cursor:
            cursor.executemany(UPSERT_SQL, rows)

    def load(self, source_id: str, frequency: str) -> list[CoverageRecord]:
        """Return every record of one source and frequency."""
        with self._connect() as conn:
            rows = conn.execute(SELECT_SQL, (source_id, frequency)).fetchall()
        return [
            CoverageRecord(
                source_id=source_id,
                dataset=str(dataset),
                execution_id=str(execution_id),
                frequency=frequency,
                window_start=_utc(start),
                window_end=_utc(end),
                row_count=int(row_count),
                dag_id=None if dag_id is None else str(dag_id),
                airflow_run_id=None if run_id is None else str(run_id),
            )
            for dataset, execution_id, start, end, dag_id, run_id, row_count in rows
        ]


Window = tuple[datetime, datetime]


@dataclass(frozen=True, slots=True)
class Gap:
    """An expected window and the datasets it lacks."""

    window: Window
    missing_datasets: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CoverageReport:
    """Result of comparing one cadence with the ledger."""

    frequency: str
    first_window: Window
    last_window: Window | None
    expected_windows: int
    gaps: tuple[Gap, ...] = ()
    overlaps: tuple[tuple[Window, Window], ...] = ()
    before_start: tuple[Window, ...] = ()
    rerun_windows: tuple[Window, ...] = ()

    @property
    def clean(self) -> bool:
        """True when there is no gap and no overlap."""
        return not self.gaps and not self.overlaps


def coverage_report(
    frequency: str,
    records: Sequence[CoverageRecord],
    *,
    datasets: Sequence[str],
    cadence: Cadence | None = None,
) -> CoverageReport:
    """Compare the expected windows of ``frequency`` with ``records``.

    Only records of ``frequency`` count. The expected windows run from the
    cadence's first window to the last recorded window end.
    """
    if cadence is None:
        if frequency not in CADENCES:
            msg = f"no cadence for frequency {frequency!r}"
            raise CoverageError(msg)
        cadence = CADENCES[frequency]
    first = (cadence.first_start, cadence.first_start + cadence.length)
    by_window: dict[Window, set[str]] = {}
    executions: dict[Window, set[str]] = {}
    for item in records:
        if item.frequency != frequency:
            continue
        window = (item.window_start, item.window_end)
        by_window.setdefault(window, set()).add(item.dataset)
        executions.setdefault(window, set()).add(item.execution_id)
    in_range = sorted(w for w in by_window if w[0] >= cadence.first_start)
    before = tuple(sorted(w for w in by_window if w[0] < cadence.first_start))
    reruns = tuple(sorted(w for w, ids in executions.items() if len(ids) > 1))
    if not in_range:
        return CoverageReport(
            frequency=frequency,
            first_window=first,
            last_window=None,
            expected_windows=0,
            before_start=before,
            rerun_windows=reruns,
        )
    last_end = max(end for _, end in in_range)
    expected: list[Window] = []
    start = cadence.first_start
    while start + cadence.length <= last_end:
        expected.append((start, start + cadence.length))
        start += cadence.length
    wanted = tuple(datasets)
    gaps = tuple(
        Gap(window, tuple(d for d in wanted if d not in by_window.get(window, set())))
        for window in expected
        if not set(wanted) <= by_window.get(window, set())
    )
    overlaps = tuple(
        (a, b)
        for index, a in enumerate(in_range)
        for b in in_range[index + 1 :]
        if b[0] < a[1]
    )
    return CoverageReport(
        frequency=frequency,
        first_window=first,
        last_window=max(in_range, key=lambda w: w[1]),
        expected_windows=len(expected),
        gaps=gaps,
        overlaps=overlaps,
        before_start=before,
        rerun_windows=reruns,
    )


def format_report(report: CoverageReport) -> list[str]:
    """Return the report as text lines (dates, counts, and dataset names)."""
    lines = [
        f"frequency={report.frequency} first_window={_fmt(report.first_window)} "
        f"last_window={_fmt(report.last_window) if report.last_window else '-'} "
        f"expected_windows={report.expected_windows} gaps={len(report.gaps)} "
        f"overlaps={len(report.overlaps)} before_start={len(report.before_start)} "
        f"rerun_windows={len(report.rerun_windows)}"
    ]
    lines += [
        f"gap {_fmt(gap.window)} missing={','.join(gap.missing_datasets)}"
        for gap in report.gaps
    ]
    lines += [f"overlap {_fmt(a)} {_fmt(b)}" for a, b in report.overlaps]
    lines += [f"before_start {_fmt(w)}" for w in report.before_start]
    lines += [f"rerun {_fmt(w)}" for w in report.rerun_windows]
    lines.append("clean" if report.clean else "not clean")
    return lines


def _fmt(window: Window) -> str:
    return f"[{window[0].date().isoformat()},{window[1].date().isoformat()})"


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        msg = "coverage window bound is not a timezone-aware timestamp"
        raise CoverageError(msg)
    return value.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    """CLI: ``report --frequency daily|weekly``. Exit 1 on a gap or overlap."""
    from open_data_platform.ingestion.postgres_oltp import DATASETS, SOURCE_ID

    parser = argparse.ArgumentParser(description="Window coverage gap report.")
    commands = parser.add_subparsers(dest="command", required=True)
    report = commands.add_parser("report", help="List gaps and overlaps.")
    report.add_argument("--frequency", required=True, choices=sorted(CADENCES))
    args = parser.parse_args(argv)
    configure_cli_logging()
    try:
        ledger = PostgresCoverageLedger(os.environ)
        records = ledger.load(SOURCE_ID, args.frequency)
        result = coverage_report(args.frequency, records, datasets=DATASETS)
    except (CoverageError, KeyError) as exc:
        log_failure(exc)
        return 2
    log_stage(
        "coverage_report",
        source_id=SOURCE_ID,
        frequency=args.frequency,
        gap_count=len(result.gaps),
        overlap_count=len(result.overlaps),
    )
    for line in format_report(result):
        sys.stdout.write(line + "\n")
    return 0 if result.clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
