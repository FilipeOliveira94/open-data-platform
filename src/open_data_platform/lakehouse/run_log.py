"""Execution metadata of the lakehouse refresh (P3-08, ADR-0026).

Three tables in ``lakehouse_state``, next to the promotion registry, kept
across runs so growth over time can be queried (Phase 5 charts them):

- ``step_runs``: one row per attempt of a ``medallion_build`` step, with its
  start, end, duration, status (``ran``, ``skipped``, ``failed``), and on
  failure the exception type and a one-line message. A retry of the same
  Airflow run adds a row; ``run_label`` is the Airflow run id.
- ``dataset_runs``: one row per run, layer, and dataset: raw objects read,
  rows read and written, the table total, and the duration of its write.
  Gold rows are dbt models (``source_id`` is the model schema).
- ``table_io``: one row per run, layer, dataset, and Iceberg table that the
  dataset's Spark script scanned or committed, with measured scan files,
  skipped files, bytes, and rows, and commit files and rows.

A retry of the same run replaces its ``dataset_runs`` and ``table_io`` rows.
``bronze_refreshes`` (ADR-0024) stays the record of the promotion decision.

No row value is stored: names, counts, durations, and a message that the
caller already limits to one line without values.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

from open_data_platform.lakehouse.promotion_registry import (
    SCHEMA,
    state_transaction,
)

if TYPE_CHECKING:
    from open_data_platform.lakehouse.spark_metrics import TableIO

STEP_RAN = "ran"
STEP_SKIPPED = "skipped"
STEP_FAILED = "failed"
DATASET_RAN = "ran"
DATASET_FAILED = "failed"
MESSAGE_LIMIT = 500

DDL = (
    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.step_runs (
        run_label text NOT NULL,
        step text NOT NULL,
        started_at timestamptz NOT NULL,
        finished_at timestamptz NOT NULL,
        seconds numeric(12, 3) NOT NULL CHECK (seconds >= 0),
        status text NOT NULL CHECK (status IN ('ran', 'skipped', 'failed')),
        full_rebuild boolean NOT NULL,
        error_type text,
        error_message text,
        PRIMARY KEY (run_label, step, started_at)
    )""",
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.dataset_runs (
        run_label text NOT NULL,
        layer text NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
        source_id text NOT NULL,
        dataset text NOT NULL,
        status text NOT NULL CHECK (status IN ('ran', 'failed')),
        mode text NOT NULL,
        objects_read integer CHECK (objects_read >= 0),
        rows_read bigint CHECK (rows_read >= 0),
        rows_written bigint CHECK (rows_written >= 0),
        table_rows bigint CHECK (table_rows >= 0),
        seconds numeric(12, 3) NOT NULL CHECK (seconds >= 0),
        recorded_at timestamptz NOT NULL,
        PRIMARY KEY (run_label, layer, source_id, dataset)
    )""",
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.table_io (
        run_label text NOT NULL,
        layer text NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
        source_id text NOT NULL,
        dataset text NOT NULL,
        table_name text NOT NULL,
        scans integer NOT NULL,
        scanned_files bigint,
        skipped_files bigint,
        scanned_bytes bigint,
        rows_scanned bigint,
        commits integer NOT NULL,
        added_files bigint,
        removed_files bigint,
        added_rows bigint,
        removed_rows bigint,
        total_rows bigint,
        recorded_at timestamptz NOT NULL,
        PRIMARY KEY (run_label, layer, source_id, dataset, table_name)
    )""",
)


@dataclass(frozen=True, slots=True)
class StepRun:
    """One attempt of one ``medallion_build`` step."""

    run_label: str
    step: str
    started_at: datetime
    finished_at: datetime
    status: str
    full_rebuild: bool = False
    error_type: str | None = None
    error_message: str | None = None

    @property
    def seconds(self) -> float:
        return max((self.finished_at - self.started_at).total_seconds(), 0.0)


@dataclass(frozen=True, slots=True)
class DatasetRun:
    """What one run did to one dataset in one layer."""

    layer: str
    source_id: str
    dataset: str
    status: str
    mode: str
    seconds: float
    objects_read: int | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    table_rows: int | None = None
    tables: tuple[TableIO, ...] = field(default=())


class RunLog(Protocol):
    """Where ``medallion_build`` and the refresh record execution metadata."""

    def record_step(self, run: StepRun) -> None: ...

    def record_datasets(self, run_label: str, runs: Sequence[DatasetRun]) -> None: ...


def one_line(message: str) -> str | None:
    """The message when it is one line, cut to ``MESSAGE_LIMIT``, else ``None``.

    Callers pass exception messages that name a step, dataset, or count; a
    multi-line message is dropped like ``pipeline_log.log_failure`` does.
    """
    if not message or "\n" in message or "\r" in message:
        return None
    return message[:MESSAGE_LIMIT]


class PostgresRunLog:
    """Run log in the catalog database. One short transaction per call."""

    def __init__(self, env: Mapping[str, str]) -> None:
        self._env = dict(env)
        with state_transaction(self._env, "run log") as conn:
            for statement in DDL:
                conn.execute(statement)

    def record_step(self, run: StepRun) -> None:
        with state_transaction(self._env, "run log") as conn:
            conn.execute(
                f"INSERT INTO {SCHEMA}.step_runs (run_label, step, started_at, "
                "finished_at, seconds, status, full_rebuild, error_type, "
                "error_message) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (run_label, step, started_at) DO NOTHING",
                (
                    run.run_label,
                    run.step,
                    run.started_at,
                    run.finished_at,
                    round(run.seconds, 3),
                    run.status,
                    run.full_rebuild,
                    run.error_type,
                    run.error_message,
                ),
            )

    def record_datasets(self, run_label: str, runs: Sequence[DatasetRun]) -> None:
        if not runs:
            return
        now = _now()
        with state_transaction(self._env, "run log") as conn, conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO {SCHEMA}.dataset_runs (run_label, layer, source_id, "
                "dataset, status, mode, objects_read, rows_read, rows_written, "
                "table_rows, seconds, recorded_at) VALUES "
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (run_label, layer, source_id, dataset) DO UPDATE SET "
                "status = EXCLUDED.status, mode = EXCLUDED.mode, "
                "objects_read = EXCLUDED.objects_read, "
                "rows_read = EXCLUDED.rows_read, "
                "rows_written = EXCLUDED.rows_written, "
                "table_rows = EXCLUDED.table_rows, seconds = EXCLUDED.seconds, "
                "recorded_at = EXCLUDED.recorded_at",
                [
                    (
                        run_label,
                        item.layer,
                        item.source_id,
                        item.dataset,
                        item.status,
                        item.mode,
                        item.objects_read,
                        item.rows_read,
                        item.rows_written,
                        item.table_rows,
                        round(item.seconds, 3),
                        now,
                    )
                    for item in runs
                ],
            )
            # A retry replaces the tables of each dataset it records.
            cur.executemany(
                f"DELETE FROM {SCHEMA}.table_io WHERE run_label = %s "
                "AND layer = %s AND source_id = %s AND dataset = %s",
                [(run_label, r.layer, r.source_id, r.dataset) for r in runs],
            )
            rows = [
                (
                    run_label,
                    item.layer,
                    item.source_id,
                    item.dataset,
                    io.table,
                    io.scans,
                    io.scanned_files,
                    io.skipped_files,
                    io.scanned_bytes,
                    io.rows_scanned,
                    io.commits,
                    io.added_files,
                    io.removed_files,
                    io.added_rows,
                    io.removed_rows,
                    io.total_rows,
                    now,
                )
                for item in runs
                for io in item.tables
            ]
            if rows:
                cur.executemany(
                    f"INSERT INTO {SCHEMA}.table_io (run_label, layer, source_id, "
                    "dataset, table_name, scans, scanned_files, skipped_files, "
                    "scanned_bytes, rows_scanned, commits, added_files, "
                    "removed_files, added_rows, removed_rows, total_rows, "
                    "recorded_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s, %s, %s, %s, %s)",
                    rows,
                )


class BestEffortRunLog:
    """Open the Postgres run log on first use; report its failures, never raise.

    Execution metadata must not fail or mask a pipeline step. A step that
    runs before the catalog database exists (``ensure-catalog``) or a run
    with PostgreSQL down prints one line to stderr per lost record and goes
    on.
    """

    def __init__(self, env: Mapping[str, str]) -> None:
        self._env = dict(env)
        self._log: PostgresRunLog | None = None

    def _call(self, what: str, action: Callable[[PostgresRunLog], None]) -> None:
        from open_data_platform.lakehouse.promotion_registry import RegistryError

        try:
            if self._log is None:
                self._log = PostgresRunLog(self._env)
            action(self._log)
        except RegistryError as exc:
            print(f"run log: {what} not recorded: {exc}", file=sys.stderr, flush=True)

    def record_step(self, run: StepRun) -> None:
        self._call("step run", lambda log: log.record_step(run))

    def record_datasets(self, run_label: str, runs: Sequence[DatasetRun]) -> None:
        self._call("dataset runs", lambda log: log.record_datasets(run_label, runs))


class InMemoryRunLog:
    """Run log for unit tests."""

    def __init__(self) -> None:
        self.steps: list[StepRun] = []
        self.datasets: dict[tuple[str, str, str, str], DatasetRun] = {}

    def record_step(self, run: StepRun) -> None:
        self.steps.append(run)

    def record_datasets(self, run_label: str, runs: Sequence[DatasetRun]) -> None:
        for item in runs:
            key = (run_label, item.layer, item.source_id, item.dataset)
            self.datasets[key] = item


def _now() -> datetime:
    return datetime.now(UTC)
