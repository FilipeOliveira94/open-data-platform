"""Promotion registry and pending-layer flags for the incremental refresh.

ADR-0024. The registry lives in the PostgreSQL database of the Iceberg JDBC
catalog, schema ``lakehouse_state``:

- ``bronze_promotions``: one row per raw object promoted into Bronze, keyed
  by ``raw_object_key``, with the ETag that was read and the rows it gave.
  An object is new when its key is absent or its ETag differs.
- ``pending_layers``: a dataset that Bronze changed stays pending for
  ``silver`` until a Silver write of it succeeds; a Silver write marks
  ``gold`` pending until ``dbt build`` succeeds. A failed or skipped
  downstream step therefore runs on the next refresh even when no new raw
  object arrives.
- ``bronze_refreshes``: one row per dataset per Bronze run with the objects
  listed, promoted, and waiting for a marker, and the rows appended. A
  refresh that writes no table still records its rows here.
- ``layer_checks`` (P3-16): the quality gate. A Bronze write marks the
  ``bronze`` check of the dataset ``pending`` in the transaction that
  registers its objects, with the expected row total; a Silver write marks
  the ``silver`` check ``pending`` in the transaction that clears its Silver
  flag. A check step records ``passed`` or ``failed``. A pending or failed
  check is open: the check step runs it again on the next refresh, and no
  write step builds the next layer on it. A dataset with no row has never
  been written since the gate existed and has no open check.

The registry is written only after Spark has committed the Bronze table. A
crash between the commit and the registry write leaves the objects
unregistered; the next run stages them again and the Bronze ``MERGE`` inserts
nothing for rows already present.

Every statement is parameterized. Object keys and ETags are values, never
SQL text. No row values are stored.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import psycopg

SCHEMA = "lakehouse_state"
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
# Gold is one dbt project: its pending flag is a single row.
GOLD_DATASET: tuple[str, str] = ("dbt", "gold")
LAYERS = (SILVER, GOLD)
# Layers whose writes are checked before the next layer builds on them.
CHECK_LAYERS = (BRONZE, SILVER)
CHECK_PENDING = "pending"
CHECK_FAILED = "failed"
CHECK_PASSED = "passed"

_IDENTIFIER = re.compile(r"^[a-z0-9_]+$")

DDL = (
    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.bronze_promotions (
        raw_object_key text PRIMARY KEY,
        etag text NOT NULL,
        source_id text NOT NULL,
        dataset text NOT NULL,
        row_count bigint NOT NULL CHECK (row_count >= 0),
        run_label text NOT NULL,
        promoted_at timestamptz NOT NULL
    )""",
    f"""CREATE INDEX IF NOT EXISTS bronze_promotions_dataset
        ON {SCHEMA}.bronze_promotions (source_id, dataset)""",
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.pending_layers (
        layer text NOT NULL CHECK (layer IN ('silver', 'gold')),
        source_id text NOT NULL,
        dataset text NOT NULL,
        marked_by_run text NOT NULL,
        marked_at timestamptz NOT NULL,
        PRIMARY KEY (layer, source_id, dataset)
    )""",
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.bronze_refreshes (
        run_label text NOT NULL,
        source_id text NOT NULL,
        dataset text NOT NULL,
        mode text NOT NULL,
        objects_listed integer NOT NULL,
        objects_incomplete integer NOT NULL,
        objects_promoted integer NOT NULL,
        rows_appended bigint NOT NULL,
        table_rows bigint,
        recorded_at timestamptz NOT NULL,
        PRIMARY KEY (run_label, source_id, dataset)
    )""",
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.layer_checks (
        layer text NOT NULL CHECK (layer IN ('bronze', 'silver')),
        source_id text NOT NULL,
        dataset text NOT NULL,
        status text NOT NULL CHECK (status IN ('pending', 'failed', 'passed')),
        expected_rows bigint CHECK (expected_rows >= 0),
        marked_by_run text NOT NULL,
        marked_at timestamptz NOT NULL,
        checked_by_run text,
        checked_at timestamptz,
        PRIMARY KEY (layer, source_id, dataset)
    )""",
)


class RegistryError(RuntimeError):
    """The registry cannot be read or written. Names the step, not values."""


@dataclass(frozen=True, slots=True)
class PromotedObject:
    """One raw object promoted into Bronze."""

    raw_object_key: str
    etag: str
    row_count: int


@dataclass(frozen=True, slots=True)
class RefreshRecord:
    """Counts for one dataset in one Bronze run."""

    source_id: str
    dataset: str
    mode: str
    objects_listed: int
    objects_incomplete: int
    objects_promoted: int
    rows_appended: int
    table_rows: int | None


@dataclass(frozen=True, slots=True)
class OpenCheck:
    """A check that is pending or failed for one dataset.

    ``expected_rows`` is the Bronze row total registered with the write that
    marked the check; Silver checks carry ``None``.
    """

    status: str
    marked_at: datetime
    expected_rows: int | None


class PromotionRegistry(Protocol):
    """What Bronze, Silver, and Gold need from the registry."""

    def promoted(self, source_id: str, dataset: str) -> dict[str, str]:
        """Return ``raw_object_key -> etag`` for one dataset."""
        ...

    def record_promotion(
        self,
        source_id: str,
        dataset: str,
        objects: Sequence[PromotedObject],
        *,
        run_label: str,
        replace_dataset: bool,
    ) -> int:
        """Register objects, mark Silver and the Bronze check pending.

        Return the dataset total. ``replace_dataset`` first removes every
        registered object of the dataset (a full rebuild).
        """
        ...

    def pending(self, layer: str) -> dict[tuple[str, str], datetime]:
        """Return the pending datasets of ``layer`` with their mark time."""
        ...

    def complete(
        self,
        layer: str,
        dataset: tuple[str, str],
        marked_at: datetime | None,
        *,
        run_label: str,
        mark_next: str | None,
    ) -> None:
        """Clear one pending flag if unchanged, and mark ``mark_next`` pending.

        ``marked_at`` is the mark read before the write started; ``None``
        clears nothing (a full rebuild of a dataset that was not pending).
        Completing ``silver`` also marks the Silver check of the dataset
        pending, because the table was written.
        """
        ...

    def open_checks(self, layer: str) -> dict[tuple[str, str], OpenCheck]:
        """Return the datasets whose ``layer`` check is pending or failed."""
        ...

    def record_check(
        self,
        layer: str,
        dataset: tuple[str, str],
        marked_at: datetime,
        *,
        passed: bool,
        run_label: str,
    ) -> None:
        """Store a check result if no write marked the check again meanwhile.

        A write that lands while the check runs leaves the check pending.
        """
        ...

    def record_refresh(self, run_label: str, records: Sequence[RefreshRecord]) -> None:
        """Store the per-dataset counts of one Bronze run."""
        ...


def catalog_db(env: Mapping[str, str]) -> str:
    """Return ``ICEBERG_CATALOG_DB`` after the simple-identifier check."""
    name = env.get("ICEBERG_CATALOG_DB", "iceberg")
    if not _IDENTIFIER.fullmatch(name):
        msg = "ICEBERG_CATALOG_DB must be a simple identifier"
        raise RegistryError(msg)
    return name


@contextmanager
def state_transaction(
    env: Mapping[str, str], label: str
) -> Iterator[psycopg.Connection]:
    """One transaction on the catalog database. Errors name ``label`` only."""
    import psycopg

    for name in ("POSTGRES_USER", "POSTGRES_PASSWORD"):
        if name not in env:
            msg = f"{label} needs {name}"
            raise RegistryError(msg)
    try:
        conn = psycopg.connect(
            host=env.get("POSTGRES_HOST", "postgres"),
            port=int(env.get("POSTGRES_PORT", "5432")),
            user=env["POSTGRES_USER"],
            password=env["POSTGRES_PASSWORD"],
            dbname=catalog_db(env),
        )
    except psycopg.Error as exc:
        msg = f"cannot connect to the {label}"
        raise RegistryError(msg) from exc
    try:
        with conn, conn.transaction():
            yield conn
    except psycopg.Error as exc:
        msg = f"{label} statement failed: {type(exc).__name__}"
        raise RegistryError(msg) from exc


def _require_layer(layer: str) -> None:
    if layer not in LAYERS:
        msg = f"unknown layer: {layer!r}"
        raise RegistryError(msg)


def _require_check_layer(layer: str) -> None:
    if layer not in CHECK_LAYERS:
        msg = f"unknown check layer: {layer!r}"
        raise RegistryError(msg)


class PostgresPromotionRegistry:
    """Registry in the catalog database. One short transaction per call."""

    def __init__(self, env: Mapping[str, str]) -> None:
        missing = [
            name for name in ("POSTGRES_USER", "POSTGRES_PASSWORD") if name not in env
        ]
        if missing:
            msg = f"promotion registry needs {', '.join(missing)}"
            raise RegistryError(msg)
        self._env = dict(env)
        catalog_db(env)
        with self._connect() as conn:
            for statement in DDL:
                conn.execute(statement)

    def _connect(self) -> AbstractContextManager[psycopg.Connection]:
        return state_transaction(self._env, "promotion registry")

    def promoted(self, source_id: str, dataset: str) -> dict[str, str]:
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT raw_object_key, etag FROM {SCHEMA}.bronze_promotions "
                "WHERE source_id = %s AND dataset = %s",
                (source_id, dataset),
            ).fetchall()
        return {str(key): str(etag) for key, etag in rows}

    def record_promotion(
        self,
        source_id: str,
        dataset: str,
        objects: Sequence[PromotedObject],
        *,
        run_label: str,
        replace_dataset: bool,
    ) -> int:
        now = datetime.now(UTC)
        with self._connect() as conn:
            if replace_dataset:
                conn.execute(
                    f"DELETE FROM {SCHEMA}.bronze_promotions "
                    "WHERE source_id = %s AND dataset = %s",
                    (source_id, dataset),
                )
            with conn.cursor() as cursor:
                cursor.executemany(
                    f"INSERT INTO {SCHEMA}.bronze_promotions "
                    "(raw_object_key, etag, source_id, dataset, row_count, "
                    "run_label, promoted_at) VALUES (%s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (raw_object_key) DO UPDATE SET "
                    "etag = EXCLUDED.etag, row_count = EXCLUDED.row_count, "
                    "run_label = EXCLUDED.run_label, "
                    "promoted_at = EXCLUDED.promoted_at",
                    [
                        (
                            item.raw_object_key,
                            item.etag,
                            source_id,
                            dataset,
                            item.row_count,
                            run_label,
                            now,
                        )
                        for item in objects
                    ],
                )
            _mark(conn, SILVER, (source_id, dataset), run_label, now)
            row = conn.execute(
                f"SELECT COALESCE(SUM(row_count), 0) FROM {SCHEMA}.bronze_promotions "
                "WHERE source_id = %s AND dataset = %s",
                (source_id, dataset),
            ).fetchone()
            total = int(row[0]) if row is not None else 0
            _mark_check(conn, BRONZE, (source_id, dataset), run_label, now, total)
        return total

    def pending(self, layer: str) -> dict[tuple[str, str], datetime]:
        _require_layer(layer)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT source_id, dataset, marked_at FROM {SCHEMA}.pending_layers "
                "WHERE layer = %s",
                (layer,),
            ).fetchall()
        return {(str(source), str(name)): marked for source, name, marked in rows}

    def complete(
        self,
        layer: str,
        dataset: tuple[str, str],
        marked_at: datetime | None,
        *,
        run_label: str,
        mark_next: str | None,
    ) -> None:
        _require_layer(layer)
        now = datetime.now(UTC)
        with self._connect() as conn:
            if marked_at is not None:
                conn.execute(
                    f"DELETE FROM {SCHEMA}.pending_layers WHERE layer = %s "
                    "AND source_id = %s AND dataset = %s AND marked_at = %s",
                    (layer, dataset[0], dataset[1], marked_at),
                )
            if layer == SILVER:
                _mark_check(conn, SILVER, dataset, run_label, now, None)
            if mark_next is not None:
                _require_layer(mark_next)
                target = GOLD_DATASET if mark_next == GOLD else dataset
                _mark(conn, mark_next, target, run_label, now)

    def open_checks(self, layer: str) -> dict[tuple[str, str], OpenCheck]:
        _require_check_layer(layer)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT source_id, dataset, status, marked_at, expected_rows "
                f"FROM {SCHEMA}.layer_checks WHERE layer = %s AND status <> %s",
                (layer, CHECK_PASSED),
            ).fetchall()
        return {
            (str(source), str(name)): OpenCheck(
                status=str(status),
                marked_at=marked,
                expected_rows=None if expected is None else int(expected),
            )
            for source, name, status, marked, expected in rows
        }

    def record_check(
        self,
        layer: str,
        dataset: tuple[str, str],
        marked_at: datetime,
        *,
        passed: bool,
        run_label: str,
    ) -> None:
        _require_check_layer(layer)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE {SCHEMA}.layer_checks SET status = %s, "
                "checked_by_run = %s, checked_at = %s "
                "WHERE layer = %s AND source_id = %s AND dataset = %s "
                "AND marked_at = %s",
                (
                    CHECK_PASSED if passed else CHECK_FAILED,
                    run_label,
                    datetime.now(UTC),
                    layer,
                    dataset[0],
                    dataset[1],
                    marked_at,
                ),
            )

    def record_refresh(self, run_label: str, records: Sequence[RefreshRecord]) -> None:
        if not records:
            return
        now = datetime.now(UTC)
        with self._connect() as conn, conn.cursor() as cursor:
            cursor.executemany(
                f"INSERT INTO {SCHEMA}.bronze_refreshes "
                "(run_label, source_id, dataset, mode, objects_listed, "
                "objects_incomplete, objects_promoted, rows_appended, table_rows, "
                "recorded_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (run_label, source_id, dataset) DO UPDATE SET "
                "mode = EXCLUDED.mode, objects_listed = EXCLUDED.objects_listed, "
                "objects_incomplete = EXCLUDED.objects_incomplete, "
                "objects_promoted = EXCLUDED.objects_promoted, "
                "rows_appended = EXCLUDED.rows_appended, "
                "table_rows = EXCLUDED.table_rows, "
                "recorded_at = EXCLUDED.recorded_at",
                [
                    (
                        run_label,
                        item.source_id,
                        item.dataset,
                        item.mode,
                        item.objects_listed,
                        item.objects_incomplete,
                        item.objects_promoted,
                        item.rows_appended,
                        item.table_rows,
                        now,
                    )
                    for item in records
                ],
            )


def _mark(
    conn: psycopg.Connection,
    layer: str,
    dataset: tuple[str, str],
    run_label: str,
    now: datetime,
) -> None:
    conn.execute(
        f"INSERT INTO {SCHEMA}.pending_layers "
        "(layer, source_id, dataset, marked_by_run, marked_at) "
        "VALUES (%s, %s, %s, %s, %s) "
        "ON CONFLICT (layer, source_id, dataset) DO UPDATE SET "
        "marked_by_run = EXCLUDED.marked_by_run, marked_at = EXCLUDED.marked_at",
        (layer, dataset[0], dataset[1], run_label, now),
    )


def _mark_check(
    conn: psycopg.Connection,
    layer: str,
    dataset: tuple[str, str],
    run_label: str,
    now: datetime,
    expected_rows: int | None,
) -> None:
    conn.execute(
        f"INSERT INTO {SCHEMA}.layer_checks "
        "(layer, source_id, dataset, status, expected_rows, marked_by_run, "
        "marked_at) VALUES (%s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (layer, source_id, dataset) DO UPDATE SET "
        "status = EXCLUDED.status, expected_rows = EXCLUDED.expected_rows, "
        "marked_by_run = EXCLUDED.marked_by_run, marked_at = EXCLUDED.marked_at, "
        "checked_by_run = NULL, checked_at = NULL",
        (layer, dataset[0], dataset[1], CHECK_PENDING, expected_rows, run_label, now),
    )


class InMemoryPromotionRegistry:
    """Same contract as :class:`PostgresPromotionRegistry`, in memory.

    Unit tests and local dry runs use it. Nothing persists.
    """

    def __init__(self) -> None:
        self.objects: dict[str, tuple[str, str, str, int, str]] = {}
        self.pending_rows: dict[tuple[str, str, str], tuple[str, datetime]] = {}
        self.refreshes: dict[tuple[str, str, str], RefreshRecord] = {}
        self.checks: dict[tuple[str, str, str], tuple[OpenCheck, str]] = {}
        self._clock = 0

    def _now(self) -> datetime:
        self._clock += 1
        return datetime(2026, 1, 1, tzinfo=UTC).replace(microsecond=self._clock)

    def promoted(self, source_id: str, dataset: str) -> dict[str, str]:
        return {
            key: etag
            for key, (etag, source, name, _rows, _run) in self.objects.items()
            if (source, name) == (source_id, dataset)
        }

    def record_promotion(
        self,
        source_id: str,
        dataset: str,
        objects: Sequence[PromotedObject],
        *,
        run_label: str,
        replace_dataset: bool,
    ) -> int:
        if replace_dataset:
            for key in list(self.promoted(source_id, dataset)):
                del self.objects[key]
        for item in objects:
            self.objects[item.raw_object_key] = (
                item.etag,
                source_id,
                dataset,
                item.row_count,
                run_label,
            )
        now = self._now()
        self.pending_rows[(SILVER, source_id, dataset)] = (run_label, now)
        total = sum(
            rows
            for _etag, source, name, rows, _run in self.objects.values()
            if (source, name) == (source_id, dataset)
        )
        self._mark_check(BRONZE, (source_id, dataset), run_label, now, total)
        return total

    def _mark_check(
        self,
        layer: str,
        dataset: tuple[str, str],
        run_label: str,
        now: datetime,
        expected_rows: int | None,
    ) -> None:
        self.checks[(layer, *dataset)] = (
            OpenCheck(status=CHECK_PENDING, marked_at=now, expected_rows=expected_rows),
            run_label,
        )

    def pending(self, layer: str) -> dict[tuple[str, str], datetime]:
        _require_layer(layer)
        return {
            (source, name): marked
            for (row_layer, source, name), (_run, marked) in self.pending_rows.items()
            if row_layer == layer
        }

    def complete(
        self,
        layer: str,
        dataset: tuple[str, str],
        marked_at: datetime | None,
        *,
        run_label: str,
        mark_next: str | None,
    ) -> None:
        _require_layer(layer)
        key = (layer, dataset[0], dataset[1])
        current = self.pending_rows.get(key)
        if current is not None and marked_at is not None and current[1] == marked_at:
            del self.pending_rows[key]
        now = self._now()
        if layer == SILVER:
            self._mark_check(SILVER, dataset, run_label, now, None)
        if mark_next is not None:
            _require_layer(mark_next)
            target = GOLD_DATASET if mark_next == GOLD else dataset
            self.pending_rows[(mark_next, *target)] = (run_label, now)

    def open_checks(self, layer: str) -> dict[tuple[str, str], OpenCheck]:
        _require_check_layer(layer)
        return {
            (source, name): check
            for (row_layer, source, name), (check, _run) in self.checks.items()
            if row_layer == layer and check.status != CHECK_PASSED
        }

    def record_check(
        self,
        layer: str,
        dataset: tuple[str, str],
        marked_at: datetime,
        *,
        passed: bool,
        run_label: str,
    ) -> None:
        _require_check_layer(layer)
        key = (layer, *dataset)
        current = self.checks.get(key)
        if current is None or current[0].marked_at != marked_at:
            return
        status = CHECK_PASSED if passed else CHECK_FAILED
        self.checks[key] = (
            OpenCheck(
                status=status,
                marked_at=marked_at,
                expected_rows=current[0].expected_rows,
            ),
            run_label,
        )

    def record_refresh(self, run_label: str, records: Sequence[RefreshRecord]) -> None:
        for item in records:
            self.refreshes[(run_label, item.source_id, item.dataset)] = item
