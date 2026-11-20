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
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import psycopg

SCHEMA = "lakehouse_state"
SILVER = "silver"
GOLD = "gold"
# Gold is one dbt project: its pending flag is a single row.
GOLD_DATASET: tuple[str, str] = ("dbt", "gold")
LAYERS = (SILVER, GOLD)

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
        """Register objects, mark Silver pending, and return the dataset total.

        ``replace_dataset`` first removes every registered object of the
        dataset (a full rebuild).
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


def _require_layer(layer: str) -> None:
    if layer not in LAYERS:
        msg = f"unknown layer: {layer!r}"
        raise RegistryError(msg)


class PostgresPromotionRegistry:
    """Registry in the catalog database. One short transaction per call."""

    def __init__(self, env: Mapping[str, str]) -> None:
        self._env = dict(env)
        self._dbname = catalog_db(env)
        with self._connect() as conn:
            for statement in DDL:
                conn.execute(statement)

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
            msg = "cannot connect to the promotion registry"
            raise RegistryError(msg) from exc
        try:
            with conn, conn.transaction():
                yield conn
        except psycopg.Error as exc:
            msg = f"promotion registry statement failed: {type(exc).__name__}"
            raise RegistryError(msg) from exc

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
        return int(row[0]) if row is not None else 0

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
        with self._connect() as conn:
            if marked_at is not None:
                conn.execute(
                    f"DELETE FROM {SCHEMA}.pending_layers WHERE layer = %s "
                    "AND source_id = %s AND dataset = %s AND marked_at = %s",
                    (layer, dataset[0], dataset[1], marked_at),
                )
            if mark_next is not None:
                _require_layer(mark_next)
                target = GOLD_DATASET if mark_next == GOLD else dataset
                _mark(conn, mark_next, target, run_label, datetime.now(UTC))

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


class InMemoryPromotionRegistry:
    """Same contract as :class:`PostgresPromotionRegistry`, in memory.

    Unit tests and local dry runs use it. Nothing persists.
    """

    def __init__(self) -> None:
        self.objects: dict[str, tuple[str, str, str, int, str]] = {}
        self.pending_rows: dict[tuple[str, str, str], tuple[str, datetime]] = {}
        self.refreshes: dict[tuple[str, str, str], RefreshRecord] = {}
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
        self.pending_rows[(SILVER, source_id, dataset)] = (run_label, self._now())
        return sum(
            rows
            for _etag, source, name, rows, _run in self.objects.values()
            if (source, name) == (source_id, dataset)
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
        if mark_next is not None:
            _require_layer(mark_next)
            target = GOLD_DATASET if mark_next == GOLD else dataset
            self.pending_rows[(mark_next, *target)] = (run_label, self._now())

    def record_refresh(self, run_label: str, records: Sequence[RefreshRecord]) -> None:
        for item in records:
            self.refreshes[(run_label, item.source_id, item.dataset)] = item
