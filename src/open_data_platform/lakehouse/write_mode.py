"""Lakehouse write modes for Bronze and Silver (ADR-0024).

Since P3-13 the scheduled refresh is incremental at Bronze:

- Bronze appends the rows of raw objects not promoted before
  (``INCREMENTAL_APPEND``). One ``MERGE INTO`` per dataset inserts the staged
  rows that are not already in the table, so a retry after a commit whose
  registry write was lost inserts nothing.
- Silver rebuilds, as one ``CREATE OR REPLACE TABLE ... AS SELECT``, only the
  datasets that received new Bronze rows (``FULL_OVERWRITE`` per dataset).
- ``--full-rebuild`` rebuilds Bronze from every raw object of a complete
  execution and Silver from Bronze (``FULL_OVERWRITE``). It is for a contract
  version change or recovery and is never the scheduled path. A dataset that
  the promotion registry has never seen is bootstrapped the same way.

Every replace is one Iceberg commit (P3-11), so a reader never sees the table
missing or empty and a failed write keeps the previous snapshot.

``INCREMENTAL_MERGE`` (upsert by key) and ``APPEND_ONLY`` (windowed append
into Silver) stay placeholders. Selecting them fails loudly.
"""

from __future__ import annotations

from enum import Enum


class WriteMode(Enum):
    """How a lakehouse job commits rows into an Iceberg table."""

    FULL_OVERWRITE = "full_overwrite"
    INCREMENTAL_APPEND = "incremental_append"
    # Placeholders — not implemented. Selecting them must fail loudly.
    INCREMENTAL_MERGE = "incremental_merge"
    APPEND_ONLY = "append_only"


# Scheduled refresh: Bronze appends new raw objects; Silver replaces the
# datasets that changed.
BRONZE_WRITE_MODE = WriteMode.INCREMENTAL_APPEND
SILVER_WRITE_MODE = WriteMode.FULL_OVERWRITE
# ``--full-rebuild`` and a dataset's first promotion.
REBUILD_WRITE_MODE = WriteMode.FULL_OVERWRITE

IMPLEMENTED_WRITE_MODES: frozenset[WriteMode] = frozenset(
    {WriteMode.FULL_OVERWRITE, WriteMode.INCREMENTAL_APPEND}
)


class WriteModeError(ValueError):
    """Raised when a write mode is selected but not implemented."""


def require_implemented(write_mode: WriteMode, *, context: str) -> None:
    """Reject placeholder modes before rendering SQL or promoting data."""
    if write_mode in IMPLEMENTED_WRITE_MODES:
        return
    implemented = sorted(mode.value for mode in IMPLEMENTED_WRITE_MODES)
    msg = (
        f"{context}: write_mode {write_mode.value!r} is a placeholder; "
        f"only {implemented} are implemented"
    )
    raise WriteModeError(msg)


def render_incremental_merge_sql(*, context: str) -> str:
    """Placeholder for a future MERGE / upsert commit path.

    Keep call sites branching on ``WriteMode`` so incremental SQL can land
    here without changing Bronze/Silver field-mapping modules.
    """
    require_implemented(WriteMode.INCREMENTAL_MERGE, context=context)
    msg = "unreachable"  # pragma: no cover
    raise AssertionError(msg)


def render_append_only_sql(*, context: str) -> str:
    """Placeholder for a future append-only / windowed commit path."""
    require_implemented(WriteMode.APPEND_ONLY, context=context)
    msg = "unreachable"  # pragma: no cover
    raise AssertionError(msg)
