"""Lakehouse write modes for Bronze and Silver.

P2-03 / P2-04 implement **full load + overwrite** only: each run rebuilds the
whole Iceberg table from the current Bronze/raw inputs.

Incremental MERGE, append-only windows, and CDC upserts are **placeholders**.
Selecting them must fail loudly until a later phase implements the commit path
(P2-10 incremental-strategy docs, P3 schedules/backfills, P4 CDC).
"""

from __future__ import annotations

from enum import Enum


class WriteMode(Enum):
    """How a lakehouse job commits rows into an Iceberg table."""

    FULL_OVERWRITE = "full_overwrite"
    # Placeholders — not implemented. Selecting them must fail loudly.
    INCREMENTAL_MERGE = "incremental_merge"
    APPEND_ONLY = "append_only"


# Default for every Bronze and Silver path shipped in P2-03 / P2-04.
CURRENT_WRITE_MODE = WriteMode.FULL_OVERWRITE

IMPLEMENTED_WRITE_MODES: frozenset[WriteMode] = frozenset({WriteMode.FULL_OVERWRITE})


class WriteModeError(ValueError):
    """Raised when a write mode is selected but not implemented."""


def require_implemented(write_mode: WriteMode, *, context: str) -> None:
    """Reject placeholder modes before rendering SQL or promoting data."""
    if write_mode in IMPLEMENTED_WRITE_MODES:
        return
    implemented = [mode.value for mode in IMPLEMENTED_WRITE_MODES]
    msg = (
        f"{context}: write_mode {write_mode.value!r} is a placeholder; "
        f"only {implemented} is implemented (full load + overwrite)"
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
