"""Table specifications, value formatting, and planted record defects."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from open_data_platform.simulators.bioguardia.context import DayContext

Row = list[Any]


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    type: str  # string, integer, decimal(p,s), timestamp, date, boolean, json
    nullable: bool
    classification: str  # public, internal, pii
    description: str

    @property
    def scale(self) -> int | None:
        if self.type.startswith("decimal("):
            return int(self.type[:-1].split(",")[1])
        return None


@dataclass(frozen=True, slots=True)
class Defect:
    """A record defect a table can carry, and the column it touches."""

    kind: str  # invalid_value, malformed_timestamp, null_required, orphan_fk,
    #            duplicate_exact, duplicate_conflict
    column: str | None = None
    value: str | None = None


@dataclass(frozen=True)
class TableSpec:
    name: str
    source: str  # bioguardia_ops or bioguardia_finance
    change_type: str  # snapshot, change, append
    key: tuple[str, ...]
    grain: str
    description: str
    columns: tuple[Column, ...]
    rows: Callable[[DayContext], Iterator[Row]]
    payload_format: str = "csv"  # csv or jsonl
    totals: tuple[str, ...] = ()
    time_column: str | None = None
    defect_rate: float = 0.0
    defects: tuple[Defect, ...] = ()
    present: Callable[[DayContext], bool] = field(default=lambda ctx: True)
    since_version: dict[str, str] = field(default_factory=dict)

    @property
    def file_name(self) -> str:
        return f"{self.name}.{self.payload_format}.gz"

    def columns_for(self, ctx: DayContext) -> tuple[Column, ...]:
        """Columns present on the day (a schema change adds columns from a date)."""
        if not self.since_version:
            return self.columns
        return tuple(
            c for c in self.columns if c.name not in self.since_version or ctx.schema_v2
        )

    def schema_version(self, ctx: DayContext) -> int:
        return 2 if self.since_version and ctx.schema_v2 else 1

    def index(self, column: str) -> int:
        for position, item in enumerate(self.columns):
            if item.name == column:
                return position
        raise KeyError(column)


def col(
    name: str,
    kind: str,
    description: str,
    *,
    nullable: bool = False,
    classification: str = "internal",
) -> Column:
    return Column(name, kind, nullable, classification, description)


def dec(value: int, scale: int) -> str:
    """Format an integer of ``10**-scale`` units as a decimal string."""
    sign = "-" if value < 0 else ""
    whole, frac = divmod(abs(value), 10**scale)
    return f"{sign}{whole}.{frac:0{scale}d}" if scale else f"{sign}{whole}"


def coord(value: float) -> str:
    return f"{value:.6f}"


def flag(value: bool) -> str:
    return "true" if value else "false"


def parse_scaled(text: str, scale: int) -> int | None:
    """Back from ``dec`` to an integer; None for an empty or malformed value."""
    if not text:
        return None
    try:
        whole, _, frac = text.partition(".")
        frac = (frac + "0" * scale)[:scale] if scale else ""
        negative = whole.startswith("-")
        digits = int(whole.lstrip("-") or "0") * 10**scale + (int(frac) if frac else 0)
        return -digits if negative else digits
    except ValueError:
        return None


def apply_defect(spec: TableSpec, row: Row | dict[str, Any], defect: Defect) -> list:
    """The rows to write instead of ``row`` for one planted defect."""
    if defect.kind == "duplicate_exact":
        return [row, row.copy()]
    if isinstance(row, dict):
        changed = dict(row)
        if defect.kind == "duplicate_conflict":
            changed[defect.column] = defect.value
            return [row, changed]
        changed[defect.column] = (
            None if defect.kind == "null_required" else defect.value
        )
        return [changed]
    changed = list(row)
    position = spec.index(defect.column) if defect.column else -1
    if defect.kind == "duplicate_conflict":
        changed[position] = defect.value
        return [row, changed]
    changed[position] = "" if defect.kind == "null_required" else defect.value
    return [changed]
