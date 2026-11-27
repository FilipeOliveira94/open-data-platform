"""What a table generator needs for one export date and version."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from open_data_platform.simulators.bioguardia.clock import (
    DAY_SECONDS,
    LocalClock,
    local_midnight,
)
from open_data_platform.simulators.bioguardia.fleet import Fleet, fleet_for
from open_data_platform.simulators.bioguardia.rng import Rng
from open_data_platform.simulators.bioguardia.spec import Row, TableSpec, apply_defect
from open_data_platform.simulators.bioguardia.world import World


@dataclass
class Planted:
    """Defects planted in one drop: keys and types only, never values."""

    record_defects: list[dict[str, Any]] = field(default_factory=list)
    free_text_pii: list[dict[str, Any]] = field(default_factory=list)
    file_defects: list[dict[str, Any]] = field(default_factory=list)
    corrections: list[dict[str, Any]] = field(default_factory=list)

    def as_document(self) -> dict[str, Any]:
        return {
            "file_defects": self.file_defects,
            "record_defects": self.record_defects,
            "free_text_pii": self.free_text_pii,
            "corrections": self.corrections,
            "counts": {
                "file_defects": len(self.file_defects),
                "record_defects": len(self.record_defects),
                "free_text_pii": len(self.free_text_pii),
                "corrections": len(self.corrections),
            },
        }


@dataclass
class DayContext:
    """One export date: the world, the fleet, formatting, and what was planted."""

    world: World
    day: date
    version: int = 1
    planted: Planted = field(default_factory=Planted)
    cache: dict[Any, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.fleet: Fleet = fleet_for(self.world)
        self.config = self.world.config
        self.start = local_midnight(self.day)
        self.end = self.start + DAY_SECONDS
        # Patrols and lab results can carry instants of the day before.
        self.clock = LocalClock(self.day - timedelta(days=1), 2)

    @property
    def defects(self) -> bool:
        return self.config.defects

    @property
    def schema_v2(self) -> bool:
        return self.day >= self.config.schema_v2_from

    def rng(self, *parts: object) -> Rng:
        """A stream for one table and date; the version never changes it."""
        return Rng(self.config.seed, *parts, self.day.isoformat())

    def defect_stream(self, table: str) -> Rng:
        return Rng(self.config.seed, "defects", table, self.day.isoformat())

    def correction_stream(self, table: str) -> Rng:
        return Rng(self.config.seed, "corrections", table, self.day.isoformat())

    def emit(
        self, spec: TableSpec, row: Row, key: str, draw: float, pick: float
    ) -> list:
        """The rows to write for ``row``, with a planted defect when ``draw`` hits."""
        if not self.defects or not spec.defects or draw >= spec.defect_rate:
            return [row]
        defect = spec.defects[int(pick * len(spec.defects)) % len(spec.defects)]
        self.planted.record_defects.append(
            {
                "table": spec.name,
                "key": key,
                "defect": defect.kind,
                "column": defect.column,
            }
        )
        return apply_defect(spec, row, defect)

    def note_pii(
        self, table: str, key: str, column: str, kinds: tuple[str, ...]
    ) -> None:
        self.planted.free_text_pii.append(
            {"table": table, "key": key, "column": column, "types": list(kinds)}
        )

    def note_correction(self, table: str, key: str, column: str) -> None:
        self.planted.corrections.append({"table": table, "key": key, "column": column})
