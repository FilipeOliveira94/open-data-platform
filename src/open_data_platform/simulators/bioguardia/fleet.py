"""The device fleet, its change history, and what it emits each UTC day.

One model serves both feeds: the ``devices`` snapshot and the
``device_daily_counters`` table of the daily export, and the events the
telemetry producer publishes. A device emits on a fixed cadence with a
per-device phase; fire bursts, chattering, maintenance, and connectivity
outages change that schedule. Counting and listing emissions use the same
segments, so the counters and the stream agree by construction.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import cached_property

from open_data_platform.simulators.bioguardia.clock import (
    DAY_SECONDS,
    local_midnight,
    utc_midnight,
)
from open_data_platform.simulators.bioguardia.rng import Rng, splitmix64, unit
from open_data_platform.simulators.bioguardia.world import (
    FIRE_SEASON,
    RAIN_SEASON,
    World,
)


@dataclass(frozen=True, slots=True)
class DeviceType:
    name: str
    prefix: str
    count: int
    cadence: int  # seconds between readings; 0 when the type sends no readings
    primary_metric: str
    model_family: str


DEVICE_TYPES: tuple[DeviceType, ...] = (
    DeviceType("smoke_heat", "SMK", 12000, 120, "smoke_ppm", "SH"),
    DeviceType("weather", "WTH", 1500, 60, "temp_c", "WS"),
    DeviceType("river_gauge", "RVG", 800, 60, "water_level_cm", "RG"),
    DeviceType("soil_moisture", "SOL", 5000, 300, "vwc_pct", "SM"),
    DeviceType("acoustic", "ACU", 6000, 0, "", "AR"),
    DeviceType("camera_trap", "CAM", 8000, 0, "", "CT"),
)
TYPE_BY_NAME = {t.name: t for t in DEVICE_TYPES}
HEALTH_CADENCE = 240
FIRE_ALARM_CADENCE = 1
CHATTER_FACTOR = 30
GENERATION_SPACE = 1_000_000
# Calibration spread of the primary metric per type: offset sigma, gain sigma.
CALIBRATION_SPREAD = {
    "smoke_heat": (2.0, 0.03),
    "weather": (0.5, 0.02),
    "river_gauge": (3.0, 0.02),
    "soil_moisture": (1.5, 0.03),
}
POWER_SOURCES = ("solar", "battery", "solar_battery", "grid")

READINGS = "R"
HEALTH = "H"
ALERTS = "A"


@dataclass(frozen=True, slots=True)
class DeviceSlot:
    """A mounting position; devices replace each other in it over time."""

    kind: DeviceType
    slot: int
    zone: int
    lat: float
    lon: float
    phase: int
    health_phase: int
    detection_rate: float
    power_source: str


@dataclass(frozen=True, slots=True)
class Device:
    """One physical device: a generation of a slot, from install to decommission."""

    slot: DeviceSlot
    generation: int
    serial: int
    device_id: str
    installed_at: int
    decommissioned_at: int

    @property
    def kind(self) -> DeviceType:
        return self.slot.kind


@dataclass(frozen=True, slots=True)
class Calibration:
    calibrated_at: int
    entered_at: int
    offset_e4: int  # offset of the primary metric, 1e-4 units
    gain_e4: int  # gain, 1e-4 units (10000 = 1.0)


@dataclass(frozen=True, slots=True)
class DeviceState:
    """A device's attributes at an instant: true, or as recorded."""

    device: Device
    zone: int
    lat: float
    lon: float
    firmware: str
    calibration: Calibration | None
    status: str
    status_changed_at: int
    updated_at: int


@dataclass(frozen=True, slots=True)
class Segment:
    """Emissions at ``anchor + k * cadence`` inside ``[start, end)``."""

    start: int
    end: int
    cadence: int
    anchor: int

    def first_at_or_after(self, instant: int) -> int:
        lo = max(instant, self.start)
        steps = -((self.anchor - lo) // self.cadence)  # ceil((lo - anchor) / c)
        return self.anchor + max(0, steps) * self.cadence

    def count(self, lo: int, hi: int) -> int:
        """Emissions with time in ``[lo, hi)``."""
        lo = max(lo, self.start)
        hi = min(hi, self.end)
        if hi <= lo:
            return 0
        first = self.first_at_or_after(lo)
        if first >= hi:
            return 0
        return (hi - 1 - first) // self.cadence + 1

    def times(self, lo: int, hi: int) -> range:
        lo = max(lo, self.start)
        hi = min(hi, self.end)
        if hi <= lo:
            return range(0)
        return range(self.first_at_or_after(lo), hi, self.cadence)


@dataclass(frozen=True, slots=True)
class Alert:
    at: int
    alert_type: str
    severity: int
    confidence_e3: int


@dataclass(slots=True)
class DeviceDay:
    """What one device does during one UTC day."""

    device: Device
    zone: int
    firmware: str
    calibration: Calibration | None
    readings: list[Segment]
    health: list[Segment]
    alerts: list[Alert]
    outages: list[tuple[int, int]]
    skew: list[tuple[int, int, int]] = field(default_factory=list)
    calibrations: list[Calibration] = field(default_factory=list)

    def calibration_at(self, instant: int) -> Calibration | None:
        """The physical calibration in force at ``instant`` (by business time)."""
        found = None
        for calibration in self.calibrations:
            if calibration.calibrated_at <= instant:
                found = calibration
            else:
                break
        return found

    def stream_segments(self, stream: str) -> list[Segment]:
        return self.readings if stream == READINGS else self.health

    def count(self, stream: str, lo: int, hi: int) -> int:
        if stream == ALERTS:
            return sum(1 for a in self.alerts if lo <= a.at < hi)
        return sum(s.count(lo, hi) for s in self.stream_segments(stream))

    def emissions(
        self, stream: str, lo: int, hi: int, day_start: int
    ) -> Iterator[tuple[int, int]]:
        """(time, seq) of the stream's emissions in ``[lo, hi)``.

        ``seq`` counts from 00:00 UTC of the device-day.
        """
        if stream == ALERTS:
            for seq, alert in enumerate(self.alerts):
                if lo <= alert.at < hi:
                    yield alert.at, seq
            return
        before = 0
        for segment in self.stream_segments(stream):
            if segment.end <= lo:
                before += segment.count(day_start, segment.end)
                continue
            if segment.start >= hi:
                break
            offset = before + segment.count(day_start, max(lo, segment.start))
            for n, at in enumerate(segment.times(lo, hi)):
                yield at, offset + n
            before += segment.count(day_start, segment.end)


@dataclass(frozen=True, slots=True)
class FireEvent:
    zone: int
    start: int
    end: int
    intensity: float


class Fleet:
    """Device slots and generations for one world."""

    def __init__(self, world: World) -> None:
        self.world = world
        self.config = world.config
        self.slots: list[DeviceSlot] = []
        self._build_slots()
        self._generations: dict[tuple[str, int], list[Device]] = {}
        self._plans: dict[date, DayPlan] = {}
        self._fires: dict[date, list[FireEvent]] = {}
        self._threats: dict[date, dict[tuple[str, int], list[Alert]]] = {}

    # -- slots and generations ---------------------------------------------

    def _build_slots(self) -> None:
        world = self.world
        riparian = world.zones_by_type.get("riparian") or [z.index for z in world.zones]
        for kind in DEVICE_TYPES:
            rng = Rng(self.config.seed, "slots", kind.name)
            for slot in range(self.config.scaled(kind.count)):
                pool = riparian if kind.name == "river_gauge" else None
                zone_index = (
                    pool[rng.below(len(pool))] if pool else rng.below(len(world.zones))
                )
                zone = world.zones[zone_index]
                rate = rng.lognormal(0.0, 0.45)
                if kind.name == "acoustic" and rng.chance(0.02):
                    rate *= 5.0  # recorders by water: frog and insect choruses
                self.slots.append(
                    DeviceSlot(
                        kind=kind,
                        slot=slot,
                        zone=zone_index,
                        lat=zone.lat + rng.gauss(0.0, 0.01),
                        lon=zone.lon + rng.gauss(0.0, 0.01),
                        phase=rng.below(kind.cadence) if kind.cadence else 0,
                        health_phase=rng.below(HEALTH_CADENCE),
                        detection_rate=rate,
                        power_source=rng.choice(POWER_SOURCES),
                    )
                )

    @cached_property
    def slots_by_type(self) -> dict[str, list[DeviceSlot]]:
        grouped: dict[str, list[DeviceSlot]] = {}
        for slot in self.slots:
            grouped.setdefault(slot.kind.name, []).append(slot)
        return grouped

    def generations(self, slot: DeviceSlot, until: int) -> list[Device]:
        """Every device of ``slot`` installed up to ``until``."""
        key = (slot.kind.name, slot.slot)
        devices = self._generations.setdefault(key, [])
        while not devices or devices[-1].installed_at <= until:
            generation = len(devices)
            rng = Rng(self.config.seed, "device", slot.kind.name, slot.slot, generation)
            if generation == 0:
                installed = local_midnight(date(2019, 1, 1)) + rng.below(
                    int(6.5 * 365) * DAY_SECONDS
                )
            else:
                installed = (
                    devices[-1].decommissioned_at + rng.between(7, 60) * DAY_SECONDS
                )
            lifetime = max(365.0, rng.exponential(6 * 365.0)) * DAY_SECONDS
            serial = generation * GENERATION_SPACE + slot.slot + 1
            devices.append(
                Device(
                    slot=slot,
                    generation=generation,
                    serial=serial,
                    device_id=f"{slot.kind.prefix}-{serial:07d}",
                    installed_at=installed,
                    decommissioned_at=installed + int(lifetime),
                )
            )
            if devices[-1].installed_at > until:
                break
        return [d for d in devices if d.installed_at <= until]

    def device_at(self, slot: DeviceSlot, instant: int) -> Device | None:
        """The device mounted in ``slot`` at ``instant``."""
        for device in reversed(self.generations(slot, instant)):
            if device.installed_at <= instant < device.decommissioned_at:
                return device
            if device.installed_at <= instant:
                return None
        return None

    def listed(self, slot: DeviceSlot, until: int) -> list[Device]:
        """Devices a snapshot at ``until`` lists.

        Installed, and not more than 60 days past decommission.
        """
        keep = 60 * DAY_SECONDS
        return [
            d
            for d in self.generations(slot, until)
            if until < d.decommissioned_at + keep
        ]

    # -- device history -----------------------------------------------------

    def calibrations(self, device: Device, until: int) -> list[Calibration]:
        """Calibrations performed up to ``until`` (by business time)."""
        kind = device.kind
        if kind.name not in CALIBRATION_SPREAD:
            return []
        offset_sigma, gain_sigma = CALIBRATION_SPREAD[kind.name]
        rng = Rng(self.config.seed, "calibration", device.device_id)
        out: list[Calibration] = []
        when = device.installed_at
        limit = min(until, device.decommissioned_at)
        while when <= limit:
            late = rng.between(1, 5) * DAY_SECONDS if rng.chance(0.10) else 0
            out.append(
                Calibration(
                    calibrated_at=when,
                    entered_at=when + late + rng.below(4 * 3600),
                    offset_e4=round(rng.gauss(0.0, offset_sigma) * 10_000),
                    gain_e4=round((1.0 + rng.gauss(0.0, gain_sigma)) * 10_000),
                )
            )
            when += int((rng.exponential(70.0) + 20) * DAY_SECONDS)
        return out

    def _relocations(self, device: Device, until: int) -> list[tuple[int, int]]:
        rng = Rng(self.config.seed, "relocation", device.device_id)
        reserve_zones = self.world.zones_by_reserve[
            self.world.zones[device.slot.zone].reserve
        ]
        out: list[tuple[int, int]] = []
        when = device.installed_at
        limit = min(until, device.decommissioned_at)
        while True:
            when += int((rng.exponential(500.0) + 60) * DAY_SECONDS)
            if when > limit:
                return out
            out.append((when, reserve_zones[rng.below(len(reserve_zones))]))

    def maintenance(self, device: Device, until: int) -> list[tuple[int, int]]:
        """Maintenance windows ``[start, end)`` that started up to ``until``."""
        rng = Rng(self.config.seed, "maintenance", device.device_id)
        out: list[tuple[int, int]] = []
        when = device.installed_at
        limit = min(until, device.decommissioned_at)
        while True:
            when += int((rng.exponential(200.0) + 20) * DAY_SECONDS)
            length = rng.between(1, 10) * DAY_SECONDS
            if when > limit:
                return out
            out.append((when, min(when + length, device.decommissioned_at)))

    def _firmware_updates(self, device: Device, until: int) -> int:
        rng = Rng(self.config.seed, "firmware", device.device_id)
        count = 0
        when = device.installed_at
        limit = min(until, device.decommissioned_at)
        while True:
            when += int((rng.exponential(150.0) + 30) * DAY_SECONDS)
            if when > limit:
                return count
            count += 1

    @staticmethod
    def firmware_version(device: Device, updates: int) -> str:
        major = 2 + device.generation + updates // 8
        return f"{major}.{updates % 8}.{(device.serial + updates) % 5}"

    def state(self, device: Device, instant: int, *, recorded: bool) -> DeviceState:
        """The device at ``instant``: physical truth, or what the registry recorded."""
        calibrations = self.calibrations(device, instant)
        if recorded:
            calibrations = [c for c in calibrations if c.entered_at <= instant]
        calibration = calibrations[-1] if calibrations else None
        zone = device.slot.zone
        lat, lon = device.slot.lat, device.slot.lon
        updated = device.installed_at
        for moved_at, new_zone in self._relocations(device, instant):
            zone = new_zone
            target = self.world.zones[new_zone]
            jitter = Rng(self.config.seed, "relocated", device.device_id, moved_at)
            lat = target.lat + jitter.gauss(0.0, 0.01)
            lon = target.lon + jitter.gauss(0.0, 0.01)
            updated = max(updated, moved_at)
        status = "active"
        status_changed = device.installed_at
        for start, end in self.maintenance(device, instant):
            if start <= instant < end:
                status, status_changed = "maintenance", start
            elif end <= instant:
                status_changed = end
            updated = max(updated, status_changed)
        if instant >= device.decommissioned_at:
            status, status_changed = "decommissioned", device.decommissioned_at
            updated = max(updated, status_changed)
        updates = self._firmware_updates(device, instant)
        if calibration is not None:
            updated = max(
                updated,
                calibration.entered_at if recorded else calibration.calibrated_at,
            )
        return DeviceState(
            device=device,
            zone=zone,
            lat=lat,
            lon=lon,
            firmware=self.firmware_version(device, updates),
            calibration=calibration,
            status=status,
            status_changed_at=status_changed,
            updated_at=updated,
        )

    # -- world events -------------------------------------------------------

    def fires(self, day: date) -> list[FireEvent]:
        """Fires that start during UTC ``day``."""
        cached = self._fires.get(day)
        if cached is None:
            cached = self._compute_fires(day)
            if len(self._fires) > 800:
                self._fires.clear()
            self._fires[day] = cached
        return cached

    def threat_alerts(self, day: date) -> dict[tuple[str, int], list[Alert]]:
        """Chainsaw, gunshot, and vehicle detections during UTC ``day``."""
        cached = self._threats.get(day)
        if cached is None:
            cached = _threat_alerts(self, day, utc_midnight(day))
            if len(self._threats) > 800:
                self._threats.clear()
            self._threats[day] = cached
        return cached

    def _compute_fires(self, day: date) -> list[FireEvent]:
        rng = Rng(self.config.seed, "fires", day.isoformat())
        start = utc_midnight(day)
        season = FIRE_SEASON[day.month - 1]
        out: list[FireEvent] = []
        for zone in self.world.zones:
            if rng.random() < 0.0006 * season * zone.risk_factor:
                # Afternoon peak, local time: 12:00 to 19:00 is 15:00 to 22:00 UTC.
                hour = 15 + rng.gauss(2.5, 2.5)
                ignition = start + int(min(max(hour, 0.0), 23.9) * 3600)
                duration = int(min(10.0, max(1.0, rng.lognormal(1.1, 0.5))) * 3600)
                out.append(
                    FireEvent(
                        zone.index, ignition, ignition + duration, rng.uniform(0.5, 2.0)
                    )
                )
        return out

    def plan(self, day: date) -> DayPlan:
        """Everything the fleet emits during UTC ``day`` (cached, two days kept)."""
        plan = self._plans.get(day)
        if plan is None:
            plan = DayPlan.build(self, day)
            self._plans[day] = plan
            for old in sorted(self._plans)[:-3]:
                del self._plans[old]
        return plan


def _key(*values: int) -> int:
    h = 0x5DEECE66D
    for value in values:
        h = splitmix64(h ^ (value & 0xFFFFFFFFFFFFFFFF))
    return h


@dataclass
class DayPlan:
    """The emissions of every device during one UTC day."""

    day: date
    start: int
    end: int
    devices: list[DeviceDay]
    fires: list[FireEvent]
    chatter: dict[tuple[str, int], tuple[int, int]] = field(default_factory=dict)
    skewed: dict[tuple[str, int], tuple[int, int, int]] = field(default_factory=dict)

    @classmethod
    def build(cls, fleet: Fleet, day: date) -> DayPlan:
        cfg = fleet.config
        start = utc_midnight(day)
        end = start + DAY_SECONDS
        seed = cfg.seed
        day_key = day.toordinal()
        buffer = cfg.buffer_hours * 3600
        fires = fleet.fires(day) + [
            f for f in fleet.fires(day - timedelta(days=1)) if f.end > start
        ]
        burning: dict[int, list[FireEvent]] = {}
        for fire in fires:
            burning.setdefault(fire.zone, []).append(fire)
        rng = Rng(seed, "day-events", day.isoformat())
        chatter_pool = fleet.slots_by_type.get(
            "smoke_heat", []
        ) + fleet.slots_by_type.get("weather", [])
        chatter: dict[tuple[str, int], tuple[int, int]] = {}
        for _ in range(rng.count(3 * cfg.scale)):
            if not chatter_pool:
                break
            slot = chatter_pool[rng.below(len(chatter_pool))]
            begin = start + rng.below(DAY_SECONDS - 7 * 3600)
            chatter[(slot.kind.name, slot.slot)] = (
                begin,
                begin + rng.between(2, 6) * 3600,
            )
        skewed: dict[tuple[str, int], tuple[int, int, int]] = {}
        for _ in range(rng.count(2 * cfg.scale)):
            slot = fleet.slots[rng.below(len(fleet.slots))]
            begin = start + rng.below(DAY_SECONDS - 6 * 3600)
            stop = begin + rng.between(1, 6) * 3600
            offset = 7200 if rng.chance(0.5) else 946684800 - begin
            skewed[(slot.kind.name, slot.slot)] = (begin, stop, offset)
        threats = fleet.threat_alerts(day)
        season_rain = RAIN_SEASON[day.month - 1]
        devices: list[DeviceDay] = []
        for slot in fleet.slots:
            for device in fleet.generations(slot, end):
                if device.decommissioned_at <= start or device.installed_at >= end:
                    continue
                active_from = max(start, device.installed_at)
                active_to = min(end, device.decommissioned_at)
                truth = fleet.state(device, active_from, recorded=False)
                off = [
                    (max(a, active_from), min(b, active_to))
                    for a, b in fleet.maintenance(device, active_to)
                    if a < active_to and b > active_from
                ]
                windows = _subtract((active_from, active_to), off)
                kind = slot.kind
                cadence = (
                    max(1, round(kind.cadence / cfg.rate_scale)) if kind.cadence else 0
                )
                health_cadence = max(1, round(HEALTH_CADENCE / cfg.rate_scale))
                readings: list[Segment] = []
                if cadence:
                    bursts: list[tuple[int, int, int]] = []
                    if kind.name == "smoke_heat":
                        for fire in burning.get(truth.zone, ()):
                            bursts.append((fire.start, fire.end, FIRE_ALARM_CADENCE))
                    window = chatter.get((kind.name, slot.slot))
                    if window:
                        bursts.append(
                            (window[0], window[1], max(1, cadence // CHATTER_FACTOR))
                        )
                    base_anchor = start + slot.phase % cadence
                    for lo, hi in windows:
                        readings.extend(
                            _with_bursts(lo, hi, cadence, base_anchor, bursts)
                        )
                health = [
                    Segment(
                        lo,
                        hi,
                        health_cadence,
                        start + slot.health_phase % health_cadence,
                    )
                    for lo, hi in windows
                ]
                alerts = _device_alerts(
                    fleet,
                    slot,
                    device,
                    truth.zone,
                    day,
                    start,
                    burning,
                    threats,
                    season_rain,
                    windows,
                )
                outages = _outages(seed, device, day_key, start, buffer)
                skew = skewed.get((kind.name, slot.slot))
                history = fleet.calibrations(device, active_to)
                earlier = [c for c in history if c.calibrated_at <= active_from]
                in_day = [c for c in history if c.calibrated_at > active_from]
                devices.append(
                    DeviceDay(
                        device=device,
                        zone=truth.zone,
                        firmware=truth.firmware,
                        calibration=truth.calibration,
                        readings=readings,
                        health=health,
                        alerts=alerts,
                        outages=outages,
                        skew=[skew] if skew else [],
                        calibrations=earlier[-1:] + in_day,
                    )
                )
        return cls(
            day=day,
            start=start,
            end=end,
            devices=devices,
            fires=fires,
            chatter=chatter,
            skewed=skewed,
        )

    def counters(
        self, device_day: DeviceDay, buffer_seconds: int
    ) -> tuple[int, int, int, int, int]:
        """Generated per stream, messages dropped on the device, and outage seconds."""
        lo, hi = self.start, self.end
        readings = device_day.count(READINGS, lo, hi)
        health = device_day.count(HEALTH, lo, hi)
        alerts = device_day.count(ALERTS, lo, hi)
        dropped = 0
        outage_seconds = 0
        for begin, finish in device_day.outages:
            outage_seconds += max(0, min(finish, hi) - max(begin, lo))
            cut = finish - buffer_seconds
            if cut > begin:
                a, b = max(begin, lo), min(cut, hi)
                if b > a:
                    dropped += sum(
                        device_day.count(s, a, b) for s in (READINGS, HEALTH, ALERTS)
                    )
        return readings, health, alerts, dropped, outage_seconds


def _subtract(
    window: tuple[int, int], holes: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    pieces = [window]
    for hole_lo, hole_hi in sorted(holes):
        next_pieces = []
        for lo, hi in pieces:
            if hole_hi <= lo or hole_lo >= hi:
                next_pieces.append((lo, hi))
                continue
            if hole_lo > lo:
                next_pieces.append((lo, hole_lo))
            if hole_hi < hi:
                next_pieces.append((hole_hi, hi))
        pieces = next_pieces
    return [(lo, hi) for lo, hi in pieces if hi > lo]


def _with_bursts(
    lo: int, hi: int, cadence: int, anchor: int, bursts: list[tuple[int, int, int]]
) -> list[Segment]:
    """Base segments in ``[lo, hi)`` with burst segments cut in."""
    inside = sorted(
        (max(a, lo), min(b, hi), c) for a, b, c in bursts if a < hi and b > lo
    )
    segments: list[Segment] = []
    cursor = lo
    for a, b, c in inside:
        if a < cursor:
            a = cursor
        if b <= a:
            continue
        if a > cursor:
            segments.append(Segment(cursor, a, cadence, anchor))
        segments.append(Segment(a, b, c, a))
        cursor = b
    if cursor < hi:
        segments.append(Segment(cursor, hi, cadence, anchor))
    return segments


def _outages(
    seed: int, device: Device, day_key: int, start: int, buffer: int
) -> list[tuple[int, int]]:
    """Connectivity outages that touch this UTC day, overlapping ones merged.

    An outage starts on its own day and lasts at most 18 hours, so outages of
    neighbouring days can overlap. They are merged into one longer outage,
    the same way on every day they touch, so counters and deliveries agree.
    """
    raw: list[tuple[int, int]] = []
    for offset in range(-3, 3):
        key = _key(seed, device.serial, day_key + offset, 0x0A7)
        if unit(key) >= 0.015:
            continue
        day_start = start + offset * DAY_SECONDS
        begin = day_start + int(unit(key + 1) * DAY_SECONDS)
        median = 3 * 3600
        length = int(min(18 * 3600, median * math.exp(0.9 * _normal(key + 2))))
        raw.append((begin, begin + max(600, length)))
    return [
        (begin, finish)
        for begin, finish in merge_intervals(raw)
        if finish >= start and begin < start + DAY_SECONDS
    ]


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Union of half-open intervals, sorted; touching intervals are joined."""
    merged: list[tuple[int, int]] = []
    for begin, finish in sorted(intervals):
        if merged and begin <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], finish))
        else:
            merged.append((begin, finish))
    return merged


def _normal(key: int) -> float:
    u1 = 1.0 - unit(key)
    u2 = unit(key + 7)
    return math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)


def _threat_alerts(
    fleet: Fleet, day: date, start: int
) -> dict[tuple[str, int], list[Alert]]:
    """Chainsaw, gunshot, and vehicle detections by acoustic recorders."""
    rng = Rng(fleet.config.seed, "threats", day.isoformat())
    recorders = fleet.slots_by_type.get("acoustic", [])
    by_zone: dict[int, list[DeviceSlot]] = {}
    for slot in recorders:
        by_zone.setdefault(slot.zone, []).append(slot)
    out: dict[tuple[str, int], list[Alert]] = {}
    scale = fleet.config.scale
    for alert_type, mean, severity in (
        ("chainsaw", 20, 4),
        ("gunshot", 10, 5),
        ("vehicle", 30, 2),
    ):
        for _ in range(rng.count(mean * scale)):
            if not recorders:
                break
            origin = recorders[rng.below(len(recorders))]
            at = start + rng.below(DAY_SECONDS)
            neighbours = by_zone.get(origin.zone, [origin])
            heard = [origin] + [s for s in neighbours if s is not origin][
                : rng.below(3)
            ]
            for slot in heard:
                out.setdefault(("acoustic", slot.slot), []).append(
                    Alert(
                        at + rng.below(20), alert_type, severity, rng.between(550, 990)
                    )
                )
    return out


def _device_alerts(
    fleet: Fleet,
    slot: DeviceSlot,
    device: Device,
    zone: int,
    day: date,
    start: int,
    burning: dict[int, list[FireEvent]],
    threats: dict[tuple[str, int], list[Alert]],
    season_rain: float,
    windows: list[tuple[int, int]],
) -> list[Alert]:
    seed = fleet.config.seed
    alerts: list[Alert] = []
    kind = slot.kind.name
    key = _key(seed, device.serial, day.toordinal(), 0xA1)
    if kind == "smoke_heat":
        for fire in burning.get(zone, ()):
            if start <= fire.start < start + DAY_SECONDS:
                k = _key(key, fire.start)
                alerts.append(
                    Alert(
                        fire.start + int(unit(k) * 900),
                        "smoke_alarm",
                        5,
                        900 + int(unit(k + 1) * 99),
                    )
                )
                if unit(k + 2) < 0.5:
                    alerts.append(
                        Alert(
                            fire.start + 300 + int(unit(k + 3) * 900),
                            "heat_spike",
                            4,
                            800 + int(unit(k + 4) * 199),
                        )
                    )
    elif kind == "acoustic":
        alerts.extend(threats.get(("acoustic", slot.slot), ()))
    elif kind == "river_gauge":
        if unit(key) < 0.02 * season_rain:
            at = start + int(unit(key + 1) * DAY_SECONDS)
            alerts.append(Alert(at, "water_level_high", 3, 950))
            if unit(key + 2) < 0.2:
                alerts.append(
                    Alert(
                        at + 1800 + int(unit(key + 3) * 5400),
                        "water_level_critical",
                        5,
                        980,
                    )
                )
    elif kind == "camera_trap" and unit(key) < 0.005:
        alerts.append(
            Alert(
                start + int(unit(key + 1) * DAY_SECONDS),
                "human_presence",
                3,
                700 + int(unit(key + 2) * 290),
            )
        )
    if unit(key + 11) < 0.0001:
        alerts.append(
            Alert(start + int(unit(key + 12) * DAY_SECONDS), "tamper", 4, 990)
        )
    if unit(key + 13) < 0.002:
        alerts.append(
            Alert(start + int(unit(key + 14) * DAY_SECONDS), "low_battery", 2, 999)
        )
    inside = [a for a in alerts if any(lo <= a.at < hi for lo, hi in windows)]
    inside.sort(key=lambda a: (a.at, a.alert_type))
    return inside


_FLEETS: dict[int, Fleet] = {}


def fleet_for(world: World) -> Fleet:
    """The fleet of ``world``, built once per process."""
    fleet = _FLEETS.get(id(world))
    if fleet is None or fleet.world is not world:
        fleet = Fleet(world)
        _FLEETS[id(world)] = fleet
    return fleet
