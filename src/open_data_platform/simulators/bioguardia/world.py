"""The BioGuardia world: reserves, zones, species, staff, and citizen reporters.

Everything is derived from the seed and the scale. Entities have their own
change timelines, so the state on any day can be computed without generating
the days before it. The device fleet and its telemetry live in ``fleet``.
"""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import cached_property, lru_cache

from open_data_platform.simulators.bioguardia.clock import (
    DAY_SECONDS,
    local_midnight,
)
from open_data_platform.simulators.bioguardia.geography import (
    MUNICIPALITIES,
    Municipality,
)
from open_data_platform.simulators.bioguardia.names import (
    COMMON_HEADS,
    COMMON_QUALIFIERS,
    EPITHETS,
    FAMILY_STEMS,
    GENUS_ENDINGS,
    GENUS_SYLLABLES,
    RESERVE_CATEGORIES,
    RESERVE_FEATURES,
)
from open_data_platform.simulators.bioguardia.pii import Person, person
from open_data_platform.simulators.bioguardia.rng import Rng, cumulative, zipf_weights

# Dates that anchor the world. Changes are simulated from WORLD_START on.
WORLD_START = date(2015, 1, 1)
REGISTRATION_START = date(2020, 3, 1)
ERASURE_START = date(2024, 1, 1)
DEFAULT_SCHEMA_V2_FROM = date(2026, 12, 15)

ZONE_TYPES: tuple[tuple[str, float], ...] = (
    ("core", 0.35),
    ("buffer", 0.25),
    ("restoration", 0.15),
    ("riparian", 0.15),
    ("sustainable_use", 0.10),
)
CONSERVATION_STATUSES: tuple[str, ...] = ("LC", "NT", "VU", "EN", "CR", "DD")
_STATUS_WEIGHTS = cumulative((0.60, 0.12, 0.10, 0.08, 0.04, 0.06))
TAXON_GROUPS: tuple[tuple[str, float], ...] = (
    ("bird", 0.40),
    ("mammal", 0.08),
    ("amphibian", 0.12),
    ("reptile", 0.06),
    ("insect", 0.14),
    ("tree", 0.15),
    ("plant", 0.05),
)
STAFF_ROLES: tuple[tuple[str, float], ...] = (
    ("ranger", 0.56),
    ("senior_ranger", 0.12),
    ("biologist", 0.10),
    ("field_technician", 0.13),
    ("pilot", 0.03),
    ("lab_analyst", 0.04),
    ("data_officer", 0.02),
)
EMPLOYMENT_TYPES: tuple[str, ...] = ("civil_servant", "contractor", "fellow")
PLAN_STATUSES: tuple[str, ...] = ("none", "draft", "approved", "under_revision")
FIRE_RISK_CLASSES: tuple[str, ...] = ("low", "moderate", "high", "very_high", "extreme")
# Relative fire activity per month in the Atlantic Forest (dry winter, wet summer).
FIRE_SEASON: tuple[float, ...] = (
    0.3, 0.3, 0.4, 0.6, 0.8, 1.0, 1.6, 2.4, 2.6, 1.8, 0.8, 0.4,
)  # fmt: skip
# Relative rainfall per month (wet summer).
RAIN_SEASON: tuple[float, ...] = (
    1.8, 1.6, 1.5, 1.0, 0.6, 0.4, 0.3, 0.3, 0.6, 1.0, 1.4, 1.8,
)  # fmt: skip
SPECIES_COUNT = 2500
CITIZENS_PER_DAY = 60.0
ERASURES_PER_DAY = 5.0


@dataclass(frozen=True, slots=True)
class WorldConfig:
    """The parameters that define one world. Change them and the data changes."""

    seed: int = 7
    scale: float = 1.0
    defects: bool = True
    schema_v2_from: date = DEFAULT_SCHEMA_V2_FROM
    rate_scale: float = 1.0
    buffer_hours: int = 12

    def __post_init__(self) -> None:
        if not 0 < self.scale <= 16:
            msg = "scale must be in (0, 16]"
            raise ValueError(msg)
        if not 0 < self.rate_scale <= 16:
            msg = "rate_scale must be in (0, 16]"
            raise ValueError(msg)
        if not 1 <= self.buffer_hours <= 24:
            msg = "buffer_hours must be from 1 to 24"
            raise ValueError(msg)

    def scaled(self, count: float, minimum: int = 1) -> int:
        return max(minimum, round(count * self.scale))

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> WorldConfig:
        values = os.environ if env is None else env
        defects = values.get("ODP_BIOGUARDIA_DEFECTS", "default").strip().lower()
        if defects not in {"default", "off"}:
            msg = "ODP_BIOGUARDIA_DEFECTS must be default or off"
            raise ValueError(msg)
        schema_from = values.get("ODP_BIOGUARDIA_SCHEMA_V2_FROM", "").strip()
        return cls(
            seed=int(values.get("ODP_BIOGUARDIA_SEED", "7")),
            scale=float(values.get("ODP_BIOGUARDIA_SCALE", "1")),
            defects=defects == "default",
            schema_v2_from=(
                date.fromisoformat(schema_from)
                if schema_from
                else DEFAULT_SCHEMA_V2_FROM
            ),
            rate_scale=float(values.get("ODP_TELEMETRY_RATE_SCALE", "1")),
            buffer_hours=int(values.get("ODP_TELEMETRY_BUFFER_HOURS", "12")),
        )


@dataclass(frozen=True, slots=True)
class Reserve:
    index: int
    reserve_id: str
    name: str
    category: str
    municipality: Municipality
    lat: float
    lon: float
    area_cents: int
    established_on: date
    weight: float


@dataclass(frozen=True, slots=True)
class Zone:
    index: int
    zone_id: str
    reserve: int
    zone_type: str
    lat: float
    lon: float
    area_cents: int
    elevation_m: int
    risk_factor: float


@dataclass(frozen=True, slots=True)
class Species:
    index: int
    species_id: str
    scientific_name: str
    common_name: str
    family: str
    taxon_group: str
    endemic: bool
    poaching_target: bool
    native_tree: bool
    wood_density_milli: int | None
    vocal: bool
    camera: bool


@dataclass(frozen=True, slots=True)
class StaffMember:
    """One person in a staff slot, valid from hire to termination."""

    slot: int
    generation: int
    staff_id: str
    hired_at: int
    terminated_at: int
    home_reserve: int
    first_role: str
    employment_type: str
    person: Person


@dataclass(frozen=True, slots=True)
class StaffState:
    """A staff member as the HR system records it at one instant."""

    member: StaffMember
    full_name: str
    email: str
    phone: str
    role: str
    team_code: str
    reserve: int
    effective_from: int
    status: str
    updated_at: int
    terminated: bool


@dataclass(frozen=True, slots=True)
class Citizen:
    index: int
    citizen_id: str
    registered_at: int
    person: Person
    has_cpf: bool
    has_email: bool
    has_phone: bool
    municipality_code: int
    channel: str
    consent_research: bool


@dataclass(frozen=True, slots=True)
class ErasureRequest:
    request_id: str
    citizen_index: int
    request_type: str
    requested_at: int
    effective_on: date


def _weighted_names(
    pairs: tuple[tuple[str, float], ...],
) -> tuple[list[str], list[float]]:
    return [name for name, _ in pairs], cumulative([w for _, w in pairs])


@dataclass
class World:
    """The static part of the world, built once per process."""

    config: WorldConfig
    reserves: list[Reserve] = field(default_factory=list)
    zones: list[Zone] = field(default_factory=list)
    zones_by_reserve: list[list[int]] = field(default_factory=list)
    species: list[Species] = field(default_factory=list)
    _erasure_cache: dict[date, tuple[ErasureRequest, ...]] = field(
        default_factory=dict, repr=False
    )

    def __post_init__(self) -> None:
        self._build_reserves()
        self._build_zones()
        self._build_species()

    # -- reserves and zones -------------------------------------------------

    def _build_reserves(self) -> None:
        cfg = self.config
        rng = Rng(cfg.seed, "reserves")
        count = max(4, min(200, round(120 * math.sqrt(cfg.scale))))
        raw = [rng.lognormal(0.0, 0.8) for _ in range(count - 1)]
        total = sum(raw)
        weights = [0.30] + [0.70 * w / total for w in raw]
        names: set[str] = set()
        for index in range(count):
            municipality = MUNICIPALITIES[
                index % len(MUNICIPALITIES)
                if index < len(MUNICIPALITIES)
                else rng.below(len(MUNICIPALITIES))
            ]
            category, label = (
                RESERVE_CATEGORIES[0] if index == 0 else rng.choice(RESERVE_CATEGORIES)
            )
            feature = RESERVE_FEATURES[(index * 7) % len(RESERVE_FEATURES)]
            name = f"{label} {feature}"
            suffix = 2
            while name in names:
                name = f"{label} {feature} {suffix}"
                suffix += 1
            names.add(name)
            area = 300_000.0 if index == 0 else min(250_000.0, rng.lognormal(9.4, 1.0))
            self.reserves.append(
                Reserve(
                    index=index,
                    reserve_id=f"RSV-{index + 1:04d}",
                    name=name,
                    category=category,
                    municipality=municipality,
                    lat=municipality.lat + rng.uniform(-0.12, 0.12),
                    lon=municipality.lon + rng.uniform(-0.12, 0.12),
                    area_cents=int(area * 100),
                    established_on=date(1950, 1, 1)
                    + timedelta(days=rng.below(65 * 365)),
                    weight=weights[index],
                )
            )

    def _build_zones(self) -> None:
        cfg = self.config
        rng = Rng(cfg.seed, "zones")
        total = max(len(self.reserves), cfg.scaled(6000))
        counts = _largest_remainder([r.weight for r in self.reserves], total)
        type_names, type_weights = _weighted_names(ZONE_TYPES)
        self.zones_by_reserve = [[] for _ in self.reserves]
        for reserve in self.reserves:
            n = counts[reserve.index]
            radius = math.sqrt(reserve.area_cents / 100 / 100) / 111.0  # km to degrees
            for _ in range(n):
                index = len(self.zones)
                zone_type = type_names[rng.weighted(type_weights)]
                area = reserve.area_cents / 100 / n * rng.lognormal(0.0, 0.3)
                self.zones.append(
                    Zone(
                        index=index,
                        zone_id=f"ZON-{index + 1:06d}",
                        reserve=reserve.index,
                        zone_type=zone_type,
                        lat=reserve.lat + rng.gauss(0.0, radius / 2),
                        lon=reserve.lon + rng.gauss(0.0, radius / 2),
                        area_cents=max(100, int(area * 100)),
                        elevation_m=rng.between(5, 1800),
                        risk_factor=rng.lognormal(0.0, 0.5),
                    )
                )
                self.zones_by_reserve[reserve.index].append(index)

    def fire_risk_class(self, zone: Zone, year: int, month: int) -> str:
        """The class the monthly assessment gives a zone for ``year``/``month``."""
        noise = Rng(self.config.seed, "risk", zone.index, year, month).random()
        level = FIRE_SEASON[month - 1] * zone.risk_factor * (0.6 + 0.8 * noise)
        thresholds = (0.5, 1.0, 1.8, 2.8)
        return FIRE_RISK_CLASSES[sum(level > t for t in thresholds)]

    @cached_property
    def zones_by_type(self) -> dict[str, list[int]]:
        grouped: dict[str, list[int]] = {}
        for zone in self.zones:
            grouped.setdefault(zone.zone_type, []).append(zone.index)
        return grouped

    # -- species ------------------------------------------------------------

    def _build_species(self) -> None:
        rng = Rng(self.config.seed, "species")
        group_names, group_weights = _weighted_names(TAXON_GROUPS)
        seen: set[str] = set()
        for index in range(SPECIES_COUNT):
            group = group_names[rng.weighted(group_weights)]
            while True:
                genus = rng.choice(GENUS_SYLLABLES) + rng.choice(GENUS_ENDINGS)
                scientific = f"{genus.capitalize()} {rng.choice(EPITHETS)}"
                if scientific not in seen:
                    seen.add(scientific)
                    break
            head = rng.choice(COMMON_HEADS[group])
            common = f"{head}-{rng.choice(COMMON_QUALIFIERS)}"
            tree = group == "tree"
            self.species.append(
                Species(
                    index=index,
                    species_id=f"SPC-{index + 1:04d}",
                    scientific_name=scientific,
                    common_name=common,
                    family=rng.choice(FAMILY_STEMS) + "idae"
                    if group not in {"tree", "plant"}
                    else rng.choice(FAMILY_STEMS) + "aceae",
                    taxon_group=group,
                    endemic=rng.chance(0.4),
                    poaching_target=group in {"bird", "mammal", "reptile"}
                    and rng.chance(0.08),
                    native_tree=tree,
                    wood_density_milli=rng.between(300, 950) if tree else None,
                    vocal=group in {"bird", "amphibian", "insect"}
                    or (group == "mammal" and rng.chance(0.4)),
                    camera=group in {"mammal", "reptile"}
                    or (group == "bird" and rng.chance(0.25)),
                )
            )

    def species_status(self, species: Species, until: int) -> tuple[str, int]:
        """Conservation status and assessment instant as recorded at ``until``."""
        rng = Rng(self.config.seed, "species-status", species.index)
        status = CONSERVATION_STATUSES[rng.weighted(_STATUS_WEIGHTS)]
        assessed = local_midnight(WORLD_START) - rng.below(3000) * DAY_SECONDS
        when = local_midnight(WORLD_START)
        while True:
            when += int(rng.exponential(1500.0) * DAY_SECONDS) + 30 * DAY_SECONDS
            if when > until:
                return status, assessed
            position = CONSERVATION_STATUSES.index(status)
            if status == "DD":
                position = rng.below(5)
            else:
                position = min(4, max(0, position + (1 if rng.chance(0.6) else -1)))
            status = CONSERVATION_STATUSES[position]
            assessed = when

    @cached_property
    def vocal_species(self) -> list[int]:
        return [s.index for s in self.species if s.vocal]

    @cached_property
    def camera_species(self) -> list[int]:
        return [s.index for s in self.species if s.camera]

    @cached_property
    def native_trees(self) -> list[int]:
        return [s.index for s in self.species if s.native_tree]

    def ranked_species(self, pool: str, reserve: int) -> tuple[list[int], list[float]]:
        """Species of a pool ordered by local frequency, and Zipf cumulative weights.

        The global order is shared; each reserve swaps a few ranks so the
        common species differ slightly between reserves.
        """
        return _ranked_species(self, pool, reserve)

    # -- staff --------------------------------------------------------------

    @cached_property
    def staff_slots(self) -> int:
        return max(len(self.reserves) + 3, self.config.scaled(6000))

    def staff_member(self, slot: int, generation: int) -> StaffMember:
        return _staff_member(self, slot, generation)

    def staff_at(self, slot: int, instant: int) -> StaffMember | None:
        """The member who holds ``slot`` at ``instant`` (hired, not yet terminated)."""
        generation = 0
        while True:
            member = self.staff_member(slot, generation)
            if instant < member.hired_at:
                return None
            if instant < member.terminated_at:
                return member
            generation += 1

    def staff_listed(self, slot: int, until: int) -> StaffMember | None:
        """The member a snapshot at ``until`` lists for ``slot``.

        A terminated member stays listed for 30 days, then leaves the snapshot.
        A member hired after the previous one left is listed from the hire.
        """
        generation = 0
        listed: StaffMember | None = None
        while True:
            member = self.staff_member(slot, generation)
            if member.hired_at > until:
                return listed
            if until < member.terminated_at + 30 * DAY_SECONDS:
                listed = member
            generation += 1

    def staff_state(self, member: StaffMember, until: int) -> StaffState:
        """The HR record of ``member`` as entered up to ``until``."""
        return _staff_state(self, member, until)

    # -- citizens -----------------------------------------------------------

    def citizens_registered(self, until: int) -> int:
        """How many citizens registered up to ``until``."""
        start = local_midnight(REGISTRATION_START)
        if until < start:
            return 0
        rate = CITIZENS_PER_DAY * self.config.scale
        return int((until - start) / DAY_SECONDS * rate)

    def citizen(self, index: int) -> Citizen:
        return _citizen(self, index)

    def erasures_on(self, day: date) -> tuple[ErasureRequest, ...]:
        """The erasure and consent-withdrawal requests received on ``day``."""
        cached = self._erasure_cache.get(day)
        if cached is None:
            cached = _erasures_on(self, day)
            self._erasure_cache[day] = cached
        return cached

    def requests_in_effect(
        self, until_day: date
    ) -> tuple[dict[int, ErasureRequest], dict[int, ErasureRequest]]:
        """Erasures and consent withdrawals in effect on ``until_day``, by citizen.

        Each map keeps the first request in effect for a citizen.
        """
        erased: dict[int, ErasureRequest] = {}
        withdrawn: dict[int, ErasureRequest] = {}
        day = ERASURE_START
        while day <= until_day:
            for request in self.erasures_on(day):
                if request.effective_on <= until_day:
                    target = erased if request.request_type == "erasure" else withdrawn
                    target.setdefault(request.citizen_index, request)
            day += timedelta(days=1)
        return erased, withdrawn


def _largest_remainder(weights: list[float], total: int) -> list[int]:
    """Split ``total`` by ``weights``; every share gets at least one."""
    count = len(weights)
    base = [1] * count
    rest = total - count
    if rest <= 0:
        return base
    weight_sum = sum(weights)
    raw = [rest * w / weight_sum for w in weights]
    floors = [int(x) for x in raw]
    remaining = rest - sum(floors)
    order = sorted(range(count), key=lambda i: raw[i] - floors[i], reverse=True)
    for i in order[:remaining]:
        floors[i] += 1
    return [b + f for b, f in zip(base, floors, strict=True)]


@lru_cache(maxsize=4096)
def _ranked_species_cached(
    seed: int, pool: tuple[int, ...], reserve: int
) -> tuple[list[int], list[float]]:
    rng = Rng(seed, "species-rank", reserve)
    order = list(pool)
    global_rng = Rng(seed, "species-rank-global", len(pool))
    # Global shuffle (Fisher-Yates) shared by every reserve.
    for i in range(len(order) - 1, 0, -1):
        j = global_rng.below(i + 1)
        order[i], order[j] = order[j], order[i]
    # A few local swaps among the top ranks.
    top = min(200, len(order))
    for _ in range(top // 4):
        i = rng.below(top)
        j = rng.below(top)
        order[i], order[j] = order[j], order[i]
    return order, cumulative(zipf_weights(len(order), 1.07))


def _ranked_species(
    world: World, pool: str, reserve: int
) -> tuple[list[int], list[float]]:
    members = {
        "vocal": world.vocal_species,
        "camera": world.camera_species,
        "tree": world.native_trees,
    }[pool]
    return _ranked_species_cached(world.config.seed, tuple(members), reserve)


_STAFF_CACHE: dict[tuple[int, int, int], StaffMember] = {}


def _staff_member(world: World, slot: int, generation: int) -> StaffMember:
    key = (id(world), slot, generation)
    cached = _STAFF_CACHE.get(key)
    if cached is not None:
        return cached
    seed = world.config.seed
    if generation == 0:
        rng = Rng(seed, "staff", slot, 0)
        hired = local_midnight(date(2005, 1, 1)) + rng.below(20 * 365) * DAY_SECONDS
    else:
        previous = _staff_member(world, slot, generation - 1)
        rng = Rng(seed, "staff", slot, generation)
        hired = previous.terminated_at + rng.between(15, 90) * DAY_SECONDS
    tenure = max(0.5 * 365, rng.exponential(9 * 365)) * DAY_SECONDS
    reserves = len(world.reserves)
    if slot < reserves:
        role = "reserve_manager"
        home = slot
    else:
        names, weights = _weighted_names(STAFF_ROLES)
        role = names[rng.weighted(weights)]
        home = world.zones[rng.below(len(world.zones))].reserve
    hire_day = date(1970, 1, 1) + timedelta(days=hired // DAY_SECONDS)
    member = StaffMember(
        slot=slot,
        generation=generation,
        staff_id=f"STF-{generation * 1_000_000 + slot + 1:07d}",
        hired_at=hired + rng.between(8, 17) * 3600,
        terminated_at=hired + int(tenure),
        home_reserve=home,
        first_role=role,
        employment_type=rng.choice(EMPLOYMENT_TYPES),
        person=person(Rng(seed, "staff-person", slot, generation), hire_day),
    )
    if len(_STAFF_CACHE) > 200_000:
        _STAFF_CACHE.clear()
    _STAFF_CACHE[key] = member
    return member


_PROMOTIONS = {"ranger": "senior_ranger", "field_technician": "biologist"}


def _staff_state(world: World, member: StaffMember, until: int) -> StaffState:
    seed = world.config.seed
    key = (member.slot, member.generation)
    reserve = member.home_reserve
    team = Rng(seed, "team", *key).below(6) + 1
    role = member.first_role
    effective_from = member.hired_at
    updated_at = member.hired_at
    full_name = member.person.full_name
    email = member.person.email
    phone = member.person.phone
    status = "active"
    limit = min(until, member.terminated_at)
    # Reassignments: recorded up to 14 days before they take effect (20%).
    rng = Rng(seed, "staff-move", *key)
    when = member.hired_at
    while member.slot >= len(world.reserves):
        when += int((rng.exponential(400.0) + 30) * DAY_SECONDS)
        early = rng.between(1, 14) * DAY_SECONDS if rng.chance(0.2) else 0
        new_reserve = world.zones[rng.below(len(world.zones))].reserve
        new_team = rng.below(6) + 1
        recorded = when - early
        if recorded > limit:
            break
        reserve, team, effective_from = new_reserve, new_team, when
        updated_at = max(updated_at, recorded)
    # Promotions.
    rng = Rng(seed, "staff-role", *key)
    when = member.hired_at
    while role in _PROMOTIONS:
        when += int((rng.exponential(1200.0) + 180) * DAY_SECONDS)
        if when > limit:
            break
        role = _PROMOTIONS[role]
        updated_at = max(updated_at, when)
    # Contact changes and the rare name change.
    for attribute, mean in (("phone", 900.0), ("email", 1500.0), ("name", 6000.0)):
        rng = Rng(seed, "staff-contact", attribute, *key)
        when = member.hired_at
        while True:
            when += int((rng.exponential(mean) + 30) * DAY_SECONDS)
            if when > limit:
                break
            replacement = person(rng, date(2000, 1, 1))
            if attribute == "phone":
                phone = replacement.phone
            elif attribute == "email":
                email = replacement.email
            else:
                full_name = f"{member.person.given} {replacement.surname}"
            updated_at = max(updated_at, when)
    # Leave windows.
    rng = Rng(seed, "staff-leave", *key)
    when = member.hired_at
    while True:
        when += int((rng.exponential(500.0) + 60) * DAY_SECONDS)
        length = rng.between(7, 60) * DAY_SECONDS
        if when > limit:
            break
        if when <= limit < when + length:
            status = "on_leave"
            updated_at = max(updated_at, when)
        elif when + length <= limit:
            updated_at = max(updated_at, when + length)
    terminated = until >= member.terminated_at
    if terminated:
        status = "terminated"
        updated_at = member.terminated_at
    return StaffState(
        member=member,
        full_name=full_name,
        email=email,
        phone=phone,
        role=role,
        team_code=f"T-{reserve + 1:03d}-{team}",
        reserve=reserve,
        effective_from=effective_from,
        status=status,
        updated_at=updated_at,
        terminated=terminated,
    )


_CITIZEN_CHANNELS = ("app", "hotline", "whatsapp", "email")


def _citizen(world: World, index: int) -> Citizen:
    seed = world.config.seed
    rng = Rng(seed, "citizen", index)
    start = local_midnight(REGISTRATION_START)
    rate = CITIZENS_PER_DAY * world.config.scale
    registered = start + int(index / rate * DAY_SECONDS) + rng.below(3600)
    municipality = MUNICIPALITIES[rng.below(len(MUNICIPALITIES))]
    return Citizen(
        index=index,
        citizen_id=f"CIT-{index + 1:07d}",
        registered_at=registered,
        person=person(rng, REGISTRATION_START),
        has_cpf=rng.chance(0.6),
        has_email=rng.chance(0.8),
        has_phone=rng.chance(0.9),
        municipality_code=municipality.code,
        channel=rng.choice(_CITIZEN_CHANNELS),
        consent_research=rng.chance(0.7),
    )


def _erasures_on(world: World, day: date) -> tuple[ErasureRequest, ...]:
    rng = Rng(world.config.seed, "erasure", day.isoformat())
    registered = world.citizens_registered(local_midnight(day))
    if registered == 0:
        return ()
    requests = []
    for n in range(rng.count(ERASURES_PER_DAY * world.config.scale)):
        kind = "erasure" if rng.chance(0.8) else "consent_withdrawal"
        requests.append(
            ErasureRequest(
                request_id=f"ERQ-{day:%Y%m%d}-{n + 1:04d}",
                citizen_index=rng.below(registered),
                request_type=kind,
                requested_at=local_midnight(day) + rng.between(8 * 3600, 20 * 3600),
                effective_on=day + timedelta(days=rng.between(0, 15)),
            )
        )
    return tuple(requests)


_WORLDS: dict[tuple[int, float], World] = {}


def build_world(config: WorldConfig) -> World:
    """The world for ``config``, built once per process."""
    key = (config.seed, config.scale)
    world = _WORLDS.get(key)
    if world is None or world.config != config:
        world = World(config)
        _WORLDS[key] = world
    return world
