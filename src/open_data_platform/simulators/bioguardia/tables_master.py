"""Master tables: full snapshots as of the end of the export date."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, timedelta

from open_data_platform.simulators.bioguardia import OPS_SOURCE
from open_data_platform.simulators.bioguardia.clock import DAY_SECONDS, local_midnight
from open_data_platform.simulators.bioguardia.context import DayContext
from open_data_platform.simulators.bioguardia.rng import Rng
from open_data_platform.simulators.bioguardia.spec import (
    Defect,
    Row,
    TableSpec,
    col,
    coord,
    dec,
    flag,
)
from open_data_platform.simulators.bioguardia.world import (
    PLAN_STATUSES,
    WORLD_START,
)


def local_day_text(epoch: int) -> str:
    """The local calendar date of an instant, as text."""
    return (
        date(1970, 1, 1) + timedelta(days=(epoch - 3 * 3600) // DAY_SECONDS)
    ).isoformat()


def _timeline(rng: Rng, start: int, mean_days: float, until: int) -> list[int]:
    out: list[int] = []
    when = start
    while True:
        when += int((rng.exponential(mean_days) + 30) * DAY_SECONDS)
        if when > until:
            return out
        out.append(when)


# -- reserves ---------------------------------------------------------------


def reserves_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    until = ctx.end - 1
    origin = local_midnight(WORLD_START)
    defects = ctx.defect_stream("reserves")
    for reserve in world.reserves:
        seed = ctx.config.seed
        plan_changes = _timeline(Rng(seed, "plan", reserve.index), origin, 900.0, until)
        plan_status = PLAN_STATUSES[
            (len(plan_changes) + reserve.index) % len(PLAN_STATUSES)
        ]
        area = reserve.area_cents
        area_rng = Rng(seed, "area", reserve.index)
        revisions = _timeline(area_rng, origin, 2000.0, until)
        for _ in revisions:
            area = int(area * (1 + area_rng.uniform(-0.05, 0.05)))
        tier_changes = _timeline(
            Rng(seed, "tier", reserve.index), origin, 3000.0, until
        )
        tier = 1 + (reserve.index + len(tier_changes)) % 3
        manager = world.staff_at(reserve.index, until)
        updated = max(
            [origin, *plan_changes[-1:], *revisions[-1:], *tier_changes[-1:]]
            + ([manager.hired_at] if manager else [])
        )
        row = [
            reserve.reserve_id,
            reserve.name,
            reserve.category,
            reserve.municipality.uf,
            str(reserve.municipality.code),
            reserve.municipality.name,
            coord(reserve.lat),
            coord(reserve.lon),
            dec(area, 2),
            reserve.established_on.isoformat(),
            str(tier),
            plan_status,
            manager.staff_id if manager else "",
            local_day_text(manager.hired_at) if manager else "",
            local_day_text(revisions[-1]) if revisions else "",
            ctx.clock.fmt(updated),
        ]
        yield from ctx.emit(
            RESERVES, row, reserve.reserve_id, defects.random(), defects.random()
        )


RESERVES = TableSpec(
    name="reserves",
    source=OPS_SOURCE,
    change_type="snapshot",
    key=("reserve_id",),
    grain="One protected reserve as recorded at the end of the export date.",
    description="Protected reserves the agency manages.",
    columns=(
        col("reserve_id", "string", "Reserve identifier, `RSV-` and four digits."),
        col(
            "reserve_name", "string", "Generated reserve name.", classification="public"
        ),
        col(
            "category",
            "string",
            "Closed set: state_park, national_park, biological_reserve, ecological_station, private_natural_heritage_reserve, environmental_protection_area, wildlife_refuge.",
            classification="public",
        ),
        col("uf", "string", "State code of the municipality.", classification="public"),
        col(
            "municipality_code",
            "integer",
            "IBGE seven-digit municipality code (real reference code).",
            classification="public",
        ),
        col(
            "municipality_name",
            "string",
            "IBGE municipality name.",
            classification="public",
        ),
        col(
            "centroid_lat",
            "decimal(9,6)",
            "Approximate reserve centroid latitude.",
            classification="public",
        ),
        col(
            "centroid_lon",
            "decimal(9,6)",
            "Approximate reserve centroid longitude.",
            classification="public",
        ),
        col(
            "area_ha",
            "decimal(12,2)",
            "Area in hectares after the latest boundary revision.",
            classification="public",
        ),
        col(
            "established_on",
            "date",
            "Date the reserve was established.",
            classification="public",
        ),
        col(
            "protection_tier",
            "integer",
            "1 (strict) to 3 (sustainable use).",
            classification="public",
        ),
        col(
            "management_plan_status",
            "string",
            "Closed set: none, draft, approved, under_revision.",
            classification="public",
        ),
        col(
            "manager_staff_id",
            "string",
            "Staff id of the current manager. Empty while the post is vacant.",
            nullable=True,
        ),
        col(
            "manager_assigned_on",
            "date",
            "Date the current manager took the post.",
            nullable=True,
        ),
        col(
            "area_revised_on",
            "date",
            "Date of the latest boundary revision.",
            nullable=True,
            classification="public",
        ),
        col(
            "updated_at",
            "timestamp",
            "Last change to the record (system time), local time with offset.",
        ),
    ),
    rows=reserves_rows,
    totals=("area_ha",),
    defect_rate=0.004,
    defects=(Defect("duplicate_exact"), Defect("invalid_value", "area_ha", "-1250.00")),
)


# -- zones ------------------------------------------------------------------


def _risk_since(ctx: DayContext, zone, day: date) -> date:
    """First day of the month in which the zone's current risk class began."""
    world = ctx.world
    year, month = day.year, day.month
    current = world.fire_risk_class(zone, year, month)
    for _ in range(24):
        prev_year, prev_month = (year, month - 1) if month > 1 else (year - 1, 12)
        if world.fire_risk_class(zone, prev_year, prev_month) != current:
            return date(year, month, 1)
        year, month = prev_year, prev_month
    return date(year, month, 1)


def zones_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    defects = ctx.defect_stream("zones")
    month_start = date(ctx.day.year, ctx.day.month, 1)
    for zone in world.zones:
        reserve = world.reserves[zone.reserve]
        risk = world.fire_risk_class(zone, ctx.day.year, ctx.day.month)
        since = _risk_since(ctx, zone, ctx.day)
        row = [
            zone.zone_id,
            reserve.reserve_id,
            f"{reserve.reserve_id[4:]}-{zone.index % 1000:03d}",
            zone.zone_type,
            coord(zone.lat),
            coord(zone.lon),
            dec(zone.area_cents, 2),
            str(zone.elevation_m),
            risk,
            month_start.isoformat(),
            ctx.clock.fmt(local_midnight(since) + 4 * 3600),
        ]
        yield from ctx.emit(
            ZONES, row, zone.zone_id, defects.random(), defects.random()
        )


ZONES = TableSpec(
    name="zones",
    source=OPS_SOURCE,
    change_type="snapshot",
    key=("zone_id",),
    grain="One monitoring zone as recorded at the end of the export date.",
    description="Monitoring zones inside reserves, with the monthly fire-risk class.",
    columns=(
        col("zone_id", "string", "Zone identifier, `ZON-` and six digits."),
        col("reserve_id", "string", "Reserve the zone belongs to."),
        col(
            "zone_code",
            "string",
            "Field code of the zone inside its reserve.",
            classification="public",
        ),
        col(
            "zone_type",
            "string",
            "Closed set: core, buffer, restoration, riparian, sustainable_use.",
            classification="public",
        ),
        col(
            "centroid_lat",
            "decimal(9,6)",
            "Zone centroid latitude.",
            classification="public",
        ),
        col(
            "centroid_lon",
            "decimal(9,6)",
            "Zone centroid longitude.",
            classification="public",
        ),
        col(
            "area_ha",
            "decimal(10,2)",
            "Zone area in hectares.",
            classification="public",
        ),
        col(
            "elevation_m",
            "integer",
            "Mean elevation in meters.",
            classification="public",
        ),
        col(
            "fire_risk_class",
            "string",
            "Closed set: low, moderate, high, very_high, extreme. Recomputed on the first day of each month.",
            classification="public",
        ),
        col(
            "risk_assessed_on",
            "date",
            "First day of the month of the current assessment.",
            classification="public",
        ),
        col(
            "updated_at",
            "timestamp",
            "When the current risk class took effect (system time).",
        ),
    ),
    rows=zones_rows,
    totals=("area_ha",),
    defect_rate=0.001,
    defects=(Defect("orphan_fk", "reserve_id", "RSV-9999"), Defect("duplicate_exact")),
)


# -- staff ------------------------------------------------------------------


def staff_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    until = ctx.end - 1
    defects = ctx.defect_stream("staff")
    for slot in range(world.staff_slots):
        member = world.staff_listed(slot, until)
        if member is None:
            continue
        state = world.staff_state(member, until)
        person = member.person
        home = world.reserves[member.home_reserve].municipality.code
        row = [
            member.staff_id,
            state.full_name,
            person.cpf,
            state.email,
            state.phone,
            person.birth_date.isoformat(),
            str(home),
            state.role,
            state.team_code,
            world.reserves[state.reserve].reserve_id,
            local_day_text(state.effective_from),
            member.employment_type,
            local_day_text(member.hired_at),
            local_day_text(member.terminated_at) if state.terminated else "",
            state.status,
            ctx.clock.fmt(state.updated_at),
        ]
        yield from ctx.emit(
            STAFF, row, member.staff_id, defects.random(), defects.random()
        )


STAFF = TableSpec(
    name="staff",
    source=OPS_SOURCE,
    change_type="snapshot",
    key=("staff_id",),
    grain="One staff member as recorded by HR at the end of the export date. A terminated member stays listed for 30 days.",
    description="Agency staff with direct identifiers. Every value is synthetic; see the domain model for the safety rules.",
    columns=(
        col("staff_id", "string", "Staff identifier, `STF-` and seven digits."),
        col("full_name", "string", "Generated full name.", classification="pii"),
        col(
            "cpf",
            "string",
            "CPF-shaped value with a wrong check digit.",
            classification="pii",
        ),
        col(
            "email",
            "string",
            "Address under a reserved `.example` domain.",
            classification="pii",
        ),
        col(
            "phone",
            "string",
            "Mobile number with an unassigned area code.",
            classification="pii",
        ),
        col("birth_date", "date", "Generated birth date.", classification="pii"),
        col(
            "home_municipality_code",
            "integer",
            "IBGE code of the home municipality.",
            classification="pii",
        ),
        col(
            "role",
            "string",
            "Closed set: reserve_manager, ranger, senior_ranger, biologist, field_technician, pilot, lab_analyst, data_officer.",
        ),
        col("team_code", "string", "Team inside the assigned reserve."),
        col(
            "assigned_reserve_id",
            "string",
            "Reserve of the current or upcoming assignment.",
        ),
        col(
            "assignment_effective_from",
            "date",
            "Date the assignment takes effect; can be in the future when HR records it early.",
        ),
        col(
            "employment_type",
            "string",
            "Closed set: civil_servant, contractor, fellow.",
        ),
        col("hire_date", "date", "Hire date."),
        col(
            "termination_date",
            "date",
            "Termination date; empty while employed.",
            nullable=True,
        ),
        col("status", "string", "Closed set: active, on_leave, terminated."),
        col("updated_at", "timestamp", "Last change entered by HR (system time)."),
    ),
    rows=staff_rows,
    defect_rate=0.001,
    defects=(
        Defect("duplicate_conflict", "status", "suspended"),
        Defect("null_required", "role"),
    ),
)


# -- devices ----------------------------------------------------------------


def devices_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    fleet = ctx.fleet
    until = ctx.end - 1
    v2 = ctx.schema_v2
    defects = ctx.defect_stream("devices")
    for slot in fleet.slots:
        for device in fleet.listed(slot, until):
            state = fleet.state(device, until, recorded=True)
            zone = world.zones[state.zone]
            calibration = state.calibration
            row = [
                device.device_id,
                slot.kind.name,
                f"{slot.kind.model_family}-{200 + 100 * min(device.generation, 3)}",
                world.reserves[zone.reserve].reserve_id,
                zone.zone_id,
                coord(state.lat),
                coord(state.lon),
                local_day_text(device.installed_at),
                state.firmware,
                dec(calibration.offset_e4, 4) if calibration else "",
                dec(calibration.gain_e4, 4) if calibration else "",
                ctx.clock.fmt(calibration.calibrated_at) if calibration else "",
                state.status,
                ctx.clock.fmt(state.status_changed_at),
                local_day_text(device.decommissioned_at)
                if state.status == "decommissioned"
                else "",
            ]
            if v2:
                row.append(slot.power_source)
            row.append(ctx.clock.fmt(state.updated_at))
            yield from ctx.emit(
                DEVICES, row, device.device_id, defects.random(), defects.random()
            )


DEVICES = TableSpec(
    name="devices",
    source=OPS_SOURCE,
    change_type="snapshot",
    key=("device_id",),
    grain="One device as the registry records it at the end of the export date. A decommissioned device stays listed for 60 days.",
    description="Field devices. The calibration applies to the type's primary metric: smoke_ppm, temp_c, water_level_cm, or vwc_pct; calibrated = raw * gain + offset.",
    columns=(
        col("device_id", "string", "Device identifier: type prefix and serial."),
        col(
            "device_type",
            "string",
            "Closed set: smoke_heat, weather, river_gauge, soil_moisture, acoustic, camera_trap.",
            classification="public",
        ),
        col("model", "string", "Hardware model.", classification="public"),
        col("reserve_id", "string", "Reserve of the current zone."),
        col("zone_id", "string", "Current zone; changes when the device is relocated."),
        col("lat", "decimal(9,6)", "Installed latitude.", classification="public"),
        col("lon", "decimal(9,6)", "Installed longitude.", classification="public"),
        col("installed_on", "date", "Installation date."),
        col("firmware_version", "string", "Firmware version."),
        col(
            "calibration_offset",
            "decimal(10,4)",
            "Offset of the latest recorded calibration; empty for acoustic and camera_trap.",
            nullable=True,
        ),
        col(
            "calibration_gain",
            "decimal(6,4)",
            "Gain of the latest recorded calibration; empty for acoustic and camera_trap.",
            nullable=True,
        ),
        col(
            "calibrated_at",
            "timestamp",
            "When the latest recorded calibration was performed (business time). About 10% are entered one to five days later.",
            nullable=True,
        ),
        col("status", "string", "Closed set: active, maintenance, decommissioned."),
        col("status_changed_at", "timestamp", "When the status last changed."),
        col(
            "decommissioned_on",
            "date",
            "Decommission date; empty while in service.",
            nullable=True,
        ),
        col(
            "power_source",
            "string",
            "From schema version 2 (ODP_BIOGUARDIA_SCHEMA_V2_FROM): solar, battery, solar_battery, grid.",
        ),
        col(
            "updated_at",
            "timestamp",
            "Last change entered in the registry (system time).",
        ),
    ),
    rows=devices_rows,
    totals=("calibration_offset",),
    defect_rate=0.0005,
    defects=(
        Defect("orphan_fk", "zone_id", "ZON-999999"),
        Defect("invalid_value", "lat", "-123.456789"),
        Defect("duplicate_exact"),
    ),
    since_version={"power_source": "2"},
)


# -- species ----------------------------------------------------------------


def species_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    until = ctx.end - 1
    defects = ctx.defect_stream("species")
    for species in world.species:
        status, assessed = world.species_status(species, until)
        sensitive = species.poaching_target or status in {"EN", "CR"}
        row = [
            species.species_id,
            species.scientific_name,
            species.common_name,
            species.family,
            species.taxon_group,
            status,
            local_day_text(assessed),
            flag(species.endemic),
            flag(sensitive),
            flag(species.native_tree),
            dec(species.wood_density_milli, 3) if species.wood_density_milli else "",
            ctx.clock.fmt(max(assessed, local_midnight(WORLD_START))),
        ]
        yield from ctx.emit(
            SPECIES, row, species.species_id, defects.random(), defects.random()
        )


SPECIES = TableSpec(
    name="species",
    source=OPS_SOURCE,
    change_type="snapshot",
    key=("species_id",),
    grain="One species of the agency's reference list as of the end of the export date.",
    description="Generated species list. Names are not real taxa and statuses are not IUCN assessments.",
    columns=(
        col("species_id", "string", "Species identifier, `SPC-` and four digits."),
        col(
            "scientific_name",
            "string",
            "Generated binomial name.",
            classification="public",
        ),
        col(
            "common_name_pt",
            "string",
            "Generated Portuguese common name.",
            classification="public",
        ),
        col("family", "string", "Generated family name.", classification="public"),
        col(
            "taxon_group",
            "string",
            "Closed set: bird, mammal, amphibian, reptile, insect, tree, plant.",
            classification="public",
        ),
        col(
            "conservation_status",
            "string",
            "Closed set: LC, NT, VU, EN, CR, DD.",
            classification="public",
        ),
        col(
            "assessed_on",
            "date",
            "Date of the assessment that set the status.",
            classification="public",
        ),
        col(
            "endemic",
            "boolean",
            "Endemic to the Atlantic Forest.",
            classification="public",
        ),
        col(
            "sensitive_location",
            "boolean",
            "Observation coordinates must be generalized before publication (EN, CR, or a poaching target).",
            classification="public",
        ),
        col(
            "native_tree",
            "boolean",
            "Native tree used in restoration.",
            classification="public",
        ),
        col(
            "wood_density_g_cm3",
            "decimal(4,3)",
            "Wood density of a native tree; empty otherwise.",
            nullable=True,
            classification="public",
        ),
        col("updated_at", "timestamp", "Last change (system time)."),
    ),
    rows=species_rows,
    defect_rate=0.001,
    defects=(Defect("duplicate_conflict", "conservation_status", "XX"),),
)


# -- citizen reporters ------------------------------------------------------


def citizens_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    until = ctx.end - 1
    erased, withdrawn = world.requests_in_effect(ctx.day)
    defects = ctx.defect_stream("citizen_reporters")
    for index in range(world.citizens_registered(until)):
        citizen = world.citizen(index)
        person = citizen.person
        erasure = erased.get(index)
        withdrawal = withdrawn.get(index)
        consent = citizen.consent_research and withdrawal is None and erasure is None
        anonymized_at = (
            local_midnight(erasure.effective_on) + 3 * 3600 if erasure else None
        )
        consent_changed = (
            local_midnight(withdrawal.effective_on) + 3 * 3600 if withdrawal else None
        )
        updated = max(
            [citizen.registered_at]
            + [t for t in (anonymized_at, consent_changed) if t is not None]
        )
        hidden = erasure is not None
        row = [
            citizen.citizen_id,
            "" if hidden else person.full_name,
            "" if hidden or not citizen.has_cpf else person.cpf,
            "" if hidden or not citizen.has_email else person.email,
            "" if hidden or not citizen.has_phone else person.phone,
            "" if hidden else str(citizen.municipality_code),
            ctx.clock.fmt(citizen.registered_at),
            citizen.channel,
            flag(consent),
            ctx.clock.fmt(consent_changed) if consent_changed else "",
            ctx.clock.fmt(anonymized_at) if anonymized_at else "",
            ctx.clock.fmt(updated),
        ]
        yield from ctx.emit(
            CITIZENS, row, citizen.citizen_id, defects.random(), defects.random()
        )


CITIZENS = TableSpec(
    name="citizen_reporters",
    source=OPS_SOURCE,
    change_type="snapshot",
    key=("citizen_id",),
    grain="One registered citizen reporter as of the end of the export date. After an erasure takes effect the row stays with its identifiers emptied.",
    description="Community members registered to report fires, logging, and hunting.",
    columns=(
        col("citizen_id", "string", "Reporter identifier, `CIT-` and seven digits."),
        col(
            "full_name",
            "string",
            "Generated full name; empty after erasure.",
            nullable=True,
            classification="pii",
        ),
        col(
            "cpf",
            "string",
            "CPF-shaped value with a wrong check digit; optional.",
            nullable=True,
            classification="pii",
        ),
        col(
            "email",
            "string",
            "Address under a reserved `.example` domain; optional.",
            nullable=True,
            classification="pii",
        ),
        col(
            "phone",
            "string",
            "Mobile number with an unassigned area code; optional.",
            nullable=True,
            classification="pii",
        ),
        col(
            "municipality_code",
            "integer",
            "IBGE code of the home municipality.",
            nullable=True,
            classification="pii",
        ),
        col("registered_at", "timestamp", "Registration time."),
        col(
            "preferred_channel", "string", "Closed set: app, hotline, whatsapp, email."
        ),
        col(
            "consent_research", "boolean", "Consent to use reports in research outputs."
        ),
        col(
            "consent_changed_at",
            "timestamp",
            "When consent was withdrawn.",
            nullable=True,
        ),
        col(
            "anonymized_at",
            "timestamp",
            "When an erasure request took effect.",
            nullable=True,
        ),
        col("updated_at", "timestamp", "Last change (system time)."),
    ),
    rows=citizens_rows,
    defect_rate=0.0002,
    defects=(Defect("duplicate_exact"),),
)

MASTER_TABLES = (RESERVES, ZONES, STAFF, DEVICES, SPECIES, CITIZENS)
