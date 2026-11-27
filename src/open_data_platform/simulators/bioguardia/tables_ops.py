"""Field operations: patrols, track points, incidents, citizen reports, notices."""

from __future__ import annotations

import math
from collections.abc import Iterator
from datetime import timedelta

from open_data_platform.simulators.bioguardia import OPS_SOURCE
from open_data_platform.simulators.bioguardia.context import DayContext
from open_data_platform.simulators.bioguardia.lifecycles import (
    SPEED_KMH,
    Incident,
    Patrol,
    incidents_created_on,
    patrols_planned_for,
    reports_received_on,
)
from open_data_platform.simulators.bioguardia.rng import Rng
from open_data_platform.simulators.bioguardia.spec import (
    Defect,
    Row,
    TableSpec,
    col,
    coord,
    dec,
)
from open_data_platform.simulators.bioguardia.tables_master import local_day_text

INCIDENT_LOOKBACK_DAYS = 85
NOTICE_LOOKBACK_DAYS = 330
REPORT_LOOKBACK_DAYS = 4
POINT_INTERVAL = 10


def _patrols_touching(ctx: DayContext) -> list[Patrol]:
    """Patrols with an event (created, started, ended, cancelled) on the export date."""
    key = ("patrols", ctx.day)
    if key not in ctx.cache:
        found = []
        for offset in range(-1, 4):
            for patrol in patrols_planned_for(
                ctx.world, ctx.day + timedelta(days=offset)
            ):
                events = (
                    patrol.created_at,
                    patrol.started_at,
                    patrol.ended_at,
                    patrol.cancelled_at,
                )
                if any(t is not None and ctx.start <= t < ctx.end for t in events):
                    found.append(patrol)
        ctx.cache[key] = found
    return ctx.cache[key]


def walk(
    ctx: DayContext, patrol: Patrol
) -> Iterator[tuple[int, float, float, int, int]]:
    """Track points of a patrol: (instant, lat, lon, accuracy dm, speed in 0.1 km/h)."""
    zone = ctx.world.zones[patrol.zone]
    rng = Rng(ctx.config.seed, "walk", patrol.patrol_id)
    lat = zone.lat + rng.gauss(0.0, 0.004)
    lon = zone.lon + rng.gauss(0.0, 0.004)
    heading = rng.uniform(0.0, 2 * math.pi)
    speed = SPEED_KMH[patrol.transport]
    cos_lat = math.cos(math.radians(lat)) or 1.0
    for at in range(patrol.started_at, patrol.ended_at, POINT_INTERVAL):
        heading += rng.gauss(0.0, 0.35)
        velocity = speed * (0.4 + 1.2 * rng.random())
        step = velocity * POINT_INTERVAL / 3600 / 111.0
        lat += step * math.cos(heading)
        lon += step * math.sin(heading) / cos_lat
        yield at, lat, lon, 30 + int(rng.random() * 220), int(velocity * 10)


# -- patrol track points -------------------------------------------------------


def track_points_rows(ctx: DayContext) -> Iterator[Row]:
    distances: dict[str, int] = {}
    ctx.cache["patrol_distance"] = distances
    defects = ctx.defect_stream("patrol_track_points")
    rate = TRACK_POINTS.defect_rate if ctx.defects else -1.0
    fmt = ctx.clock.fmt
    for patrol in _patrols_touching(ctx):
        if (
            patrol.cancelled_at is not None
            or not ctx.start <= patrol.ended_at < ctx.end
        ):
            continue
        total_m = 0.0
        previous: tuple[float, float] | None = None
        for seq, (at, lat, lon, accuracy, speed) in enumerate(walk(ctx, patrol)):
            if previous is not None:
                total_m += speed / 10 * POINT_INTERVAL / 3.6
            previous = (lat, lon)
            row = [
                patrol.patrol_id,
                str(seq),
                fmt(at),
                patrol.leader_staff_id,
                f"{lat:.6f}",
                f"{lon:.6f}",
                dec(accuracy, 1),
                dec(speed, 1),
            ]
            draw = defects.random()
            if draw < rate:
                yield from ctx.emit(
                    TRACK_POINTS,
                    row,
                    f"{patrol.patrol_id}/{seq}",
                    draw,
                    defects.random(),
                )
            else:
                yield row
        distances[patrol.patrol_id] = int(total_m / 10)  # hundredths of a km


TRACK_POINTS = TableSpec(
    name="patrol_track_points",
    source=OPS_SOURCE,
    change_type="append",
    key=("patrol_id", "seq"),
    grain="One GPS fix of a patrol leader's handheld, every 10 seconds. Delivered in the drop of the day the patrol ended.",
    description="Patrol tracks. A sequence of positions of an identifiable person is personal data.",
    columns=(
        col("patrol_id", "string", "Patrol of the track."),
        col("seq", "integer", "Position of the fix in the track, from 0."),
        col("recorded_at", "timestamp", "Fix time, local time with offset."),
        col("staff_id", "string", "Staff id of the leader carrying the device."),
        col("lat", "decimal(9,6)", "Latitude.", classification="pii"),
        col("lon", "decimal(9,6)", "Longitude.", classification="pii"),
        col("accuracy_m", "decimal(5,1)", "Horizontal accuracy in meters."),
        col("speed_kmh", "decimal(5,1)", "Ground speed in km/h."),
    ),
    rows=track_points_rows,
    totals=("speed_kmh",),
    time_column="recorded_at",
    defect_rate=0.00002,
    defects=(
        Defect("invalid_value", "lat", "-123.456789"),
        Defect("malformed_timestamp", "recorded_at", "2026-13-45T25:61:00-03:00"),
        Defect("duplicate_exact"),
    ),
)


# -- patrols -----------------------------------------------------------------


def patrols_rows(ctx: DayContext) -> Iterator[Row]:
    distances: dict[str, int] = ctx.cache.get("patrol_distance", {})
    defects = ctx.defect_stream("patrols")
    last = ctx.end - 1
    for patrol in _patrols_touching(ctx):
        if patrol.cancelled_at is not None and patrol.cancelled_at <= last:
            continue
        started = patrol.started_at <= last
        ended = patrol.ended_at <= last
        status = (
            ("aborted" if patrol.aborted else "completed")
            if ended
            else ("in_progress" if started else "planned")
        )
        updated = max(
            t
            for t in (patrol.created_at, patrol.started_at, patrol.ended_at)
            if t <= last
        )
        distance = ""
        if ended:
            if patrol.patrol_id not in distances:
                speeds = [speed for *_, speed in walk(ctx, patrol)]
                # Same rule as the track: every step after the first fix.
                total_m = sum(v / 10 * POINT_INTERVAL / 3.6 for v in speeds[1:])
                distances[patrol.patrol_id] = int(total_m / 10)
            distance = dec(distances[patrol.patrol_id], 2)
        zone = ctx.world.zones[patrol.zone]
        row = [
            patrol.patrol_id,
            ctx.world.reserves[patrol.reserve].reserve_id,
            zone.zone_id,
            patrol.team_code,
            patrol.leader_staff_id,
            str(patrol.member_count),
            patrol.transport,
            ctx.clock.fmt(patrol.planned_start),
            ctx.clock.fmt(patrol.started_at) if started else "",
            ctx.clock.fmt(patrol.ended_at) if ended else "",
            distance,
            status,
            ctx.clock.fmt(updated),
        ]
        yield from ctx.emit(
            PATROLS, row, patrol.patrol_id, defects.random(), defects.random()
        )


PATROLS = TableSpec(
    name="patrols",
    source=OPS_SOURCE,
    change_type="change",
    key=("patrol_id",),
    grain="One ranger patrol in its state at the end of the export date; listed on each date it is created, starts, or ends. A cancelled plan is deleted (see deleted_records).",
    description="Ranger patrols.",
    columns=(
        col(
            "patrol_id",
            "string",
            "Patrol identifier: `PTR-`, planned date, and number.",
        ),
        col("reserve_id", "string", "Reserve patrolled."),
        col("zone_id", "string", "Zone where the patrol starts."),
        col("team_code", "string", "Team on duty."),
        col("leader_staff_id", "string", "Staff id of the patrol leader."),
        col("member_count", "integer", "People in the patrol."),
        col(
            "transport_mode",
            "string",
            "Closed set: foot, vehicle, boat, drone_support.",
        ),
        col("planned_start", "timestamp", "Planned start."),
        col(
            "started_at",
            "timestamp",
            "Actual start; empty while planned.",
            nullable=True,
        ),
        col("ended_at", "timestamp", "Actual end; empty until it ends.", nullable=True),
        col(
            "distance_km",
            "decimal(7,2)",
            "Distance covered, from the track; empty until it ends.",
            nullable=True,
        ),
        col(
            "status", "string", "Closed set: planned, in_progress, completed, aborted."
        ),
        col("updated_at", "timestamp", "Last change (system time)."),
    ),
    rows=patrols_rows,
    totals=("distance_km",),
    defect_rate=0.002,
    defects=(
        Defect("invalid_value", "distance_km", "-3.50"),
        Defect("duplicate_conflict", "status", "completed"),
    ),
)


# -- incidents ---------------------------------------------------------------


def _incidents_window(ctx: DayContext, back: int) -> Iterator[Incident]:
    for offset in range(back, -1, -1):
        yield from incidents_created_on(ctx.world, ctx.day - timedelta(days=offset))


def incidents_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    last = ctx.end - 1
    defects = ctx.defect_stream("incidents")
    corrections = ctx.correction_stream("incidents") if ctx.version == 2 else None
    for incident in _incidents_window(ctx, INCIDENT_LOOKBACK_DAYS):
        if incident.deleted_at is not None and incident.deleted_at <= last:
            continue
        if not any(ctx.start <= t < ctx.end for t, _ in incident.transitions):
            continue
        status_at, status = next(
            (t, s) for t, s in reversed(incident.transitions) if t <= last
        )
        severity = incident.severity
        if corrections is not None and corrections.random() < 0.01:
            severity = 1 + severity % 5
            ctx.note_correction("incidents", incident.incident_id, "severity")
        if incident.notes_pii:
            ctx.note_pii("incidents", incident.incident_id, "notes", incident.notes_pii)
        zone = world.zones[incident.zone]
        row = [
            incident.incident_id,
            world.reserves[incident.reserve].reserve_id,
            zone.zone_id,
            incident.incident_type,
            str(severity),
            ctx.clock.fmt(incident.reported_at),
            incident.reported_by,
            incident.reporter_staff_id or "",
            world.citizen(incident.reporter_citizen).citizen_id
            if incident.reporter_citizen is not None
            else "",
            incident.source_device_id or "",
            incident.patrol_id or "",
            coord(incident.lat),
            coord(incident.lon),
            dec(incident.area_cents, 2) if incident.area_cents is not None else "",
            status,
            ctx.clock.fmt(status_at),
            incident.notes,
            ctx.clock.fmt(status_at),
        ]
        yield from ctx.emit(
            INCIDENTS, row, incident.incident_id, defects.random(), defects.random()
        )


INCIDENTS = TableSpec(
    name="incidents",
    source=OPS_SOURCE,
    change_type="change",
    key=("incident_id",),
    grain="One field incident in its state at the end of the export date; listed on each date its status changes. A merged duplicate is deleted (see deleted_records).",
    description="Incidents reported by patrols, staff, citizens, acoustic sensors, and satellite alerts. Notes are free text and can hold personal data.",
    columns=(
        col(
            "incident_id",
            "string",
            "Incident identifier: `INC-`, report date, and number.",
        ),
        col("reserve_id", "string", "Reserve."),
        col("zone_id", "string", "Zone."),
        col(
            "incident_type",
            "string",
            "Closed set: illegal_logging, poaching, encroachment, illegal_mining, trash_dumping, invasive_species, road_kill, trap_found, unauthorized_fire, wildfire.",
        ),
        col("severity", "integer", "1 (minor) to 5 (critical)."),
        col("reported_at", "timestamp", "When the incident was reported."),
        col(
            "reported_by_type",
            "string",
            "Closed set: patrol, staff, citizen, sensor_alert, satellite_alert.",
        ),
        col(
            "reporter_staff_id",
            "string",
            "Staff who reported it, for patrol and staff reports.",
            nullable=True,
        ),
        col(
            "reporter_citizen_id",
            "string",
            "Citizen who reported it, for citizen reports.",
            nullable=True,
        ),
        col(
            "source_device_id",
            "string",
            "Acoustic recorder whose alert raised it, for sensor alerts.",
            nullable=True,
        ),
        col(
            "patrol_id",
            "string",
            "Patrol that found it, for patrol reports.",
            nullable=True,
        ),
        col("lat", "decimal(9,6)", "Latitude.", classification="public"),
        col("lon", "decimal(9,6)", "Longitude.", classification="public"),
        col(
            "estimated_area_ha",
            "decimal(10,2)",
            "Affected area, when it applies.",
            nullable=True,
            classification="public",
        ),
        col(
            "status",
            "string",
            "Closed set: reported, verified, under_investigation, referred_to_police, closed, dismissed.",
        ),
        col("status_changed_at", "timestamp", "When the current status was set."),
        col(
            "notes",
            "string",
            "Free text in Portuguese; 3 to 5% of rows carry a planted name, CPF, phone, or email.",
            classification="pii",
        ),
        col("updated_at", "timestamp", "Last change (system time)."),
    ),
    rows=incidents_rows,
    totals=("estimated_area_ha",),
    time_column="reported_at",
    defect_rate=0.002,
    defects=(
        Defect("orphan_fk", "zone_id", "ZON-999999"),
        Defect("null_required", "incident_type"),
        Defect("duplicate_exact"),
    ),
)


# -- citizen reports -----------------------------------------------------------


def reports_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    last = ctx.end - 1
    defects = ctx.defect_stream("citizen_reports")
    for offset in range(REPORT_LOOKBACK_DAYS, -1, -1):
        for report in reports_received_on(world, ctx.day - timedelta(days=offset)):
            if report.deleted_at is not None and report.deleted_at <= last:
                continue
            if not (
                ctx.start <= report.received_at < ctx.end
                or ctx.start <= report.triaged_at < ctx.end
            ):
                continue
            triaged = report.triaged_at <= last
            if report.description_pii:
                ctx.note_pii(
                    "citizen_reports",
                    report.report_id,
                    "description",
                    report.description_pii,
                )
            row = [
                report.report_id,
                world.citizen(report.citizen).citizen_id
                if report.citizen is not None
                else "",
                ctx.clock.fmt(report.received_at),
                report.channel,
                report.category,
                coord(report.lat),
                coord(report.lon),
                report.description,
                (report.linked_incident_id or "") if triaged else "",
                report.triage_status if triaged else "received",
                ctx.clock.fmt(report.triaged_at if triaged else report.received_at),
            ]
            yield from ctx.emit(
                REPORTS, row, report.report_id, defects.random(), defects.random()
            )


REPORTS = TableSpec(
    name="citizen_reports",
    source=OPS_SOURCE,
    change_type="change",
    key=("report_id",),
    grain="One report from the community app or hotline in its state at the end of the export date. Spam is deleted after triage (see deleted_records).",
    description="Community reports. The description and the reporter's coordinates can identify a person.",
    columns=(
        col("report_id", "string", "Report identifier: `RPT-`, date, and number."),
        col(
            "citizen_id",
            "string",
            "Reporter; empty for anonymous reports.",
            nullable=True,
        ),
        col("received_at", "timestamp", "When the report arrived."),
        col("channel", "string", "Closed set: app, hotline, whatsapp, email."),
        col(
            "category",
            "string",
            "Closed set: fire_smoke, logging, hunting, animal_sighting, trash, other.",
        ),
        col("lat", "decimal(9,6)", "Latitude the reporter gave.", classification="pii"),
        col(
            "lon", "decimal(9,6)", "Longitude the reporter gave.", classification="pii"
        ),
        col(
            "description",
            "string",
            "Free text in Portuguese; 3 to 5% carry a planted name, CPF, phone, or email.",
            classification="pii",
        ),
        col(
            "linked_incident_id",
            "string",
            "Incident opened from the report, set at triage.",
            nullable=True,
        ),
        col(
            "triage_status",
            "string",
            "Closed set: received, triaged, linked, dismissed, spam.",
        ),
        col("updated_at", "timestamp", "Last change (system time)."),
    ),
    rows=reports_rows,
    defect_rate=0.002,
    defects=(Defect("invalid_value", "lat", "91.000000"), Defect("duplicate_exact")),
)


# -- infraction notices --------------------------------------------------------


def notices_rows(ctx: DayContext) -> Iterator[Row]:
    last = ctx.end - 1
    defects = ctx.defect_stream("infraction_notices")
    for incident in _incidents_window(ctx, NOTICE_LOOKBACK_DAYS):
        notice = incident.notice
        if notice is None or not any(
            ctx.start <= t < ctx.end for t, _, _ in notice.transitions
        ):
            continue
        status_at, status, amount = next(
            item for item in reversed(notice.transitions) if item[0] <= last
        )
        row = [
            notice.notice_id,
            notice.incident_id,
            notice.offender_type,
            notice.offender_name,
            notice.offender_document,
            notice.infraction_type,
            local_day_text(notice.issued_at),
            notice.due_on.isoformat(),
            dec(amount, 2),
            status,
            ctx.clock.fmt(status_at),
            ctx.clock.fmt(status_at),
        ]
        yield from ctx.emit(
            NOTICES, row, notice.notice_id, defects.random(), defects.random()
        )


NOTICES = TableSpec(
    name="infraction_notices",
    source=OPS_SOURCE,
    change_type="change",
    key=("notice_id",),
    grain="One environmental infraction notice in its state at the end of the export date; listed on each date its status changes.",
    description="Fines issued after an investigation. Individual offenders are identified by name and CPF; companies by name and CNPJ. Amounts are BRL.",
    columns=(
        col(
            "notice_id",
            "string",
            "Notice identifier: `AIN-`, report date, and number of the incident.",
        ),
        col("incident_id", "string", "Incident that led to the notice."),
        col("offender_type", "string", "Closed set: individual, company."),
        col(
            "offender_name",
            "string",
            "Generated person or company name.",
            classification="pii",
        ),
        col(
            "offender_document",
            "string",
            "CPF (individual) or CNPJ (company), wrong check digit by construction.",
            classification="pii",
        ),
        col("infraction_type", "string", "Incident type the fine applies to."),
        col("issued_on", "date", "Issue date."),
        col("due_on", "date", "Payment due date; reset after an appeal decision."),
        col("fine_amount_brl", "decimal(14,2)", "Fine in BRL after any reduction."),
        col(
            "status",
            "string",
            "Closed set: issued, appealed, reduced, upheld, paid, overdue, cancelled.",
        ),
        col("status_changed_at", "timestamp", "When the current status was set."),
        col("updated_at", "timestamp", "Last change (system time)."),
    ),
    rows=notices_rows,
    totals=("fine_amount_brl",),
    defect_rate=0.003,
    defects=(
        Defect("invalid_value", "fine_amount_brl", "-500.00"),
        Defect("duplicate_conflict", "status", "paid"),
    ),
)

OPS_TABLES = (TRACK_POINTS, PATROLS, INCIDENTS, REPORTS, NOTICES)
