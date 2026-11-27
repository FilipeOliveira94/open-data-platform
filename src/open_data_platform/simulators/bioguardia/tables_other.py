"""Restoration, control, and finance tables."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date, timedelta
from typing import Any

from open_data_platform.simulators.bioguardia import FINANCE_SOURCE, OPS_SOURCE
from open_data_platform.simulators.bioguardia.clock import local_midnight
from open_data_platform.simulators.bioguardia.context import DayContext
from open_data_platform.simulators.bioguardia.lifecycles import (
    incidents_created_on,
    lots_planted_on,
    patrols_planned_for,
    reports_received_on,
    samples_collected_on,
)
from open_data_platform.simulators.bioguardia.spec import (
    Defect,
    Row,
    TableSpec,
    col,
    dec,
)
from open_data_platform.simulators.bioguardia.tables_master import local_day_text
from open_data_platform.simulators.bioguardia.tables_ops import (
    INCIDENT_LOOKBACK_DAYS,
    NOTICE_LOOKBACK_DAYS,
    REPORT_LOOKBACK_DAYS,
)

LOT_LOOKBACK_DAYS = 380
SOIL_LOOKBACK_DAYS = 75


# -- planting lots ---------------------------------------------------------------


def lots_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    last = ctx.end - 1
    defects = ctx.defect_stream("planting_lots")
    for offset in range(LOT_LOOKBACK_DAYS, -1, -1):
        for lot in lots_planted_on(world, ctx.day - timedelta(days=offset)):
            planted_today = offset == 0
            checks_today = [c for c in lot.checks if ctx.start <= c[0] < ctx.end]
            if not (planted_today or checks_today):
                continue
            done = {
                after: survival for when, after, survival in lot.checks if when <= last
            }
            last_check = max(
                (when for when, _, _ in lot.checks if when <= last), default=None
            )
            zone = world.zones[lot.zone]
            row = [
                lot.lot_id,
                zone.zone_id,
                world.reserves[zone.reserve].reserve_id,
                world.species[lot.species].species_id,
                lot.planted_on.isoformat(),
                str(lot.seedlings),
                dec(lot.area_cents, 2),
                str(lot.crew),
                dec(done[30], 2) if 30 in done else "",
                dec(done[90], 2) if 90 in done else "",
                dec(done[365], 2) if 365 in done else "",
                local_day_text(last_check) if last_check else "",
                ctx.clock.fmt(last_check or lot.planted_at),
            ]
            yield from ctx.emit(
                LOTS, row, lot.lot_id, defects.random(), defects.random()
            )


LOTS = TableSpec(
    name="planting_lots",
    source=OPS_SOURCE,
    change_type="change",
    key=("lot_id",),
    grain="One reforestation planting lot in its state at the end of the export date; listed when planted and after each survival check (30, 90, 365 days).",
    description="Restoration plantings with native trees and their survival checks.",
    columns=(
        col("lot_id", "string", "Lot identifier: `LOT-`, planting date, and number."),
        col("zone_id", "string", "Restoration zone."),
        col("reserve_id", "string", "Reserve."),
        col("species_id", "string", "Native tree species planted."),
        col("planted_on", "date", "Planting date."),
        col("seedlings_count", "integer", "Seedlings planted."),
        col("area_ha", "decimal(8,2)", "Planted area."),
        col("crew_size", "integer", "People in the planting crew."),
        col(
            "survival_pct_30d",
            "decimal(5,2)",
            "Survival at the 30-day check.",
            nullable=True,
        ),
        col(
            "survival_pct_90d",
            "decimal(5,2)",
            "Survival at the 90-day check.",
            nullable=True,
        ),
        col(
            "survival_pct_365d",
            "decimal(5,2)",
            "Survival at the 365-day check.",
            nullable=True,
        ),
        col("last_check_on", "date", "Date of the latest check.", nullable=True),
        col("updated_at", "timestamp", "Last change (system time)."),
    ),
    rows=lots_rows,
    totals=("seedlings_count", "area_ha"),
    defect_rate=0.003,
    defects=(
        Defect("invalid_value", "seedlings_count", "-250"),
        Defect("orphan_fk", "species_id", "SPC-9999"),
    ),
)


# -- soil lab results --------------------------------------------------------------


def soil_rows(ctx: DayContext) -> Iterator[dict[str, Any]]:
    world = ctx.world
    defects = ctx.defect_stream("soil_lab_results")
    for offset in range(SOIL_LOOKBACK_DAYS, -1, -1):
        for result in samples_collected_on(world, ctx.day - timedelta(days=offset)):
            if not ctx.start <= result.received_at < ctx.end:
                continue
            row = {
                "sample_id": result.sample_id,
                "result_version": result.version,
                "zone_id": world.zones[result.zone].zone_id,
                "lot_id": result.lot_id,
                "collected_on": result.collected_on.isoformat(),
                "received_at": ctx.clock.fmt(result.received_at),
                "lab_code": result.lab_code,
                "analytes": [
                    {
                        "analyte": name,
                        "value": dec(value, scale),
                        "unit": unit,
                        "method": method,
                    }
                    for name, unit, method, value, scale in result.analytes
                ],
                "is_correction": result.correction,
                "updated_at": ctx.clock.fmt(result.received_at),
            }
            yield from ctx.emit(
                SOIL,
                row,
                f"{result.sample_id}/{result.version}",
                defects.random(),
                defects.random(),
            )


SOIL = TableSpec(
    name="soil_lab_results",
    source=OPS_SOURCE,
    change_type="change",
    key=("sample_id", "result_version"),
    grain="One lab result version of a soil sample, delivered on the day the lab sends it: 5 to 30 days after collection; about 3% get a corrected version 10 to 40 days later.",
    description="Soil analyses for carbon accounting. JSON Lines with nested analytes; values are decimal strings.",
    columns=(
        col(
            "sample_id",
            "string",
            "Sample identifier: `SMP-`, collection date, and number.",
        ),
        col("result_version", "integer", "1 for the first result; 2 for a correction."),
        col("zone_id", "string", "Zone sampled."),
        col(
            "lot_id",
            "string",
            "Planting lot sampled, when the sample belongs to one.",
            nullable=True,
        ),
        col("collected_on", "date", "Collection date (business date)."),
        col(
            "received_at", "timestamp", "When the lab result arrived (late by design)."
        ),
        col("lab_code", "string", "Laboratory."),
        col(
            "analytes",
            "json",
            "Array of {analyte, value (decimal string), unit, method}: organic_carbon %, bulk_density g/cm3, ph_h2o, total_nitrogen %, available_p mg/kg, clay %.",
        ),
        col("is_correction", "boolean", "True for a re-issued result."),
        col("updated_at", "timestamp", "Last change (system time)."),
    ),
    rows=soil_rows,
    payload_format="jsonl",
    time_column="received_at",
    defect_rate=0.003,
    defects=(Defect("null_required", "zone_id"), Defect("duplicate_exact")),
)


# -- canopy surveys ------------------------------------------------------------


def canopy_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    rng = ctx.rng("canopy")
    defects = ctx.defect_stream("canopy_surveys")
    previous_month_end = ctx.day - timedelta(days=1)
    first = previous_month_end.replace(day=1)
    zones = world.zones_by_type.get("restoration", []) + world.zones_by_type.get(
        "core", []
    )
    for n in range(ctx.config.scaled(3000)):
        zone = world.zones[zones[rng.below(len(zones))]]
        flown = first + timedelta(days=rng.below(previous_month_end.day))
        p50 = rng.between(300, 2500)
        survey_id = f"CNP-{previous_month_end:%Y%m}-{n + 1:05d}"
        row = [
            survey_id,
            zone.zone_id,
            world.reserves[zone.reserve].reserve_id,
            flown.isoformat(),
            rng.choice(("LDR-M300", "LDR-L2")),
            dec(rng.between(80, 600), 1),
            dec(p50, 2),
            dec(p50 + rng.between(200, 1500), 2),
            dec(rng.between(3000, 9800), 2),
            dec(int(p50 * rng.uniform(5, 12)), 2),
        ]
        yield from ctx.emit(CANOPY, row, survey_id, defects.random(), defects.random())


CANOPY = TableSpec(
    name="canopy_surveys",
    source=OPS_SOURCE,
    change_type="append",
    key=("survey_id",),
    grain="One drone LiDAR survey of a zone flown in the previous month; delivered only in the drop of the first day of each month.",
    description="Canopy structure and biomass estimates from drone LiDAR. Absent from the drop on other days by design.",
    columns=(
        col(
            "survey_id", "string", "Survey identifier: `CNP-`, month flown, and number."
        ),
        col("zone_id", "string", "Zone surveyed."),
        col("reserve_id", "string", "Reserve."),
        col("flown_on", "date", "Flight date."),
        col("sensor", "string", "LiDAR sensor model."),
        col("point_density_ppm2", "decimal(6,1)", "Points per square meter."),
        col("canopy_height_p50_m", "decimal(5,2)", "Median canopy height."),
        col("canopy_height_p95_m", "decimal(5,2)", "95th percentile canopy height."),
        col("canopy_cover_pct", "decimal(5,2)", "Canopy cover."),
        col(
            "agb_mg_ha",
            "decimal(7,2)",
            "Above-ground biomass estimate, Mg per hectare.",
        ),
    ),
    rows=canopy_rows,
    totals=("agb_mg_ha",),
    defect_rate=0.002,
    defects=(Defect("invalid_value", "canopy_cover_pct", "130.00"),),
    present=lambda ctx: ctx.day.day == 1,
)


# -- device daily counters -------------------------------------------------------


def counters_rows(ctx: DayContext) -> Iterator[Row]:
    plan = ctx.fleet.plan(ctx.day)
    buffer = ctx.config.buffer_hours * 3600
    defects = ctx.defect_stream("device_daily_counters")
    for device_day in plan.devices:
        readings, health, alerts, dropped, outage = plan.counters(device_day, buffer)
        row = [
            device_day.device.device_id,
            ctx.day.isoformat(),
            str(readings),
            str(health),
            str(alerts),
            str(dropped),
            str(outage),
            device_day.firmware,
        ]
        key = f"{device_day.device.device_id}/{ctx.day.isoformat()}"
        yield from ctx.emit(COUNTERS, row, key, defects.random(), defects.random())


COUNTERS = TableSpec(
    name="device_daily_counters",
    source=OPS_SOURCE,
    change_type="append",
    key=("device_id", "utc_date"),
    grain="One device and UTC day: the messages it generated per stream and the ones it lost when its 12-hour buffer overflowed during an outage. The drop of local date D carries UTC day D.",
    description="The devices' own counters, read during the nightly sync. The telemetry stream should carry generated minus dropped events per device and UTC day (reconciliation R2).",
    columns=(
        col("device_id", "string", "Device."),
        col("utc_date", "date", "UTC day counted."),
        col("readings_generated", "integer", "Readings generated (topic readings)."),
        col("health_generated", "integer", "Health messages generated (topic health)."),
        col("alerts_generated", "integer", "Alerts generated (topic alerts)."),
        col(
            "messages_dropped",
            "integer",
            "Messages lost on the device: generated during an outage longer than the buffer.",
        ),
        col(
            "outage_seconds", "integer", "Seconds without connectivity during the day."
        ),
        col("firmware_version", "string", "Firmware at the start of the day."),
    ),
    rows=counters_rows,
    totals=(
        "readings_generated",
        "health_generated",
        "alerts_generated",
        "messages_dropped",
    ),
    defect_rate=0.0,
)


# -- erasure requests ------------------------------------------------------------


def erasure_rows(ctx: DayContext) -> Iterator[Row]:
    for request in ctx.world.erasures_on(ctx.day):
        yield [
            request.request_id,
            ctx.world.citizen(request.citizen_index).citizen_id,
            request.request_type,
            ctx.clock.fmt(request.requested_at),
            request.effective_on.isoformat(),
            "received",
        ]


ERASURES = TableSpec(
    name="erasure_requests",
    source=OPS_SOURCE,
    change_type="append",
    key=("request_id",),
    grain="One request from a citizen reporter to erase their personal data or withdraw consent, received on the export date.",
    description="Requests modeled after LGPD concepts (not legal guidance). From effective_on, the reporter's row in citizen_reporters is anonymized or loses consent; downstream copies must follow.",
    columns=(
        col("request_id", "string", "Request identifier: `ERQ-`, date, and number."),
        col("citizen_id", "string", "Reporter who made the request."),
        col("request_type", "string", "Closed set: erasure, consent_withdrawal."),
        col("requested_at", "timestamp", "When the request arrived."),
        col("effective_on", "date", "Date the source applies it (0 to 15 days later)."),
        col("status", "string", "Always received in this table."),
    ),
    rows=erasure_rows,
)


# -- deleted records -------------------------------------------------------------


def deleted_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    found: list[tuple[int, str, str]] = []
    for offset in range(-1, 4):
        for patrol in patrols_planned_for(world, ctx.day + timedelta(days=offset)):
            if (
                patrol.cancelled_at is not None
                and ctx.start <= patrol.cancelled_at < ctx.end
            ):
                found.append((patrol.cancelled_at, "patrols", patrol.patrol_id))
    for offset in range(INCIDENT_LOOKBACK_DAYS, -1, -1):
        for incident in incidents_created_on(world, ctx.day - timedelta(days=offset)):
            if (
                incident.deleted_at is not None
                and ctx.start <= incident.deleted_at < ctx.end
            ):
                found.append((incident.deleted_at, "incidents", incident.incident_id))
    for offset in range(REPORT_LOOKBACK_DAYS + 3, -1, -1):
        for report in reports_received_on(world, ctx.day - timedelta(days=offset)):
            if (
                report.deleted_at is not None
                and ctx.start <= report.deleted_at < ctx.end
            ):
                found.append((report.deleted_at, "citizen_reports", report.report_id))
    for when, table, record in sorted(found):
        yield [table, record, ctx.clock.fmt(when)]


DELETED = TableSpec(
    name="deleted_records",
    source=OPS_SOURCE,
    change_type="append",
    key=("table_name", "record_id"),
    grain="One hard delete in a change table on the export date.",
    description="Cancelled patrol plans, merged duplicate incidents, and spam reports. A deleted record does not appear in later drops.",
    columns=(
        col("table_name", "string", "Closed set: patrols, incidents, citizen_reports."),
        col("record_id", "string", "Primary key of the deleted record."),
        col("deleted_at", "timestamp", "When it was deleted."),
    ),
    rows=deleted_rows,
)


# -- finance: fine payments --------------------------------------------------------


def payments_rows(ctx: DayContext) -> Iterator[Row]:
    defects = ctx.defect_stream("fine_payments")
    for offset in range(NOTICE_LOOKBACK_DAYS, -1, -1):
        for incident in incidents_created_on(
            ctx.world, ctx.day - timedelta(days=offset)
        ):
            notice = incident.notice
            if notice is None:
                continue
            for payment in notice.payments:
                if payment.posted_on != ctx.day:
                    continue
                row = [
                    payment.payment_id,
                    payment.notice_ref,
                    payment.payer_document,
                    payment.paid_on.isoformat(),
                    payment.posted_on.isoformat(),
                    dec(payment.amount_cents, 2),
                    payment.method,
                    payment.bank_reference,
                    "true" if payment.is_refund else "false",
                ]
                yield from ctx.emit(
                    PAYMENTS,
                    row,
                    payment.payment_id,
                    defects.random(),
                    defects.random(),
                )


PAYMENTS = TableSpec(
    name="fine_payments",
    source=FINANCE_SOURCE,
    change_type="append",
    key=("payment_id",),
    grain="One payment or refund the finance system posted on the export date. A payment is posted 0 to 3 days after it is paid.",
    description="Payments of infraction notices from the finance system, in BRL. Cases: full, installments, partial, overpaid, duplicate, unmatched reference, paid then cancelled (refund).",
    columns=(
        col("payment_id", "string", "Payment identifier."),
        col(
            "notice_id",
            "string",
            "Notice the payer referenced; about 3% carry a wrong digit and match no notice.",
        ),
        col(
            "payer_document",
            "string",
            "CPF or CNPJ of the payer, wrong check digit by construction.",
            classification="pii",
        ),
        col("paid_on", "date", "Date the payer paid (business date)."),
        col("posted_on", "date", "Date finance posted it; decides the drop."),
        col(
            "amount_brl",
            "decimal(14,2)",
            "Amount in BRL; a refund is positive with is_refund true.",
        ),
        col("payment_method", "string", "Closed set: boleto, pix, darf, transfer."),
        col("bank_reference", "string", "Bank transaction reference."),
        col("is_refund", "boolean", "True when the agency returned the money."),
    ),
    rows=payments_rows,
    totals=("amount_brl",),
    defect_rate=0.004,
    defects=(Defect("duplicate_exact"),),
)

OTHER_OPS_TABLES = (LOTS, SOIL, CANOPY, COUNTERS, ERASURES, DELETED)
FINANCE_TABLES = (PAYMENTS,)
_ = (json, date, local_midnight)
