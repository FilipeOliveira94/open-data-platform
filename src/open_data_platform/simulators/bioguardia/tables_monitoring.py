"""Monitoring tables: acoustic recordings and detections, camera traps, satellites."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import timedelta
from typing import Any

from open_data_platform.simulators.bioguardia import OPS_SOURCE
from open_data_platform.simulators.bioguardia.clock import DAY_SECONDS, local_midnight
from open_data_platform.simulators.bioguardia.context import DayContext
from open_data_platform.simulators.bioguardia.fleet import Device, DeviceSlot, _subtract
from open_data_platform.simulators.bioguardia.lifecycles import (
    deforestation_alerts_on,
    incidents_created_on,
)
from open_data_platform.simulators.bioguardia.rng import Rng, cumulative
from open_data_platform.simulators.bioguardia.spec import (
    Defect,
    Row,
    TableSpec,
    col,
    coord,
    dec,
    flag,
)
from open_data_platform.simulators.bioguardia.world import FIRE_SEASON

RECORDING_INTERVAL = 300
RECORDING_SECONDS = 60
SEGMENT_SECONDS = 3
CLASSIFIER_VERSION = "bgc-2.4.1"
DETECTION_BASE_RATE = 9.6
# Relative acoustic activity per local hour (dawn and dusk choruses), mean 1.
_HOUR_ACTIVITY_RAW = (
    0.9, 0.9, 0.9, 0.9, 0.9, 2.5, 2.5, 1.6, 1.6, 0.55, 0.55, 0.55,
    0.55, 0.55, 0.55, 0.55, 0.55, 1.8, 1.8, 1.2, 1.2, 1.2, 1.2, 0.9,
)  # fmt: skip
HOUR_ACTIVITY = tuple(v * 24 / sum(_HOUR_ACTIVITY_RAW) for v in _HOUR_ACTIVITY_RAW)
SOUND_CLASSES = (
    ("species", 0.88),
    ("rain", 0.03),
    ("wind", 0.03),
    ("stream", 0.02),
    ("vehicle", 0.02),
    ("human_voice", 0.01),
    ("aircraft", 0.007),
    ("chainsaw", 0.002),
    ("gunshot", 0.001),
)
_SOUND_LABELS = [name for name, _ in SOUND_CLASSES]
_SOUND_WEIGHTS = cumulative([w for _, w in SOUND_CLASSES])
# Camera triggers per local hour (nocturnal and crepuscular animals).
_CAMERA_HOURS = cumulative(
    (1.6, 1.6, 1.4, 1.2, 1.4, 2.2, 2.0, 1.0, 0.7, 0.6, 0.6, 0.6,
     0.6, 0.6, 0.6, 0.7, 0.9, 1.6, 2.2, 2.0, 1.8, 1.8, 1.7, 1.6)
)  # fmt: skip


def _active_windows(ctx: DayContext, device: Device) -> list[tuple[int, int]]:
    lo = max(ctx.start, device.installed_at)
    hi = min(ctx.end, device.decommissioned_at)
    if hi <= lo:
        return []
    holes = [(a, b) for a, b in ctx.fleet.maintenance(device, hi) if a < hi and b > lo]
    return _subtract((lo, hi), holes)


def _devices_today(
    ctx: DayContext, kind: str
) -> Iterator[tuple[DeviceSlot, Device, list[tuple[int, int]]]]:
    noon = ctx.start + 12 * 3600
    for slot in ctx.fleet.slots_by_type.get(kind, []):
        device = ctx.fleet.device_at(slot, noon)
        if device is None:
            continue
        windows = _active_windows(ctx, device)
        if windows:
            yield slot, device, windows


# -- acoustic recordings -----------------------------------------------------


def _recording_starts(
    ctx: DayContext, slot: DeviceSlot, windows: list[tuple[int, int]]
) -> list[int]:
    phase = slot.health_phase % RECORDING_INTERVAL
    starts = []
    for at in range(ctx.start + phase, ctx.end, RECORDING_INTERVAL):
        if any(lo <= at and at + RECORDING_SECONDS <= hi for lo, hi in windows):
            starts.append(at)
    return starts


def recordings_rows(ctx: DayContext) -> Iterator[Row]:
    fmt = ctx.clock.fmt
    day_text = ctx.day.strftime("%Y%m%d")
    defects = ctx.defect_stream("acoustic_recordings")
    rate = RECORDINGS.defect_rate if ctx.defects else -1.0
    for slot, device, windows in _devices_today(ctx, "acoustic"):
        rng = Rng(ctx.config.seed, "recordings", device.device_id, ctx.day.isoformat())
        sample_rate = 48000 if device.generation % 2 == 0 else 32000
        battery = 60 + rng.below(40)
        for at in _recording_starts(ctx, slot, windows):
            local = fmt(at)
            recording_id = f"{device.device_id}-{day_text}{local[11:13]}{local[14:16]}"
            size = int(
                sample_rate * 2 * RECORDING_SECONDS * (0.50 + 0.1 * rng.random())
            )
            battery = max(5, min(100, battery + rng.between(-2, 2)))
            row = [
                recording_id,
                device.device_id,
                local,
                str(RECORDING_SECONDS),
                str(sample_rate),
                "flac",
                str(size),
                f"s3://bioguardia-media/audio/{device.device_id}/{ctx.day.isoformat()}/{recording_id}.flac",
                str(battery),
                dec(150 + rng.below(250), 1),
            ]
            draw = defects.random()
            if draw < rate:
                yield from ctx.emit(
                    RECORDINGS, row, recording_id, draw, defects.random()
                )
            else:
                yield row


RECORDINGS = TableSpec(
    name="acoustic_recordings",
    source=OPS_SOURCE,
    change_type="append",
    key=("recording_id",),
    grain="One 60-second recording, every 5 minutes per active recorder. The audio file itself is not generated.",
    description="Metadata of passive acoustic recordings.",
    columns=(
        col(
            "recording_id", "string", "Recorder id and local start time (yyyymmddHHMM)."
        ),
        col("device_id", "string", "Acoustic recorder."),
        col("started_at", "timestamp", "Recording start."),
        col("duration_s", "integer", "Length in seconds."),
        col("sample_rate_hz", "integer", "48000 or 32000."),
        col("audio_format", "string", "Always flac."),
        col("file_bytes", "integer", "Size of the audio file."),
        col(
            "media_uri",
            "string",
            "Where the agency's media store would hold the file; not generated.",
        ),
        col("battery_pct", "integer", "Battery level at the start."),
        col("enclosure_temp_c", "decimal(4,1)", "Temperature inside the enclosure."),
    ),
    rows=recordings_rows,
    totals=("file_bytes",),
    time_column="started_at",
    defect_rate=0.00002,
    defects=(Defect("invalid_value", "duration_s", "-60"), Defect("duplicate_exact")),
)


# -- acoustic detections -----------------------------------------------------


def detections_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    fmt = ctx.clock.fmt
    day_text = ctx.day.strftime("%Y%m%d")
    defects = ctx.defect_stream("acoustic_detections")
    rate = DETECTIONS.defect_rate if ctx.defects else -1.0
    corrections = (
        ctx.correction_stream("acoustic_detections") if ctx.version == 2 else None
    )
    start = ctx.start
    for slot, device, windows in _devices_today(ctx, "acoustic"):
        rng = Rng(ctx.config.seed, "detections", device.device_id, ctx.day.isoformat())
        random = rng.random
        order, weights = world.ranked_species("vocal", world.zones[slot.zone].reserve)
        total = weights[-1]
        base = DETECTION_BASE_RATE * slot.detection_rate
        device_id = device.device_id
        for at in _recording_starts(ctx, slot, windows):
            local = fmt(at)
            recording_id = f"{device_id}-{day_text}{local[11:13]}{local[14:16]}"
            hour = (at - start) // 3600
            for n in range(rng.poisson(base * HOUR_ACTIVITY[hour])):
                segment = int(random() * (RECORDING_SECONDS // SEGMENT_SECONDS))
                sound = _SOUND_LABELS[_pick(_SOUND_WEIGHTS, random())]
                species = (
                    order[_pick(weights, random() * total)]
                    if sound == "species"
                    else -1
                )
                confidence = 100 + int(890 * random() ** 0.6)
                detection_id = f"{recording_id}-{n:03d}"
                if corrections is not None and corrections.random() < 0.001:
                    confidence = max(100, confidence - 100)
                    ctx.note_correction(
                        "acoustic_detections", detection_id, "confidence"
                    )
                row = [
                    detection_id,
                    recording_id,
                    device_id,
                    fmt(at + segment * SEGMENT_SECONDS),
                    sound,
                    world.species[species].species_id if species >= 0 else "",
                    str(segment * SEGMENT_SECONDS),
                    str(SEGMENT_SECONDS),
                    f"0.{confidence:03d}",
                    CLASSIFIER_VERSION,
                ]
                draw = defects.random()
                if draw < rate:
                    yield from ctx.emit(
                        DETECTIONS, row, detection_id, draw, defects.random()
                    )
                else:
                    yield row


def _pick(cumulative_weights: list[float], value: float) -> int:
    lo, hi = 0, len(cumulative_weights) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if cumulative_weights[mid] > value:
            hi = mid
        else:
            lo = mid + 1
    return lo


DETECTIONS = TableSpec(
    name="acoustic_detections",
    source=OPS_SOURCE,
    change_type="append",
    key=("detection_id",),
    grain="One sound event the on-device classifier found in a 3-second segment of a recording.",
    description="Classifier detections. Species frequency follows a Zipf law per reserve; `human_voice` marks recordings with people talking.",
    columns=(
        col("detection_id", "string", "Recording id and the detection's number in it."),
        col("recording_id", "string", "Recording the detection belongs to."),
        col("device_id", "string", "Acoustic recorder."),
        col("detected_at", "timestamp", "Start of the 3-second segment."),
        col(
            "sound_class",
            "string",
            "Closed set: species, rain, wind, stream, vehicle, human_voice, aircraft, chainsaw, gunshot.",
            classification="public",
        ),
        col(
            "species_id",
            "string",
            "Species, when sound_class is species.",
            nullable=True,
            classification="public",
        ),
        col("offset_s", "integer", "Segment offset inside the recording."),
        col("duration_s", "integer", "Segment length (3 s)."),
        col("confidence", "decimal(4,3)", "Classifier confidence, 0.100 to 0.990."),
        col("classifier_version", "string", "Version of the on-device classifier."),
    ),
    rows=detections_rows,
    totals=("confidence",),
    time_column="detected_at",
    defect_rate=0.00002,
    defects=(
        Defect("invalid_value", "confidence", "1.732"),
        Defect("orphan_fk", "species_id", "SPC-9999"),
        Defect("malformed_timestamp", "detected_at", "2026-13-45T25:61:00-03:00"),
        Defect("duplicate_exact"),
        Defect("duplicate_conflict", "confidence", "0.123"),
    ),
)


# -- camera traps --------------------------------------------------------------


def camera_rows(ctx: DayContext) -> Iterator[dict[str, Any]]:
    world = ctx.world
    fmt = ctx.clock.fmt
    defects = ctx.defect_stream("camera_trap_images")
    rate = CAMERA.defect_rate if ctx.defects else -1.0
    day_text = ctx.day.strftime("%Y%m%d")
    for slot, device, windows in _devices_today(ctx, "camera_trap"):
        rng = Rng(ctx.config.seed, "camera", device.device_id, ctx.day.isoformat())
        order, weights = world.ranked_species("camera", world.zones[slot.zone].reserve)
        instants = []
        for _ in range(rng.poisson(70 * slot.detection_rate)):
            hour = rng.weighted(_CAMERA_HOURS)
            at = ctx.start + hour * 3600 + rng.below(3600)
            if any(lo <= at < hi for lo, hi in windows):
                instants.append(at)
        instants.sort()
        for n, at in enumerate(instants):
            local = fmt(at)
            image_id = f"{device.device_id}-{day_text}{local[11:13]}{local[14:16]}{local[17:19]}-{n:03d}"
            draw = rng.random()
            detections: list[dict[str, Any]] = []
            blank = draw < 0.55
            human = 0.55 <= draw < 0.57
            if human:
                detections.append(
                    {
                        "label": "human",
                        "count": rng.between(1, 3),
                        "confidence": f"0.{rng.between(700, 990)}",
                        "bbox": _bbox(rng),
                    }
                )
            elif not blank:
                for _ in range(1 if rng.chance(0.8) else 2):
                    species = order[rng.weighted(weights)]
                    detections.append(
                        {
                            "label": "animal",
                            "species_id": world.species[species].species_id,
                            "count": rng.between(1, 3),
                            "confidence": f"0.{rng.between(500, 990)}",
                            "bbox": _bbox(rng),
                        }
                    )
            row = {
                "image_id": image_id,
                "device_id": device.device_id,
                "captured_at": local,
                "trigger": "timelapse" if rng.chance(0.05) else "motion",
                "is_blank": blank,
                "is_human": human,
                "detections": detections,
                "media_uri": f"s3://bioguardia-media/camera/{device.device_id}/{ctx.day.isoformat()}/{image_id}.jpg",
                "file_bytes": rng.between(300_000, 2_000_000),
            }
            pick = defects.random()
            if pick < rate:
                yield from ctx.emit(CAMERA, row, image_id, pick, defects.random())
            else:
                yield row


def _bbox(rng: Rng) -> list[str]:
    x, y = rng.uniform(0.0, 0.8), rng.uniform(0.0, 0.8)
    return [
        f"{x:.3f}",
        f"{y:.3f}",
        f"{rng.uniform(0.05, 1.0 - x):.3f}",
        f"{rng.uniform(0.05, 1.0 - y):.3f}",
    ]


CAMERA = TableSpec(
    name="camera_trap_images",
    source=OPS_SOURCE,
    change_type="append",
    key=("image_id",),
    grain="One camera-trap image with the classifier's detections. JSON Lines; the image itself is not generated.",
    description="Camera-trap image metadata. About 55% of images are blank and 2% show people, which are personal data and must not reach published outputs.",
    columns=(
        col("image_id", "string", "Camera id, local capture time, and number."),
        col("device_id", "string", "Camera trap."),
        col("captured_at", "timestamp", "Capture time."),
        col("trigger", "string", "Closed set: motion, timelapse."),
        col("is_blank", "boolean", "No animal or person found."),
        col("is_human", "boolean", "A person was found.", classification="pii"),
        col(
            "detections",
            "json",
            "Array of {label: animal or human, species_id (animals), count, confidence (decimal string), bbox [x, y, w, h] as decimal strings}.",
        ),
        col(
            "media_uri",
            "string",
            "Where the agency's media store would hold the image; not generated.",
        ),
        col("file_bytes", "integer", "Size of the image file."),
    ),
    rows=camera_rows,
    payload_format="jsonl",
    totals=("file_bytes",),
    time_column="captured_at",
    defect_rate=0.0001,
    defects=(Defect("null_required", "device_id"), Defect("duplicate_exact")),
)


# -- fire hotspots ---------------------------------------------------------------

OVERPASSES = (("SAT-A", 1, 30), ("SAT-B", 10, 30), ("SAT-A", 13, 30), ("SAT-B", 22, 30))


def hotspots_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    fleet = ctx.fleet
    rng = ctx.rng("hotspots")
    defects = ctx.defect_stream("fire_hotspots")
    corrections = ctx.correction_stream("fire_hotspots") if ctx.version == 2 else None
    fires = [
        f
        for d in (ctx.day - timedelta(days=1), ctx.day, ctx.day + timedelta(days=1))
        for f in fleet.fires(d)
    ]
    season = FIRE_SEASON[ctx.day.month - 1]
    background = rng.count(400 * ctx.config.scale * season)
    n = 0
    for index, (satellite, hour, minute) in enumerate(OVERPASSES):
        at = local_midnight(ctx.day) + hour * 3600 + minute * 60 + rng.below(600)
        spots: list[tuple[float, float, int | None, int, int]] = []
        for fire in fires:
            if fire.start <= at < fire.end:
                zone = world.zones[fire.zone]
                for _ in range(rng.between(1, 6)):
                    spots.append(
                        (
                            zone.lat + rng.gauss(0.0, 0.004),
                            zone.lon + rng.gauss(0.0, 0.004),
                            fire.zone,
                            int(fire.intensity * 4000),
                            2,
                        )
                    )
        share = background // len(OVERPASSES) + (
            1 if index < background % len(OVERPASSES) else 0
        )
        for _ in range(share):
            reserve = world.reserves[rng.below(len(world.reserves))]
            spots.append(
                (
                    reserve.lat + rng.uniform(-0.4, 0.4),
                    reserve.lon + rng.uniform(-0.4, 0.4),
                    None,
                    rng.between(300, 4000),
                    rng.below(3),
                )
            )
        for lat, lon, zone_index, frp, confidence in spots:
            n += 1
            hotspot_id = f"HS-{ctx.day:%Y%m%d}-{n:06d}"
            frp_cents = frp * 10 + rng.below(1000)
            if corrections is not None and corrections.random() < 0.01:
                frp_cents = int(frp_cents * 0.9)
                ctx.note_correction("fire_hotspots", hotspot_id, "frp_mw")
            zone = world.zones[zone_index] if zone_index is not None else None
            row = [
                hotspot_id,
                satellite,
                ctx.clock.fmt(at),
                coord(lat),
                coord(lon),
                dec(3000 + rng.below(1200), 1),
                dec(frp_cents, 2),
                ("low", "nominal", "high")[confidence],
                world.reserves[zone.reserve].reserve_id if zone else "",
                zone.zone_id if zone else "",
            ]
            yield from ctx.emit(
                HOTSPOTS, row, hotspot_id, defects.random(), defects.random()
            )


HOTSPOTS = TableSpec(
    name="fire_hotspots",
    source=OPS_SOURCE,
    change_type="append",
    key=("hotspot_id",),
    grain="One thermal anomaly pixel from a satellite overpass. Fires inside reserves appear at the overpasses while they burn; the rest are outside reserves.",
    description="Satellite fire detections delivered by the agency's satellite processing provider. Volume follows the dry season.",
    columns=(
        col("hotspot_id", "string", "Hotspot identifier: `HS-`, date, and number."),
        col(
            "satellite",
            "string",
            "Closed set: SAT-A, SAT-B (fictional sensors).",
            classification="public",
        ),
        col("detected_at", "timestamp", "Overpass time.", classification="public"),
        col("lat", "decimal(9,6)", "Pixel center latitude.", classification="public"),
        col("lon", "decimal(9,6)", "Pixel center longitude.", classification="public"),
        col(
            "brightness_k",
            "decimal(5,1)",
            "Brightness temperature in kelvin.",
            classification="public",
        ),
        col(
            "frp_mw",
            "decimal(9,2)",
            "Fire radiative power in megawatts.",
            classification="public",
        ),
        col(
            "confidence",
            "string",
            "Closed set: low, nominal, high.",
            classification="public",
        ),
        col(
            "reserve_id",
            "string",
            "Reserve, when the pixel is inside one.",
            nullable=True,
        ),
        col("zone_id", "string", "Zone, when the pixel is inside one.", nullable=True),
    ),
    rows=hotspots_rows,
    totals=("frp_mw",),
    time_column="detected_at",
    defect_rate=0.002,
    defects=(Defect("invalid_value", "lat", "-123.456789"), Defect("duplicate_exact")),
)


# -- deforestation alerts --------------------------------------------------------


def deforestation_rows(ctx: DayContext) -> Iterator[Row]:
    world = ctx.world
    last = ctx.end - 1
    defects = ctx.defect_stream("deforestation_alerts")
    linked: dict[str, str] = {}
    for incident in incidents_created_on(world, ctx.day):
        if incident.source_alert_id:
            linked[incident.source_alert_id] = incident.incident_id
    for offset in range(20, -1, -1):
        for alert in deforestation_alerts_on(world, ctx.day - timedelta(days=offset)):
            detected_today = offset == 0
            validated_today = ctx.start <= alert.validated_at < ctx.end
            if not (detected_today or validated_today):
                continue
            validated = alert.validated_at <= last
            status = (
                ("confirmed" if alert.confirmed else "false_positive")
                if validated
                else "pending"
            )
            zone = world.zones[alert.zone] if alert.zone is not None else None
            row = [
                alert.alert_id,
                alert.detected_on.isoformat(),
                alert.sensor,
                coord(alert.lat),
                coord(alert.lon),
                dec(alert.area_cents, 2),
                world.reserves[zone.reserve].reserve_id if zone else "",
                zone.zone_id if zone else "",
                f"0.{alert.confidence_e3:03d}",
                status,
                ctx.clock.fmt(alert.validated_at) if validated else "",
                linked.get(alert.alert_id, "") if validated else "",
                ctx.clock.fmt(alert.validated_at if validated else alert.detected_at),
            ]
            yield from ctx.emit(
                DEFORESTATION, row, alert.alert_id, defects.random(), defects.random()
            )


DEFORESTATION = TableSpec(
    name="deforestation_alerts",
    source=OPS_SOURCE,
    change_type="change",
    key=("alert_id",),
    grain="One satellite deforestation alert in its state at the end of the export date; listed when detected and when validated.",
    description="Optical and radar deforestation alerts and their field validation. A confirmed alert opens an incident.",
    columns=(
        col("alert_id", "string", "Alert identifier: `DFA-`, date, and number."),
        col("detected_on", "date", "Detection date.", classification="public"),
        col("sensor", "string", "Closed set: optical, radar.", classification="public"),
        col(
            "lat", "decimal(9,6)", "Polygon centroid latitude.", classification="public"
        ),
        col(
            "lon",
            "decimal(9,6)",
            "Polygon centroid longitude.",
            classification="public",
        ),
        col(
            "area_ha",
            "decimal(8,2)",
            "Cleared area in hectares.",
            classification="public",
        ),
        col("reserve_id", "string", "Reserve, when inside one.", nullable=True),
        col("zone_id", "string", "Zone, when inside one.", nullable=True),
        col(
            "confidence",
            "decimal(4,3)",
            "Detection confidence.",
            classification="public",
        ),
        col(
            "validation_status",
            "string",
            "Closed set: pending, confirmed, false_positive.",
        ),
        col("validated_at", "timestamp", "Field validation time.", nullable=True),
        col(
            "linked_incident_id",
            "string",
            "Incident opened by a confirmed alert.",
            nullable=True,
        ),
        col("updated_at", "timestamp", "Last change (system time)."),
    ),
    rows=deforestation_rows,
    totals=("area_ha",),
    defect_rate=0.003,
    defects=(
        Defect("invalid_value", "area_ha", "-12.00"),
        Defect("duplicate_conflict", "validation_status", "confirmed"),
    ),
)

MONITORING_TABLES = (RECORDINGS, DETECTIONS, CAMERA, HOTSPOTS, DEFORESTATION)
_ = DAY_SECONDS
_ = flag
