"""The telemetry gateway: publishes the fleet's events to Kafka on the wall clock.

The producer replays the world model (``fleet.DayPlan``): what each device
emits, when its connectivity outages end and it uploads its buffer, and which
events the gateway retransmits. It runs outside Airflow. It records the last
second whose messages Kafka acknowledged in a checkpoint file; after a stop it
replays from there. Events older than the devices' buffer are skipped, the way
a device loses them, and one record on the gateway topic says what was
replayed and what was skipped (counts only).

Logs carry counts and times, never payloads (AGENTS.md section 8).
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from open_data_platform.simulators.bioguardia.clock import (
    DAY_SECONDS,
    format_utc_ms,
    utc_date_of,
)
from open_data_platform.simulators.bioguardia.fleet import (
    ALERTS,
    HEALTH,
    READINGS,
    DayPlan,
    DeviceDay,
    Fleet,
    _key,
    _normal,
    fleet_for,
)
from open_data_platform.simulators.bioguardia.rng import unit
from open_data_platform.simulators.bioguardia.world import WorldConfig, build_world

SCHEMA_VERSION = 1
TOPICS = {
    READINGS: "bioguardia.telemetry.readings.v1",
    HEALTH: "bioguardia.telemetry.health.v1",
    ALERTS: "bioguardia.telemetry.alerts.v1",
}
GATEWAY_TOPIC = "bioguardia.telemetry.gateway.v1"
STREAM_NAMES = {READINGS: "readings", HEALTH: "health", ALERTS: "alerts"}
DUPLICATE_RATE = 0.001
CHECKPOINT_EVERY = 5
RESTART_GAP = 120  # seconds behind the wall clock that count as a gateway outage
MAX_REPLAY_GAP = 7 * 86400  # longer stops are recorded without per-device counts
MAX_REPLAY_RATE = 20_000  # events per second while catching up
DETAIL_CHUNK = 5_000


@dataclass(frozen=True, slots=True)
class Delivery:
    """One message the gateway sends: an event, or a retransmission of it."""

    at: int  # delivery second
    stream: str
    device_day: DeviceDay
    emitted: int  # second the device produced it
    seq: int
    day: date  # UTC day of the emission (part of the event id)
    duplicate: bool

    @property
    def event_id(self) -> str:
        return event_id(
            self.device_day.device.device_id, self.day, self.stream, self.seq
        )


def event_id(device_id: str, day: date, stream: str, seq: int) -> str:
    return f"{device_id}-{day:%Y%m%d}-{stream}{seq:06d}"


# -- schedule ---------------------------------------------------------------


class Schedule:
    """Deliveries of the fleet, by window, across UTC days."""

    def __init__(self, fleet: Fleet) -> None:
        self.fleet = fleet
        self.buffer = fleet.config.buffer_hours * 3600
        self._index: dict[date, dict[str, DeviceDay]] = {}

    def plan(self, day: date) -> DayPlan:
        return self.fleet.plan(day)

    def _by_id(self, day: date) -> dict[str, DeviceDay]:
        index = self._index.get(day)
        if index is None:
            index = {d.device.device_id: d for d in self.plan(day).devices}
            self._index[day] = index
            for old in sorted(self._index)[:-3]:
                del self._index[old]
        return index

    def deliveries(self, lo: int, hi: int) -> list[Delivery]:
        """Messages whose delivery second is in ``[lo, hi)`` (hi - lo <= one day)."""
        out: list[Delivery] = []
        days = sorted({utc_date_of(lo - 10), utc_date_of(hi - 1)})
        for day in days:
            plan = self.plan(day)
            start, end = plan.start, plan.end
            a, b = max(lo - 10, start), min(hi, end)
            if b <= a:
                continue
            for device_day in plan.devices:
                outages = device_day.outages
                for stream in (READINGS, HEALTH, ALERTS):
                    for t, seq in device_day.emissions(stream, a, b, start):
                        if outages and any(os_ <= t < oe for os_, oe in outages):
                            continue
                        key = _key(device_day.device.serial, t, seq, ord(stream))
                        at = t + int(unit(key) * 3)
                        if lo <= at < hi:
                            out.append(
                                Delivery(at, stream, device_day, t, seq, day, False)
                            )
                        if unit(key + 1) < DUPLICATE_RATE:
                            again = t + 3 + int(unit(key + 2) * 6)
                            if lo <= again < hi:
                                out.append(
                                    Delivery(
                                        again, stream, device_day, t, seq, day, True
                                    )
                                )
        # Uploads at the end of a connectivity outage. An outage that ends at
        # 00:00 UTC belongs to the previous day's plan, so read both days.
        uploaded: set[tuple[str, int, int]] = set()
        first = utc_date_of(lo)
        for day in sorted({first - timedelta(days=1), first, utc_date_of(hi - 1)}):
            for device_day in self.plan(day).devices:
                for begin, finish in device_day.outages:
                    if not lo <= finish < hi:
                        continue
                    identity = (device_day.device.device_id, begin, finish)
                    if identity in uploaded:
                        continue
                    uploaded.add(identity)
                    keep_from = max(begin, finish - self.buffer)
                    out.extend(self._backlog(device_day, keep_from, finish))
        out.sort(key=lambda d: (d.at, d.device_day.device.device_id, d.stream, d.seq))
        return out

    def _backlog(self, device_day: DeviceDay, lo: int, hi: int) -> Iterator[Delivery]:
        device_id = device_day.device.device_id
        days = {
            utc_date_of(lo) + timedelta(days=n)
            for n in range((hi - lo) // DAY_SECONDS + 2)
        }
        for day in sorted(d for d in days if d <= utc_date_of(hi - 1)):
            same = self._by_id(day).get(device_id)
            if same is None:
                continue
            plan = self.plan(day)
            a, b = max(lo, plan.start), min(hi, plan.end)
            for stream in (READINGS, HEALTH, ALERTS):
                for t, seq in same.emissions(stream, a, b, plan.start):
                    yield Delivery(hi, stream, same, t, seq, day, False)


# -- payloads -----------------------------------------------------------------


def _diurnal(t: int, peak_hour: float) -> float:
    local_hours = ((t - 3 * 3600) % DAY_SECONDS) / 3600
    return math.cos(2 * math.pi * (local_hours - peak_hour) / 24)


def _true_metrics(
    plan_fires: dict[int, list[Any]], device_day: DeviceDay, t: int
) -> dict[str, float]:
    serial = device_day.device.serial
    kind = device_day.device.kind.name
    noise = _normal(_key(serial, t, 0x51))
    warm = _diurnal(t, 14.0)
    fire = 0.0
    for event in plan_fires.get(device_day.zone, ()):
        if event.start <= t < event.end:
            fire = event.intensity
    if kind == "smoke_heat":
        return {
            "smoke_ppm": 8.0 + 2.0 * noise + 450.0 * fire,
            "temp_c": 21.0 + 6.0 * warm + 0.6 * noise + 18.0 * fire,
            "humidity_pct": 78.0 - 14.0 * warm + 2.0 * noise - 25.0 * fire,
        }
    if kind == "weather":
        return {
            "temp_c": 21.0 + 6.0 * warm + 0.5 * noise,
            "humidity_pct": 78.0 - 14.0 * warm + 2.0 * noise,
            "wind_speed_ms": max(0.0, 2.5 + 1.5 * warm + noise),
            "wind_dir_deg": (serial * 37 + t // 600 * 3) % 360,
            "pressure_hpa": 1012.0 + 2.0 * _diurnal(t, 10.0) + 0.4 * noise,
            "rain_mm": max(0.0, 0.8 * noise - 0.6),
        }
    if kind == "river_gauge":
        return {
            "water_level_cm": 120.0 + (serial % 90) + 6.0 * noise,
            "water_temp_c": 19.0 + 2.0 * warm + 0.2 * noise,
            "turbidity_ntu": max(0.0, 12.0 + 4.0 * noise),
        }
    return {
        "vwc_pct": 30.0 + (serial % 15) - 3.0 * warm + noise,
        "soil_temp_c": 19.0 + 3.0 * warm + 0.3 * noise,
    }


_PRIMARY = {
    "smoke_heat": "smoke_ppm",
    "weather": "temp_c",
    "river_gauge": "water_level_cm",
    "soil_moisture": "vwc_pct",
}


def payload(
    delivery: Delivery, sent_ms: int, fires: dict[int, list[Any]]
) -> dict[str, Any]:
    """The JSON object of one message; the primary metric is uncalibrated."""
    device_day = delivery.device_day
    device = device_day.device
    t = delivery.emitted
    key = _key(device.serial, t, delivery.seq, ord(delivery.stream))
    event_ms = t * 1000 + int(unit(key + 3) * 1000)
    for begin, finish, offset in device_day.skew:
        if begin <= t < finish:
            event_ms += offset * 1000
    body: dict[str, Any] = {
        "event_id": delivery.event_id,
        "schema_version": SCHEMA_VERSION,
        "device_id": device.device_id,
        "device_type": device.kind.name,
        "stream": STREAM_NAMES[delivery.stream],
        "seq": delivery.seq,
        "event_time": format_utc_ms(event_ms),
        "gateway_time": format_utc_ms(max(sent_ms, t * 1000)),
        "firmware_version": device_day.firmware,
    }
    if delivery.stream == READINGS:
        metrics = _true_metrics(fires, device_day, t)
        primary = _PRIMARY[device.kind.name]
        calibration = device_day.calibration_at(t)
        if calibration is not None:
            offset = calibration.offset_e4 / 10_000
            gain = calibration.gain_e4 / 10_000
            metrics[primary] = (metrics[primary] - offset) / gain
        body["metrics"] = {name: round(value, 2) for name, value in metrics.items()}
        body["battery_pct"] = 40 + int(unit(key + 4) * 60)
        body["rssi_dbm"] = -60 - int(unit(key + 5) * 50)
    elif delivery.stream == HEALTH:
        body.update(
            battery_pct=40 + int(unit(key + 4) * 60),
            solar_w=round(max(0.0, 6.0 * _diurnal(t, 12.0)) * unit(key + 6), 2),
            rssi_dbm=-60 - int(unit(key + 5) * 50),
            uptime_s=t - device.installed_at,
            storage_free_mb=200 + int(unit(key + 7) * 3800),
            enclosure_temp_c=round(22.0 + 8.0 * _diurnal(t, 14.0), 1),
        )
    else:
        alert = device_day.alerts[delivery.seq]
        body.update(
            alert_type=alert.alert_type,
            severity=alert.severity,
            confidence=round(alert.confidence_e3 / 1000, 3),
        )
    return body


# -- checkpoint -------------------------------------------------------------------


class Checkpoint:
    """The last second whose messages Kafka acknowledged, in an atomic file."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> int | None:
        """The saved cursor; None on a first run (no file).

        Raises ``CheckpointError`` when the file exists but cannot be read,
        so a damaged checkpoint is never mistaken for a first run.
        """
        if not self.path.exists():
            return None
        try:
            return int(json.loads(self.path.read_text(encoding="utf-8"))["cursor"])
        except (OSError, ValueError, KeyError, TypeError) as exc:
            msg = f"checkpoint {self.path.name} is unreadable: {type(exc).__name__}"
            raise CheckpointError(msg) from exc

    def save(self, cursor: int) -> None:
        temporary = self.path.with_suffix(".tmp")
        body = json.dumps({"cursor": cursor, "saved_at": int(time.time())})
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)


class CheckpointError(RuntimeError):
    """The checkpoint file exists but cannot be read."""


# -- producer ---------------------------------------------------------------------


def _kafka_config(env: Mapping[str, str]) -> dict[str, Any]:
    return {
        "bootstrap.servers": env.get("ODP_KAFKA_BOOTSTRAP", "kafka:9092"),
        "client.id": "bioguardia-gateway",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 50,
        "queue.buffering.max.messages": 1_000_000,
        "message.timeout.ms": 120_000,
    }


def gateway_records(
    schedule: Schedule,
    checkpoint: int,
    now: int,
    beat: Callable[[], None] = lambda: None,
) -> tuple[list[dict[str, Any]], int, int]:
    """Restart records after ``checkpoint``: a summary and per-device skips.

    Walks the stop hour by hour (about 10 s per hour at scale 1) and calls
    ``beat`` after each hour so the health check sees progress.
    """
    cut = now - schedule.buffer
    skipped: dict[tuple[str, str], int] = {}
    replayed = 0
    lo = checkpoint
    while lo < now:
        beat()
        hi = min(now, lo - lo % 3600 + 3600)
        for delivery in schedule.deliveries(lo, hi):
            if delivery.emitted < cut:
                key = (delivery.day.isoformat(), delivery.device_day.device.device_id)
                skipped[key] = skipped.get(key, 0) + 1
            else:
                replayed += 1
        lo = hi
    restart = format_utc_ms(now * 1000)
    summary = {
        "record_id": f"gw-{now}",
        "schema_version": SCHEMA_VERSION,
        "record_type": "restart",
        "restart_at": restart,
        "last_checkpoint": format_utc_ms(checkpoint * 1000),
        "replay_from": format_utc_ms(max(checkpoint, cut) * 1000),
        "replay_to": restart,
        "skipped_from": format_utc_ms(checkpoint * 1000) if cut > checkpoint else None,
        "skipped_to": format_utc_ms(cut * 1000) if cut > checkpoint else None,
        "events_replayed": replayed,
        "events_skipped": sum(skipped.values()),
    }
    records = [summary]
    by_day: dict[str, dict[str, int]] = {}
    for (day, device_id), count in sorted(skipped.items()):
        by_day.setdefault(day, {})[device_id] = count
    for day, devices in sorted(by_day.items()):
        items = list(devices.items())
        for chunk, start in enumerate(range(0, len(items), DETAIL_CHUNK)):
            records.append(
                {
                    "record_id": f"gw-{now}-{day}-{chunk}",
                    "schema_version": SCHEMA_VERSION,
                    "record_type": "skipped_detail",
                    "restart_at": restart,
                    "utc_date": day,
                    "devices": dict(items[start : start + DETAIL_CHUNK]),
                }
            )
    return records, replayed, summary["events_skipped"]


def run(
    config: WorldConfig, env: Mapping[str, str], *, max_seconds: float | None = None
) -> int:
    """Publish forever (or for ``max_seconds``).

    Exit non-zero when Kafka refuses messages.
    """
    from confluent_kafka import KafkaException, Producer

    state_dir = Path(env.get("ODP_TELEMETRY_STATE_DIR", "/var/lib/bioguardia"))
    state_dir.mkdir(parents=True, exist_ok=True)
    heartbeat = Path(
        env.get("ODP_TELEMETRY_HEARTBEAT", "/tmp/bioguardia-telemetry.heartbeat")
    )
    checkpoint = Checkpoint(state_dir / "telemetry-checkpoint.json")
    schedule = Schedule(fleet_for(build_world(config)))
    producer = Producer(_kafka_config(env))
    failures = {"count": 0}

    def delivered(error: Any, _message: Any) -> None:
        if error is not None:
            failures["count"] += 1

    def send(topic: str, key: str, value: dict[str, Any]) -> None:
        data = json.dumps(value, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                producer.produce(
                    topic, key=key.encode("utf-8"), value=data, on_delivery=delivered
                )
                return
            except BufferError:
                producer.poll(0.2)

    now = int(time.time())
    try:
        cursor = checkpoint.load()
    except CheckpointError as exc:
        # The backlog cannot be known; say so on the gateway topic and start now.
        print(f"telemetry: {exc}; starting from now", file=sys.stderr, flush=True)
        send(
            GATEWAY_TOPIC,
            f"gw-{now}",
            {
                "record_id": f"gw-{now}",
                "schema_version": SCHEMA_VERSION,
                "record_type": "checkpoint_unreadable",
                "restart_at": format_utc_ms(now * 1000),
            },
        )
        cursor = now
    if cursor is None or cursor > now:
        cursor = now
    elif now - cursor > MAX_REPLAY_GAP:
        # Too long to account event by event (and past Kafka retention): record
        # the gap with its bounds only, then resume inside the device buffer.
        send(
            GATEWAY_TOPIC,
            f"gw-{now}",
            {
                "record_id": f"gw-{now}",
                "schema_version": SCHEMA_VERSION,
                "record_type": "gap_too_long",
                "restart_at": format_utc_ms(now * 1000),
                "last_checkpoint": format_utc_ms(cursor * 1000),
            },
        )
        print(
            f"telemetry restart: behind {now - cursor} s, more than "
            f"{MAX_REPLAY_GAP} s; recording the gap without per-device counts",
            flush=True,
        )
    elif now - cursor > RESTART_GAP:

        def beat() -> None:
            heartbeat.write_text(str(int(time.time())), encoding="utf-8")

        records, replayed, skipped = gateway_records(schedule, cursor, now, beat)
        for record in records:
            send(GATEWAY_TOPIC, record["record_id"], record)
        print(
            f"telemetry restart: behind {now - cursor} s, "
            f"replaying {replayed:,} events, "
            f"skipping {skipped:,} older than the buffer",
            flush=True,
        )
    cut_after_restart = now - schedule.buffer
    # Nothing delivered before the cut can be replayed: skip straight to it.
    cursor = max(cursor, min(now, cut_after_restart))
    minute: int | None = None
    batch: list[Delivery] = []
    fires: dict[int, list[Any]] = {}
    position = 0
    last_save = time.monotonic()
    window_started = time.monotonic()
    counts = {READINGS: 0, HEALTH: 0, ALERTS: 0}
    sent_in_second = 0
    second_mark = time.monotonic()
    began = time.monotonic()
    while True:
        if max_seconds is not None and time.monotonic() - began > max_seconds:
            if producer.flush(60) or failures["count"]:
                return 1
            checkpoint.save(cursor)
            return 0
        now = int(time.time())
        heartbeat.write_text(str(now), encoding="utf-8")
        if cursor >= now:
            producer.poll(0.2)
            continue
        start_minute = cursor - cursor % 60
        if minute != start_minute:
            batch = schedule.deliveries(start_minute, start_minute + 60)
            fires = {}
            # Backlog uploads carry events of earlier days: use their fires too.
            today = utc_date_of(start_minute)
            for day in (today - timedelta(days=1), today):
                for fire in schedule.plan(day).fires:
                    if fire not in fires.get(fire.zone, ()):
                        fires.setdefault(fire.zone, []).append(fire)
            minute = start_minute
            position = 0
            while position < len(batch) and batch[position].at < cursor:
                position += 1
        target = min(now, start_minute + 60)
        while position < len(batch) and batch[position].at < target:
            delivery = batch[position]
            position += 1
            if delivery.emitted < cut_after_restart:
                continue  # lost on the device while the gateway was down
            sent_ms = int(time.time() * 1000)
            send(
                TOPICS[delivery.stream],
                delivery.device_day.device.device_id,
                payload(delivery, sent_ms, fires),
            )
            counts[delivery.stream] += 1
            sent_in_second += 1
            if sent_in_second >= MAX_REPLAY_RATE:
                elapsed = time.monotonic() - second_mark
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                sent_in_second = 0
                second_mark = time.monotonic()
        producer.poll(0)
        cursor = target
        if time.monotonic() - last_save >= CHECKPOINT_EVERY:
            remaining = producer.flush(60)
            if remaining or failures["count"]:
                print(
                    f"telemetry: {failures['count']} delivery failures, "
                    f"{remaining} unacknowledged; exiting so Compose "
                    "restarts from the checkpoint",
                    file=sys.stderr,
                    flush=True,
                )
                return 1
            checkpoint.save(cursor)
            last_save = time.monotonic()
        if time.monotonic() - window_started >= 60:
            total = sum(counts.values())
            lag = int(time.time()) - cursor
            print(
                f"telemetry {datetime.fromtimestamp(cursor, UTC):%Y-%m-%dT%H:%MZ}: "
                f"sent {total:,} "
                f"(readings {counts[READINGS]:,}, health {counts[HEALTH]:,}, "
                f"alerts {counts[ALERTS]:,}), lag {lag} s",
                flush=True,
            )
            counts = {READINGS: 0, HEALTH: 0, ALERTS: 0}
            window_started = time.monotonic()
        try:
            producer.poll(0)
        except KafkaException:
            return 1


# -- verify ---------------------------------------------------------------------------


def expected_events(
    schedule: Schedule, lo: int, hi: int, delivered_by: int
) -> dict[str, int]:
    """Event ids the model emits in ``[lo, hi)`` that are not lost on the device
    and are delivered by ``delivered_by``, mapped to their emission second."""
    out: dict[str, int] = {}
    span_lo = lo
    while span_lo < delivered_by:
        span_hi = min(delivered_by, span_lo - span_lo % 3600 + 3600)
        for delivery in schedule.deliveries(span_lo, span_hi):
            if lo <= delivery.emitted < hi and not delivery.duplicate:
                out[delivery.event_id] = delivery.emitted
        span_lo = span_hi
    return out


def _parse(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())


def verify(config: WorldConfig, env: Mapping[str, str], start: str, end: str) -> int:
    """Compare the events in Kafka for ``[start, end)`` with the model."""
    from confluent_kafka import Consumer, TopicPartition

    lo, hi = _parse(start), _parse(end)
    now = int(time.time())
    schedule = Schedule(fleet_for(build_world(config)))
    expected = expected_events(schedule, lo, hi, now - 30)
    consumer = Consumer(
        {
            "bootstrap.servers": env.get("ODP_KAFKA_BOOTSTRAP", "kafka:9092"),
            "group.id": f"bioguardia-verify-{now}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    seen: dict[str, int] = {}
    restarts: list[tuple[int, int]] = []
    for topic in (*TOPICS.values(), GATEWAY_TOPIC):
        metadata = consumer.list_topics(topic, timeout=10)
        partitions = [
            TopicPartition(topic, p, (lo - 3600) * 1000)
            for p in metadata.topics[topic].partitions
        ]
        offsets = consumer.offsets_for_times(partitions, timeout=30)
        ends = {
            p.partition: consumer.get_watermark_offsets(
                TopicPartition(topic, p.partition), timeout=10
            )[1]
            for p in partitions
        }
        consumer.assign([o for o in offsets if o.offset >= 0])
        open_partitions = {
            o.partition for o in offsets if 0 <= o.offset < ends[o.partition]
        }
        while open_partitions:
            message = consumer.poll(5)
            if message is None:
                break
            if message.error():
                continue
            if message.offset() >= ends[message.partition()] - 1:
                open_partitions.discard(message.partition())
            event = json.loads(message.value())
            if topic == GATEWAY_TOPIC:
                if event.get("record_type") == "restart":
                    restarts.append(
                        (_parse(event["last_checkpoint"]), _parse(event["restart_at"]))
                    )
                continue
            identifier = event["event_id"]
            if identifier in expected:
                seen[identifier] = seen.get(identifier, 0) + 1
        consumer.unassign()
    consumer.close()
    # Events the gateway skipped after a stop (older than the device buffer)
    # are reported as such, not as missing.
    skipped: set[str] = set()
    for checkpoint, restart in restarts:
        cut = restart - schedule.buffer
        if cut <= checkpoint or restart <= lo - schedule.buffer:
            continue
        span = checkpoint
        while span < restart:
            upper = min(restart, span - span % 3600 + 3600)
            for delivery in schedule.deliveries(span, upper):
                if delivery.emitted < cut and delivery.event_id in expected:
                    skipped.add(delivery.event_id)
            span = upper
    missing = [i for i in expected if i not in seen and i not in skipped]
    duplicates = sum(count - 1 for count in seen.values() if count > 1)
    print(
        f"verify {start} to {end}: expected {len(expected):,}, found {len(seen):,}, "
        f"gateway-skipped {len(skipped):,}, missing {len(missing):,}, "
        f"duplicate copies {duplicates:,}",
        flush=True,
    )
    return 0 if not missing else 1


def model(config: WorldConfig, start: str, end: str) -> int:
    """Print the messages the model delivers in ``[start, end)``, per topic."""
    lo, hi = _parse(start), _parse(end)
    schedule = Schedule(fleet_for(build_world(config)))
    counts = {READINGS: 0, HEALTH: 0, ALERTS: 0}
    span = lo
    while span < hi:
        upper = min(hi, span - span % 3600 + 3600)
        for delivery in schedule.deliveries(span, upper):
            counts[delivery.stream] += 1
        span = upper
    total = sum(counts.values())
    print(
        f"model {start} to {end}: {total:,} messages "
        f"(readings {counts[READINGS]:,}, health {counts[HEALTH]:,}, "
        f"alerts {counts[ALERTS]:,}), "
        f"{total * 3600 // max(1, hi - lo):,} per hour",
        flush=True,
    )
    return 0


def modeled_rate(config: WorldConfig, day: date, hour: int) -> int:
    """Events the model emits during one UTC hour (deliveries excluded)."""
    plan = fleet_for(build_world(config)).plan(day)
    lo = plan.start + hour * 3600
    return sum(
        d.count(stream, lo, lo + 3600)
        for d in plan.devices
        for stream in (READINGS, HEALTH, ALERTS)
    )
