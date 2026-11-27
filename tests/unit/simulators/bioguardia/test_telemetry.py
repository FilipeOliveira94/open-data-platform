"""Telemetry producer: rate, completeness against the counters, restarts, payloads."""

from __future__ import annotations

import json
import sys
import types
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from open_data_platform.simulators.bioguardia import telemetry
from open_data_platform.simulators.bioguardia.fleet import (
    ALERTS,
    HEALTH,
    READINGS,
    fleet_for,
)
from open_data_platform.simulators.bioguardia.telemetry import (
    Checkpoint,
    Schedule,
    gateway_records,
    modeled_rate,
    payload,
)
from open_data_platform.simulators.bioguardia.world import WorldConfig, build_world

DAY = date(2026, 12, 2)
REPO = Path(__file__).resolve().parents[4]


def test_the_modeled_rate_at_scale_one_is_at_least_one_million_per_hour() -> None:
    # Rates scale linearly with the fleet: scale 0.1 needs a tenth.
    config = WorldConfig(seed=7, scale=0.1)
    for hour in (0, 9, 18):
        assert modeled_rate(config, DAY, hour) >= 100_000


def test_delivered_events_per_device_day_equal_generated_minus_dropped() -> None:
    world = build_world(WorldConfig(seed=13, scale=0.01))
    fleet = fleet_for(world)
    schedule = Schedule(fleet)
    plan = fleet.plan(DAY)
    delivered: Counter[str] = Counter()
    ids: set[str] = set()
    lo = plan.start
    while lo < plan.end + 18 * 3600:  # outage uploads can land the next day
        for delivery in schedule.deliveries(lo, lo + 3600):
            if delivery.duplicate or delivery.day != DAY:
                continue
            assert delivery.event_id not in ids
            ids.add(delivery.event_id)
            delivered[delivery.device_day.device.device_id] += 1
        lo += 3600
    buffer = world.config.buffer_hours * 3600
    for device_day in plan.devices:
        readings, health, alerts, dropped, _ = plan.counters(device_day, buffer)
        assert (
            delivered[device_day.device.device_id]
            == readings + health + alerts - dropped
        )


def test_a_restart_skips_events_older_than_the_buffer_and_records_them() -> None:
    world = build_world(WorldConfig(seed=13, scale=0.01, buffer_hours=1))
    schedule = Schedule(fleet_for(world))
    now = schedule.plan(DAY).start + 15 * 3600
    checkpoint = now - 3 * 3600
    records, replayed, skipped = gateway_records(schedule, checkpoint, now)
    expected_skipped = 0
    expected_replayed = 0
    lo = checkpoint
    while lo < now:
        for delivery in schedule.deliveries(lo, min(now, lo + 3600)):
            if delivery.emitted < now - 3600:
                expected_skipped += 1
            else:
                expected_replayed += 1
        lo += 3600
    assert (replayed, skipped) == (expected_replayed, expected_skipped)
    summary, *details = records
    assert summary["record_type"] == "restart"
    assert summary["events_skipped"] == skipped
    assert sum(sum(d["devices"].values()) for d in details) == skipped


def test_payload_fields_match_the_source_note() -> None:
    world = build_world(WorldConfig(seed=13, scale=0.01))
    schedule = Schedule(fleet_for(world))
    plan = schedule.plan(DAY)
    note = (REPO / "docs" / "sources" / "bioguardia-telemetry.md").read_text(
        encoding="utf-8"
    )
    seen = set()
    lo = plan.start
    while len(seen) < 3 and lo < plan.end:
        for delivery in schedule.deliveries(lo, lo + 3600):
            if delivery.stream in seen:
                continue
            seen.add(delivery.stream)
            body = payload(delivery, delivery.at * 1000, {})
            for field in body:
                assert f"| `{field}` |" in note, (delivery.stream, field)
            if delivery.stream == READINGS:
                for metric in body["metrics"]:
                    assert f"`{metric}`" in note, metric
        lo += 3600
    assert seen == {READINGS, HEALTH, ALERTS}


def test_checkpoint_round_trip(tmp_path: Path) -> None:
    checkpoint = Checkpoint(tmp_path / "state.json")
    assert checkpoint.load() is None
    checkpoint.save(1_800_000_000)
    assert checkpoint.load() == 1_800_000_000


class _FakeProducer:
    sent: list[tuple[str, bytes, bytes]] = []

    def __init__(self, config: dict[str, Any]) -> None:
        assert config["acks"] == "all"
        assert config["enable.idempotence"] is True

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        self.sent.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


def test_a_restarted_producer_replays_its_backlog_and_logs_only_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fake = types.ModuleType("confluent_kafka")
    fake.Producer = _FakeProducer  # type: ignore[attr-defined]
    fake.KafkaException = RuntimeError  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake)
    _FakeProducer.sent = []
    import time

    Checkpoint(tmp_path / "telemetry-checkpoint.json").save(int(time.time()) - 600)
    env = {
        "ODP_TELEMETRY_STATE_DIR": str(tmp_path),
        "ODP_TELEMETRY_HEARTBEAT": str(tmp_path / "heartbeat"),
    }
    config = WorldConfig(seed=13, scale=0.002)
    assert telemetry.run(config, env, max_seconds=4) == 0
    topics = Counter(topic for topic, _, _ in _FakeProducer.sent)
    assert topics[telemetry.GATEWAY_TOPIC] >= 1
    assert topics[telemetry.TOPICS[READINGS]] > 0
    gateway = json.loads(
        next(v for t, _, v in _FakeProducer.sent if t == telemetry.GATEWAY_TOPIC)
    )
    assert gateway["events_skipped"] == 0
    assert Checkpoint(tmp_path / "telemetry-checkpoint.json").load() is not None
    output = capsys.readouterr().out
    assert "metrics" not in output
    assert "event_id" not in output
    assert "replaying" in output


def test_events_of_a_device_keep_contiguous_sequence_numbers_across_minutes() -> None:
    world = build_world(WorldConfig(seed=13, scale=0.005))
    schedule = Schedule(fleet_for(world))
    plan = schedule.plan(DAY)
    seqs: dict[tuple[str, str], list[int]] = {}
    for lo in range(plan.start, plan.start + 2 * 3600, 60):
        for delivery in schedule.deliveries(lo, lo + 60):
            if (
                delivery.duplicate
                or delivery.day != DAY
                or delivery.emitted >= plan.start + 3600
            ):
                continue
            if delivery.device_day.outages:
                continue  # offline devices upload their events later
            seqs.setdefault(
                (delivery.device_day.device.device_id, delivery.stream), []
            ).append(delivery.seq)
    for values in seqs.values():
        ordered = sorted(values)
        assert ordered == list(range(ordered[0], ordered[0] + len(ordered)))
    _ = timedelta


def test_overlapping_outages_are_merged() -> None:
    from open_data_platform.simulators.bioguardia.fleet import merge_intervals

    assert merge_intervals([(10, 20), (15, 30), (40, 50), (30, 35)]) == [
        (10, 35),
        (40, 50),
    ]
    assert merge_intervals([(0, 100), (10, 20)]) == [(0, 100)]


def test_an_outage_ending_at_midnight_is_uploaded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from open_data_platform.simulators.bioguardia import fleet as fleet_module
    from open_data_platform.simulators.bioguardia.world import World

    world = World(WorldConfig(seed=13, scale=0.005))
    fleet = fleet_module.Fleet(world)
    target = fleet.slots[0]
    midnight = fleet.plan(DAY).end  # 00:00 UTC of the next day
    original = fleet_module._outages

    def injected(seed: int, device: Any, day_key: int, start: int, buffer: int):
        found = original(seed, device, day_key, start, buffer)
        if device.slot is target:
            outage = (midnight - 3 * 3600, midnight)
            if outage[1] >= start and outage[0] < start + 86400:
                found = sorted({*found, outage})
        return found

    monkeypatch.setattr(fleet_module, "_outages", injected)
    fleet._plans.clear()
    schedule = Schedule(fleet)
    plan = fleet.plan(DAY)
    device_day = next(d for d in plan.devices if d.device.slot is target)
    delivered = set()
    for lo in range(plan.start, plan.end + 2 * 3600, 3600):
        for delivery in schedule.deliveries(lo, lo + 3600):
            same = delivery.device_day.device.device_id == device_day.device.device_id
            if same and delivery.day == DAY and not delivery.duplicate:
                delivered.add(delivery.event_id)
    readings, health, alerts, dropped, _ = plan.counters(device_day, 12 * 3600)
    assert len(delivered) == readings + health + alerts - dropped
