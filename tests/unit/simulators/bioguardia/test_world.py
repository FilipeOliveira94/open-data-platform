"""World model: skew, SCD2 signals, the emission schedule, and the source notes."""

from __future__ import annotations

import csv
import gzip
from datetime import date, timedelta
from pathlib import Path

import pytest

from open_data_platform.simulators.bioguardia import OPS_SOURCE
from open_data_platform.simulators.bioguardia.catalog import CATALOGS
from open_data_platform.simulators.bioguardia.clock import (
    DAY_SECONDS,
    local_midnight,
    utc_midnight,
)
from open_data_platform.simulators.bioguardia.context import DayContext
from open_data_platform.simulators.bioguardia.drop import (
    DirectoryTarget,
    drop_prefix,
    generate_drop,
)
from open_data_platform.simulators.bioguardia.fleet import (
    ALERTS,
    HEALTH,
    READINGS,
    Segment,
    fleet_for,
)
from open_data_platform.simulators.bioguardia.lifecycles import patrols_planned_for
from open_data_platform.simulators.bioguardia.tables_master import (
    devices_rows,
    staff_rows,
)
from open_data_platform.simulators.bioguardia.world import (
    World,
    WorldConfig,
    build_world,
)

DAY = date(2026, 12, 2)
REPO = Path(__file__).resolve().parents[4]


@pytest.fixture(scope="module")
def world() -> World:
    return build_world(WorldConfig(seed=11, scale=0.05))


def test_one_reserve_holds_about_thirty_percent_of_devices(world: World) -> None:
    fleet = fleet_for(world)
    share = sum(1 for s in fleet.slots if world.zones[s.zone].reserve == 0) / len(
        fleet.slots
    )
    assert 0.2 <= share <= 0.4


def test_ten_species_carry_close_to_half_of_the_detection_weight(world: World) -> None:
    _, weights = world.ranked_species("vocal", 0)
    assert 0.35 <= weights[9] / weights[-1] <= 0.65


def test_segment_counts_match_listed_times() -> None:
    segment = Segment(start=1000, end=5000, cadence=120, anchor=1037)
    for lo, hi in ((0, 10_000), (1000, 1037), (1037, 1038), (2000, 4321)):
        assert segment.count(lo, hi) == len(segment.times(lo, hi))


def test_sequence_numbers_are_contiguous_and_counters_match_emissions(
    world: World,
) -> None:
    fleet = fleet_for(world)
    plan = fleet.plan(DAY)
    buffer = world.config.buffer_hours * 3600
    checked = 0
    for device_day in plan.devices[:300]:
        for stream in (READINGS, HEALTH, ALERTS):
            seqs = [
                seq
                for _, seq in device_day.emissions(
                    stream, plan.start, plan.end, plan.start
                )
            ]
            assert seqs == list(range(len(seqs)))
            assert len(seqs) == device_day.count(stream, plan.start, plan.end)
        readings, health, alerts, dropped, _ = plan.counters(device_day, buffer)
        assert readings == device_day.count(READINGS, plan.start, plan.end)
        assert dropped <= readings + health + alerts
        checked += 1
    assert checked


def test_minute_windows_add_up_to_the_day(world: World) -> None:
    plan = fleet_for(world).plan(DAY)
    device_day = next(d for d in plan.devices if d.readings)
    total = sum(
        len(list(device_day.emissions(READINGS, lo, lo + 60, plan.start)))
        for lo in range(plan.start, plan.end, 60)
    )
    assert total == device_day.count(READINGS, plan.start, plan.end)


def test_an_outage_longer_than_the_buffer_drops_the_oldest_messages(
    world: World,
) -> None:
    fleet = fleet_for(world)
    buffer = world.config.buffer_hours * 3600
    for offset in range(90):
        plan = fleet.plan(DAY + timedelta(days=offset))
        for device_day in plan.devices:
            if len(device_day.outages) != 1:
                continue
            begin, finish = device_day.outages[0]
            if finish - begin > buffer and plan.start <= begin and finish <= plan.end:
                expected = sum(
                    device_day.count(s, begin, finish - buffer)
                    for s in (READINGS, HEALTH, ALERTS)
                )
                assert expected > 0
                assert plan.counters(device_day, buffer)[3] == expected
                return
    pytest.skip("no outage longer than the buffer in the sampled days")


def test_a_late_entered_calibration_appears_when_it_is_entered(world: World) -> None:
    fleet = fleet_for(world)
    for slot in fleet.slots:
        for device in fleet.generations(slot, local_midnight(DAY)):
            for calibration in fleet.calibrations(device, local_midnight(DAY)):
                late = calibration.entered_at - calibration.calibrated_at
                if (
                    late >= DAY_SECONDS
                    and calibration.calibrated_at > device.installed_at
                ):
                    before = fleet.state(
                        device, calibration.entered_at - 1, recorded=True
                    )
                    after = fleet.state(device, calibration.entered_at, recorded=True)
                    truth = fleet.state(
                        device, calibration.calibrated_at, recorded=False
                    )
                    assert truth.calibration == calibration
                    assert before.calibration != calibration
                    assert after.calibration == calibration
                    return
    pytest.fail("no late-entered calibration found")


def test_snapshots_show_future_dated_assignments(world: World) -> None:
    ctx = DayContext(world, DAY)
    header = [c.name for c in CATALOGS[OPS_SOURCE][2].columns]
    position = header.index("assignment_effective_from")
    found = False
    for offset in range(0, 120, 3):
        ctx = DayContext(world, DAY + timedelta(days=offset))
        for row in staff_rows(ctx):
            if row[position] > ctx.day.isoformat():
                found = True
                break
        if found:
            break
    assert found


def test_devices_reference_existing_zones_except_planted_orphans(world: World) -> None:
    ctx = DayContext(world, DAY)
    zones = {z.zone_id for z in world.zones}
    orphans = [row for row in devices_rows(ctx) if row[4] not in zones]
    planted = [d for d in ctx.planted.record_defects if d["defect"] == "orphan_fk"]
    assert len(orphans) == len(planted)


def test_a_deleted_patrol_never_appears_after_its_deletion(
    world: World, tmp_path: Path
) -> None:
    small = build_world(WorldConfig(seed=11, scale=0.01))
    planned_day, cancelled = next(
        (day, found)
        for day in (DAY + timedelta(days=n) for n in range(120))
        if (found := [p for p in patrols_planned_for(small, day) if p.cancelled_at])
    )
    target = DirectoryTarget(tmp_path)
    for offset in range(-3, 2):
        day = planned_day + timedelta(days=offset)
        generate_drop(small, OPS_SOURCE, day, 1, target, None)
        folder = tmp_path / drop_prefix(OPS_SOURCE, day, 1)
        with gzip.open(folder / "patrols.csv.gz", "rt", encoding="utf-8") as handle:
            ids = {row["patrol_id"] for row in csv.DictReader(handle)}
        for patrol in cancelled:
            if patrol.cancelled_at < local_midnight(day):
                assert patrol.patrol_id not in ids


def test_the_world_is_the_same_for_the_same_config() -> None:
    first = World(WorldConfig(seed=5, scale=0.01))
    second = World(WorldConfig(seed=5, scale=0.01))
    assert [r.name for r in first.reserves] == [r.name for r in second.reserves]
    assert [z.lat for z in first.zones] == [z.lat for z in second.zones]
    assert utc_midnight(DAY) == local_midnight(DAY) - 3 * 3600


@pytest.mark.parametrize(
    ("source", "note"),
    [
        ("bioguardia_ops", "bioguardia-ops-export.md"),
        ("bioguardia_finance", "bioguardia-finance-export.md"),
    ],
)
def test_source_notes_list_every_generated_column(source: str, note: str) -> None:
    text = (REPO / "docs" / "sources" / note).read_text(encoding="utf-8")
    for spec in CATALOGS[source]:
        assert f"### `{spec.name}`" in text, spec.name
        for column in spec.columns:
            assert f"| `{column.name}` |" in text, f"{spec.name}.{column.name}"
