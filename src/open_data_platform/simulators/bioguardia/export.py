"""Publish BioGuardia drops: a date range on demand, or the daily release loop.

The release loop is the agency's export service. It publishes every drop
whose release time has passed and that has no ``_SUCCESS`` yet, oldest first,
so a stopped service catches up when it starts again. It never publishes a
drop before its release time. Logs carry dates, table names, counts, bytes,
and durations only.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from open_data_platform.simulators.bioguardia import (
    FINANCE_SOURCE,
    OPS_SOURCE,
    TELEMETRY_SOURCE,
)
from open_data_platform.simulators.bioguardia.clock import (
    format_utc_ms,
    local_date_of,
)
from open_data_platform.simulators.bioguardia.drop import (
    DEFAULT_MAX_DROP_BYTES,
    SUCCESS_MARKER,
    BucketTarget,
    DirectoryTarget,
    DropSummary,
    Target,
    drop_prefix,
    generate_drop,
    release_at,
    versions_of,
)
from open_data_platform.simulators.bioguardia.fleet import fleet_for
from open_data_platform.simulators.bioguardia.world import (
    World,
    WorldConfig,
    build_world,
)

SOURCES = {
    "ops": (OPS_SOURCE,),
    "finance": (FINANCE_SOURCE,),
    "all": (OPS_SOURCE, FINANCE_SOURCE),
}


@dataclass(frozen=True)
class Destination:
    """Where drops and planted lists go: two buckets, or one local directory."""

    directory: Path | None = None
    landing_bucket: str = "odp-landing"
    evaluation_bucket: str = "odp-evaluation"

    def targets(self, env: Mapping[str, str]) -> tuple[Target, Target]:
        if self.directory is not None:
            return (
                DirectoryTarget(self.directory / "landing"),
                DirectoryTarget(self.directory / "evaluation"),
            )
        from open_data_platform.object_storage import client_from_env

        client = client_from_env(env)
        return (
            BucketTarget(client, self.landing_bucket),
            BucketTarget(client, self.evaluation_bucket),
        )


def telemetry_planted(world: World, day: date) -> dict[str, Any]:
    """What the stream plants on UTC ``day``: windows and counts, no payloads."""
    fleet = fleet_for(world)
    plan = fleet.plan(day)
    buffer = world.config.buffer_hours * 3600
    by_slot = {(d.device.slot.kind.name, d.device.slot.slot): d for d in plan.devices}

    def stamp(epoch: int) -> str:
        return format_utc_ms(epoch * 1000)

    outages = []
    for device_day in plan.devices:
        for begin, finish in device_day.outages:
            if plan.start < finish:
                dropped = plan.counters(device_day, buffer)[3]
                outages.append(
                    {
                        "device_id": device_day.device.device_id,
                        "start": stamp(begin),
                        "end": stamp(finish),
                        "dropped_today": dropped,
                    }
                )
    chatter = []
    for key, (begin, finish) in sorted(plan.chatter.items()):
        found = by_slot.get(key)
        if found is not None:
            chatter.append(
                {
                    "device_id": found.device.device_id,
                    "start": stamp(begin),
                    "end": stamp(finish),
                }
            )
    skewed = []
    for key, (begin, finish, offset) in sorted(plan.skewed.items()):
        found = by_slot.get(key)
        if found is not None:
            skewed.append(
                {
                    "device_id": found.device.device_id,
                    "start": stamp(begin),
                    "end": stamp(finish),
                    "offset_seconds": offset,
                }
            )
    return {
        "source_id": TELEMETRY_SOURCE,
        "utc_date": day.isoformat(),
        "fires": [
            {
                "zone_id": world.zones[f.zone].zone_id,
                "start": stamp(f.start),
                "end": stamp(f.end),
            }
            for f in plan.fires
        ],
        "chattering_devices": chatter,
        "clock_skew_devices": skewed,
        "outages": outages,
        "duplicate_rule": (
            "an event is sent twice, 3 to 8 seconds apart, "
            "when its duplicate draw is below 0.001"
        ),
        "counts": {
            "fires": len(plan.fires),
            "chattering_devices": len(chatter),
            "clock_skew_devices": len(skewed),
            "outages": len(outages),
        },
    }


def publish(
    world: World,
    source: str,
    day: date,
    version: int,
    destination: Destination,
    env: Mapping[str, str],
    *,
    max_bytes: int,
    work_dir: Path | None,
) -> DropSummary:
    landing, evaluation = destination.targets(env)
    if source == OPS_SOURCE and version == 1:
        # Written before the drop's _SUCCESS, so a complete drop always has it.
        planted = telemetry_planted(world, day)
        evaluation.put_bytes(
            f"{TELEMETRY_SOURCE}/utc_date={day.isoformat()}/planted.json",
            json.dumps(planted, separators=(",", ":")).encode("utf-8"),
            "application/json",
        )
    summary = generate_drop(
        world,
        source,
        day,
        version,
        landing,
        evaluation,
        work_dir=work_dir,
        max_bytes=max_bytes,
    )
    return summary


def describe(summary: DropSummary) -> str:
    defects = ",".join(d["defect"] for d in summary.file_defects) or "none"
    return (
        f"{summary.source} {summary.day} v{summary.version}: "
        f"{len(summary.tables)} tables, "
        f"{summary.rows:,} rows, {summary.size:,} bytes, {summary.seconds:.1f} s, "
        f"file defects {defects}"
    )


def planned_drops(
    world: World, sources: tuple[str, ...], first: date, last: date
) -> list[tuple[str, date, int]]:
    out = []
    day = first
    while day <= last:
        for source in sources:
            for version in versions_of(world, source, day):
                out.append((source, day, version))
        day += timedelta(days=1)
    return out


_WORKER: dict[str, Any] = {}


def _init_worker(config: WorldConfig) -> None:
    _WORKER["world"] = build_world(config)


def _run_one(
    task: tuple[str, date, int],
    destination: Destination,
    env: dict[str, str],
    max_bytes: int,
    work_dir: Path | None,
) -> str:
    source, day, version = task
    summary = publish(
        _WORKER["world"],
        source,
        day,
        version,
        destination,
        env,
        max_bytes=max_bytes,
        work_dir=work_dir,
    )
    return describe(summary)


def generate_range(
    config: WorldConfig,
    sources: tuple[str, ...],
    first: date,
    last: date,
    destination: Destination,
    env: Mapping[str, str],
    *,
    allow_future: bool,
    overwrite: bool,
    workers: int,
    max_bytes: int,
    work_dir: Path | None,
    now: int | None = None,
) -> int:
    """Publish every drop of the range; returns the number published."""
    world = build_world(config)
    instant = int(time.time()) if now is None else now
    tasks = planned_drops(world, sources, first, last)
    if not allow_future:
        early = [t for t in tasks if release_at(*t) > instant]
        if early:
            source, day, version = early[0]
            print(
                f"refusing {len(early)} drops not yet released "
                f"(first: {source} {day} v{version}); "
                "pass --allow-future to write them anyway",
                file=sys.stderr,
            )
            return -1
    if not overwrite:
        landing, _ = destination.targets(env)
        tasks = [
            t
            for t in tasks
            if not landing.exists(f"{drop_prefix(*t)}/{SUCCESS_MARKER}")
        ]
    print(f"publishing {len(tasks)} drops with {workers} workers", flush=True)
    if workers <= 1:
        _init_worker(config)
        for task in tasks:
            print(
                _run_one(task, destination, dict(env), max_bytes, work_dir), flush=True
            )
        return len(tasks)
    done = 0
    with ProcessPoolExecutor(
        max_workers=workers, initializer=_init_worker, initargs=(config,)
    ) as pool:
        futures = [
            pool.submit(_run_one, task, destination, dict(env), max_bytes, work_dir)
            for task in tasks
        ]
        for future in as_completed(futures):
            print(future.result(), flush=True)
            done += 1
    return done


def due_drops(
    world: World, history_start: date, now: int
) -> list[tuple[str, date, int]]:
    """Drops whose release time has passed, oldest first."""
    last = local_date_of(now) - timedelta(days=1)
    tasks = planned_drops(world, (OPS_SOURCE, FINANCE_SOURCE), history_start, last)
    return sorted(
        (t for t in tasks if release_at(*t) <= now),
        key=lambda t: (release_at(*t), t[0]),
    )


def serve(
    config: WorldConfig,
    destination: Destination,
    env: Mapping[str, str],
    *,
    history_start: date,
    interval: int,
    max_bytes: int,
    work_dir: Path | None,
    heartbeat: Path | None,
    once: bool = False,
) -> None:
    """The export service: publish what is due, then sleep, forever.

    A storage error (MinIO down, for example) ends the pass, not the service:
    the next pass retries, and drops stay in date order because a drop is
    complete only once its ``_SUCCESS`` exists.
    """
    world = build_world(config)
    _clear_stale_scratch(work_dir)
    while True:
        _beat(heartbeat)
        try:
            landing, _ = destination.targets(env)
            for task in due_drops(world, history_start, int(time.time())):
                if landing.exists(f"{drop_prefix(*task)}/{SUCCESS_MARKER}"):
                    continue
                _beat(heartbeat)
                summary = publish(
                    world,
                    *task,
                    destination,
                    env,
                    max_bytes=max_bytes,
                    work_dir=work_dir,
                )
                print(describe(summary), flush=True)
        except Exception as exc:  # logged by type; the next pass retries
            print(
                f"export pass failed ({type(exc).__name__}); retrying in {interval} s",
                file=sys.stderr,
                flush=True,
            )
            if once:
                raise
        _beat(heartbeat)
        if once:
            return
        time.sleep(interval)


def _clear_stale_scratch(work_dir: Path | None, older_than: int = 6 * 3600) -> None:
    """Remove scratch folders a killed run left behind (``bioguardia-*`` only)."""
    if work_dir is None or not work_dir.is_dir():
        return
    limit = time.time() - older_than
    for folder in work_dir.glob("bioguardia-*"):
        if folder.is_dir() and folder.stat().st_mtime < limit:
            shutil.rmtree(folder, ignore_errors=True)


def _beat(path: Path | None) -> None:
    if path is not None:
        path.write_text(str(int(time.time())), encoding="utf-8")


def settings(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    values = os.environ if env is None else env
    work = values.get("ODP_BIOGUARDIA_WORK_DIR", "").strip()
    history = values.get("ODP_BIOGUARDIA_HISTORY_START", "").strip()
    return {
        "landing_bucket": values.get("ODP_LANDING_BUCKET", "odp-landing"),
        "evaluation_bucket": values.get("ODP_EVALUATION_BUCKET", "odp-evaluation"),
        "max_bytes": int(
            values.get("ODP_BIOGUARDIA_MAX_DROP_BYTES", str(DEFAULT_MAX_DROP_BYTES))
        ),
        "work_dir": Path(work) if work else None,
        "history_start": date.fromisoformat(history) if history else None,
    }
