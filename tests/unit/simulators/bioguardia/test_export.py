"""Daily drops: completeness, determinism, defects, release times, and logs."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
from datetime import date, timedelta
from pathlib import Path

import pytest

from open_data_platform.simulators.bioguardia import FINANCE_SOURCE, OPS_SOURCE, export
from open_data_platform.simulators.bioguardia import __main__ as cli
from open_data_platform.simulators.bioguardia.catalog import OPS_CATALOG
from open_data_platform.simulators.bioguardia.clock import local_midnight
from open_data_platform.simulators.bioguardia.drop import (
    MANIFEST,
    SUCCESS_MARKER,
    DirectoryTarget,
    DropTooLargeError,
    drop_prefix,
    generate_drop,
    release_at,
    versions_of,
)
from open_data_platform.simulators.bioguardia.lifecycles import _CACHE
from open_data_platform.simulators.bioguardia.rng import unit_of
from open_data_platform.simulators.bioguardia.world import (
    World,
    WorldConfig,
    build_world,
)

SCALE = 0.003
DAY = date(2026, 12, 2)


class RecordingTarget(DirectoryTarget):
    """A directory target that remembers the order of writes."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.keys: list[str] = []

    def put_file(self, key: str, path: Path, content_type: str) -> None:
        self.keys.append(key)
        super().put_file(key, path, content_type)

    def put_bytes(self, key: str, body: bytes, content_type: str) -> None:
        self.keys.append(key)
        super().put_bytes(key, body, content_type)


@pytest.fixture(scope="module")
def world() -> World:
    return build_world(WorldConfig(seed=11, scale=SCALE))


def _drop(
    world: World,
    root: Path,
    day: date = DAY,
    version: int = 1,
    source: str = OPS_SOURCE,
):
    landing = RecordingTarget(root / "landing")
    evaluation = DirectoryTarget(root / "evaluation")
    summary = generate_drop(world, source, day, version, landing, evaluation)
    return summary, landing, root / "landing" / drop_prefix(source, day, version)


def _rows(path: Path) -> list[dict[str, str]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        if path.name.endswith(".jsonl.gz"):
            return [json.loads(line) for line in handle]
        return list(csv.DictReader(handle))


def test_a_drop_holds_every_table_its_manifest_and_success_written_last(
    world: World, tmp_path: Path
) -> None:
    summary, landing, folder = _drop(world, tmp_path)
    names = {spec.name for spec in OPS_CATALOG} - {"canopy_surveys"}
    assert {t.spec.name for t in summary.tables} == names
    assert len(summary.tables) == 21
    assert landing.keys[-1].endswith(SUCCESS_MARKER)
    assert landing.keys[-2].endswith(MANIFEST)
    manifest = json.loads((folder / MANIFEST).read_text())
    assert manifest["table_count"] == 21
    for entry in manifest["tables"]:
        path = folder / entry["file"]
        if not path.exists():
            continue  # a planted missing file
        body = path.read_bytes()
        if hashlib.sha256(body).hexdigest() != entry["sha256"]:
            continue  # a planted truncated file
        assert len(body) == entry["bytes"]
        assert len(_rows(path)) == entry["rows"]


def test_the_first_day_of_a_month_adds_canopy_surveys(
    world: World, tmp_path: Path
) -> None:
    summary, _, _ = _drop(world, tmp_path, day=date(2026, 12, 1))
    assert len(summary.tables) == 22
    assert "canopy_surveys" in {t.spec.name for t in summary.tables}


def test_regenerating_a_drop_gives_the_same_bytes(world: World, tmp_path: Path) -> None:
    first, _, _ = _drop(world, tmp_path / "a")
    second, _, _ = _drop(world, tmp_path / "b")
    assert [t.sha256 for t in first.tables] == [t.sha256 for t in second.tables]


def test_a_date_generated_alone_equals_the_same_date_after_its_neighbours(
    world: World, tmp_path: Path
) -> None:
    _CACHE.clear()
    alone, _, _ = _drop(world, tmp_path / "alone")
    _drop(world, tmp_path / "before", day=DAY - timedelta(days=1))
    after, _, _ = _drop(world, tmp_path / "after")
    assert [t.sha256 for t in alone.tables] == [t.sha256 for t in after.tables]


def test_generating_a_later_day_does_not_take_longer(
    world: World, tmp_path: Path
) -> None:
    near, _, _ = _drop(world, tmp_path / "near")
    far, _, _ = _drop(world, tmp_path / "far", day=DAY + timedelta(days=300))
    assert far.seconds < near.seconds * 3 + 2


def test_file_defects_are_planted_against_the_manifest(tmp_path: Path) -> None:
    world = build_world(WorldConfig(seed=11, scale=SCALE))
    day = next(
        DAY + timedelta(days=n)
        for n in range(400)
        if unit_of(11, "truncate", OPS_SOURCE, (DAY + timedelta(days=n)).isoformat())
        < 1 / 30
    )
    summary, _, folder = _drop(world, tmp_path, day=day)
    truncated = [d for d in summary.file_defects if d["defect"] == "truncated_file"]
    assert truncated
    manifest = {
        t["table"]: t for t in json.loads((folder / MANIFEST).read_text())["tables"]
    }
    entry = manifest[truncated[0]["table"]]
    body = (folder / entry["file"]).read_bytes()
    assert len(body) < entry["bytes"]
    assert hashlib.sha256(body).hexdigest() != entry["sha256"]
    planted = json.loads(
        (
            tmp_path / "evaluation" / drop_prefix(OPS_SOURCE, day, 1) / "planted.json"
        ).read_text()
    )
    assert planted["file_defects"] == summary.file_defects


def test_defects_off_plants_nothing_and_never_redelivers(tmp_path: Path) -> None:
    world = World(WorldConfig(seed=11, scale=SCALE, defects=False))
    summary, _, _ = _drop(world, tmp_path)
    planted = json.loads(
        (
            tmp_path / "evaluation" / drop_prefix(OPS_SOURCE, DAY, 1) / "planted.json"
        ).read_text()
    )
    assert summary.file_defects == []
    assert planted["counts"]["record_defects"] == 0
    assert all(
        versions_of(world, OPS_SOURCE, DAY + timedelta(days=n)) == [1]
        for n in range(60)
    )


def test_version_two_differs_only_by_its_listed_corrections(tmp_path: Path) -> None:
    world = build_world(WorldConfig(seed=11, scale=SCALE))
    day = next(
        DAY + timedelta(days=n)
        for n in range(200)
        if versions_of(world, OPS_SOURCE, DAY + timedelta(days=n)) == [1, 2]
    )
    _, _, first = _drop(world, tmp_path, day=day, version=1)
    _, _, second = _drop(world, tmp_path, day=day, version=2)
    planted = json.loads(
        (
            tmp_path / "evaluation" / drop_prefix(OPS_SOURCE, day, 2) / "planted.json"
        ).read_text()
    )
    corrected = {(c["table"], c["key"]) for c in planted["corrections"]}
    for table, key_column in (
        ("incidents", "incident_id"),
        ("fire_hotspots", "hotspot_id"),
    ):
        path_one, path_two = first / f"{table}.csv.gz", second / f"{table}.csv.gz"
        if not path_one.exists():
            continue
        one = {r[key_column]: r for r in _rows(path_one)}
        two = {r[key_column]: r for r in _rows(path_two)}
        changed = {k for k in one if k in two and one[k] != two[k]}
        assert changed == {k for t, k in corrected if t == table}


def test_a_drop_over_the_byte_cap_publishes_nothing(
    world: World, tmp_path: Path
) -> None:
    landing = RecordingTarget(tmp_path / "landing")
    with pytest.raises(DropTooLargeError):
        generate_drop(world, OPS_SOURCE, DAY, 1, landing, None, max_bytes=1000)
    assert landing.keys == []


def test_release_times_follow_the_schedule() -> None:
    day = date(2026, 12, 10)
    assert (
        release_at(OPS_SOURCE, day, 1) == local_midnight(date(2026, 12, 11)) + 2 * 3600
    )
    assert (
        release_at(FINANCE_SOURCE, day, 1)
        == local_midnight(date(2026, 12, 11)) + 6 * 3600
    )
    assert (
        release_at(OPS_SOURCE, day, 2) == local_midnight(date(2026, 12, 14)) + 2 * 3600
    )


def test_the_release_loop_publishes_only_what_is_due(world: World) -> None:
    now = local_midnight(date(2026, 12, 3)) + 3 * 3600  # 03:00 BRT on Dec 3
    due = export.due_drops(world, date(2026, 12, 1), now)
    assert (OPS_SOURCE, date(2026, 12, 2), 1) in due
    assert (FINANCE_SOURCE, date(2026, 12, 1), 1) in due
    assert (FINANCE_SOURCE, date(2026, 12, 2), 1) not in due  # released at 06:00
    assert all(release_at(*task) <= now for task in due)


def test_the_cli_refuses_unreleased_dates_and_logs_no_row_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("ODP_BIOGUARDIA_SEED", "11")
    monkeypatch.setenv("ODP_BIOGUARDIA_SCALE", str(SCALE))
    future = (date.today() + timedelta(days=30)).isoformat()
    args = [
        "export",
        "generate",
        "--start",
        future,
        "--end",
        future,
        "--target-dir",
        str(tmp_path),
    ]
    assert cli.main(args) == 2
    assert "refusing" in capsys.readouterr().err
    assert not (tmp_path / "landing").exists()
    assert cli.main([*args, "--allow-future", "--source", "ops"]) == 0
    output = capsys.readouterr().out
    assert "bioguardia_ops" in output
    assert "@" not in output
    assert not re.search(r"\d{3}\.\d{3}\.\d{3}-\d{2}", output)
    assert not re.search(r"\+55 \d{2}", output)


class _FailingTarget(RecordingTarget):
    """Fails on the fifth write, like a crash in the middle of an upload."""

    def put_file(self, key: str, path: Path, content_type: str) -> None:
        if len(self.keys) == 4:
            raise OSError("simulated crash")
        super().put_file(key, path, content_type)


def test_a_drop_interrupted_mid_upload_has_no_success_and_a_rerun_completes_it(
    world: World, tmp_path: Path
) -> None:
    failing = _FailingTarget(tmp_path / "landing")
    with pytest.raises(OSError):
        generate_drop(world, OPS_SOURCE, DAY, 1, failing, None)
    folder = tmp_path / "landing" / drop_prefix(OPS_SOURCE, DAY, 1)
    assert len(failing.keys) == 4
    assert not (folder / SUCCESS_MARKER).exists()
    summary = generate_drop(
        world, OPS_SOURCE, DAY, 1, DirectoryTarget(tmp_path / "landing"), None
    )
    assert (folder / SUCCESS_MARKER).exists()
    clean, _, _ = _drop(world, tmp_path / "clean")
    assert [t.sha256 for t in summary.tables] == [t.sha256 for t in clean.tables]


def test_the_manifest_time_range_skips_planted_malformed_timestamps(
    world: World, tmp_path: Path
) -> None:
    from datetime import datetime

    _, _, folder = _drop(world, tmp_path)
    for entry in json.loads((folder / MANIFEST).read_text())["tables"]:
        if entry["time_range"]:
            datetime.fromisoformat(entry["time_range"]["min"])
            datetime.fromisoformat(entry["time_range"]["max"])
            assert entry["time_range"]["max"] < "2027-12-31"


def test_a_bucket_error_other_than_not_found_is_raised() -> None:
    from botocore.exceptions import ClientError

    from open_data_platform.simulators.bioguardia.drop import BucketTarget

    class Client:
        status = 404

        def head_bucket(self, **_: object) -> None:
            return None

        def head_object(self, **_: object) -> None:
            raise ClientError(
                {
                    "Error": {"Code": str(self.status)},
                    "ResponseMetadata": {"HTTPStatusCode": self.status},
                },
                "HeadObject",
            )

    client = Client()
    target = BucketTarget(client, "landing")
    assert target.exists("missing") is False
    client.status = 503
    with pytest.raises(ClientError):
        target.exists("anything")


def test_tap_ignores_a_malformed_timestamp_for_the_range() -> None:
    from open_data_platform.simulators.bioguardia.drop import _tap
    from open_data_platform.simulators.bioguardia.tables_ops import TRACK_POINTS

    names = [c.name for c in TRACK_POINTS.columns]
    good = ["P", "0", "2026-12-02T10:00:00-03:00", "S", "-1.0", "-1.0", "1.0", "2.5"]
    late = ["P", "1", "2026-12-02T23:00:00-03:00", "S", "-1.0", "-1.0", "1.0", "2.5"]
    bad = ["P", "2", "2026-13-45T25:61:00-03:00", "S", "-1.0", "-1.0", "1.0", "2.5"]
    stats: dict[str, object] = {}
    assert len(list(_tap(TRACK_POINTS, names, [good, bad, late], stats))) == 3
    assert stats["time_range"] == {
        "column": "recorded_at",
        "min": "2026-12-02T10:00:00-03:00",
        "max": "2026-12-02T23:00:00-03:00",
    }
    assert stats["totals"] == {"sum_speed_kmh": "7.5"}
