"""Per-run scratch folder retention under the work root (P3-10)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from open_data_platform.lakehouse import medallion_build
from open_data_platform.lakehouse.medallion_build import MedallionBuildError
from open_data_platform.lakehouse.work_retention import prune_work

NOW = 1_800_000_000.0
DAY = 86_400


def _folder(root: Path, name: str, age_days: float, *, size: int = 10) -> Path:
    folder = root / name
    (folder / "dbt-target").mkdir(parents=True)
    data = folder / "dbt-target" / "run_results.json"
    data.write_bytes(b"x" * size)
    stamp = NOW - age_days * DAY
    for path in (data, folder / "dbt-target", folder):
        os.utime(path, (stamp, stamp))
    return folder


def test_removes_old_folders_and_keeps_recent_ones(tmp_path: Path) -> None:
    old = _folder(tmp_path, "scheduled__old", 8, size=25)
    recent = _folder(tmp_path, "scheduled__recent", 2)

    result = prune_work(tmp_path, "current", 7, now=NOW)

    assert not old.exists()
    assert recent.exists()
    assert (result.removed, result.kept, result.bytes_freed) == (1, 1, 25)


def test_a_recent_file_keeps_an_old_folder(tmp_path: Path) -> None:
    folder = _folder(tmp_path, "manual__quiet", 30)
    fresh = folder / "dbt-target" / "late.json"
    fresh.write_text("{}", encoding="utf-8")
    os.utime(fresh, (NOW - DAY, NOW - DAY))

    assert prune_work(tmp_path, "current", 7, now=NOW).removed == 0
    assert folder.exists()


def test_never_removes_the_current_run_folder(tmp_path: Path) -> None:
    current = _folder(tmp_path, "manual__now", 90)

    assert prune_work(tmp_path, "manual__now", 7, now=NOW).removed == 0
    assert current.exists()


def test_keeps_root_files_and_does_not_follow_symlinks(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    outside = _folder(tmp_path, "outside", 90)
    (root / "link").symlink_to(outside, target_is_directory=True)
    note = root / "README"
    note.write_text("keep", encoding="utf-8")
    os.utime(note, (NOW - 90 * DAY, NOW - 90 * DAY))
    inner = _folder(root, "scheduled__old", 90)
    (inner / "escape").symlink_to(outside, target_is_directory=True)

    result = prune_work(root, "current", 7, now=NOW)

    assert result.removed == 1
    assert not inner.exists()
    assert note.exists()
    assert (root / "link").is_symlink()
    assert (outside / "dbt-target" / "run_results.json").exists()


def test_retention_honours_the_period(tmp_path: Path) -> None:
    _folder(tmp_path, "a", 3)
    assert prune_work(tmp_path, "current", 7, now=NOW).removed == 0
    assert prune_work(tmp_path, "current", 2, now=NOW).removed == 1


def test_retention_must_be_positive(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        prune_work(tmp_path, "current", 0, now=NOW)


def test_step_reads_the_retention_from_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old = _folder(tmp_path, "scheduled__old", 8)
    monkeypatch.setattr(
        "open_data_platform.lakehouse.work_retention.time.time", lambda: NOW
    )
    work = tmp_path / "manual__now"

    assert (
        medallion_build.run_step("prune-work", work, {"ODP_WORK_RETENTION_DAYS": "10"})
        is False
    )
    assert old.exists()
    assert medallion_build.run_step("prune-work", work, {}) is True
    assert not old.exists()
    assert work.is_dir()


@pytest.mark.parametrize("value", ["0", "-1", "7d", "1.5", "\u00b2"])
def test_step_rejects_a_bad_retention(tmp_path: Path, value: str) -> None:
    with pytest.raises(MedallionBuildError, match="ODP_WORK_RETENTION_DAYS"):
        medallion_build.run_step(
            "prune-work", tmp_path / "r1", {"ODP_WORK_RETENTION_DAYS": value}
        )


def test_a_folder_that_cannot_be_removed_does_not_stop_the_others(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stuck = _folder(tmp_path, "a_stuck", 30)
    other = _folder(tmp_path, "b_other", 30)
    real_rmtree = __import__("shutil").rmtree

    def rmtree(path: Path) -> None:
        if Path(path) == stuck:
            raise PermissionError("denied")
        real_rmtree(path)

    monkeypatch.setattr(
        "open_data_platform.lakehouse.work_retention.shutil.rmtree", rmtree
    )
    result = prune_work(tmp_path, "current", 7, now=NOW)

    assert result.failed == ("a_stuck",)
    assert result.removed == 1
    assert stuck.exists()
    assert not other.exists()


def test_step_fails_after_pruning_when_a_folder_is_left(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stuck = _folder(tmp_path, "a_stuck", 30)
    other = _folder(tmp_path, "b_other", 30)
    real_rmtree = __import__("shutil").rmtree

    def rmtree(path: Path) -> None:
        if Path(path) == stuck:
            raise PermissionError("denied")
        real_rmtree(path)

    monkeypatch.setattr(
        "open_data_platform.lakehouse.work_retention.shutil.rmtree", rmtree
    )
    monkeypatch.setattr(
        "open_data_platform.lakehouse.work_retention.time.time", lambda: NOW
    )
    with pytest.raises(MedallionBuildError, match="could not remove 1 run folders"):
        medallion_build.run_step("prune-work", tmp_path / "manual__now", {})
    assert not other.exists()
