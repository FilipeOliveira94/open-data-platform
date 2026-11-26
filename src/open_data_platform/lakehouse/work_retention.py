"""Retention for the per-run scratch folders under the work root (P3-10).

Each ``medallion_build`` call writes its scratch to ``<work root>/<run label>``
(``/opt/odp/work`` on the ``odp_lakehouse_work`` volume): rendered Spark SQL,
Bronze count files, local staging copies, and the dbt ``target/`` and log
folders. No step reads another label's folder, so a finished run's folder is
only kept for inspection.

``prune_work`` removes every run folder whose newest entry is older than the
retention period. It never touches the current run's folder, plain files at
the root, or anything outside the root. It does not touch Airflow DAG runs,
task instances, Airflow task logs, Iceberg tables, raw objects, or warehouse
staging keys: none of them live under the work root.
"""

from __future__ import annotations

import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

DEFAULT_RETENTION_DAYS = 7
_DAY_SECONDS = 86_400


@dataclass(frozen=True)
class PruneResult:
    removed: int
    kept: int
    bytes_freed: int


def _newest_mtime_and_size(folder: Path) -> tuple[float, int]:
    """Newest mtime and total file size in ``folder``; symlinks are not followed."""
    newest = folder.lstat().st_mtime
    size = 0
    for root, dirs, files in os.walk(folder, followlinks=False):
        for name in (*dirs, *files):
            info = (Path(root) / name).lstat()
            newest = max(newest, info.st_mtime)
            if name in files:
                size += info.st_size
    return newest, size


def prune_work(
    work_root: Path,
    current_label: str,
    retention_days: int,
    *,
    now: float | None = None,
) -> PruneResult:
    """Delete run folders under ``work_root`` older than ``retention_days``."""
    if retention_days < 1:
        msg = "retention days must be at least 1"
        raise ValueError(msg)
    cutoff = (time.time() if now is None else now) - retention_days * _DAY_SECONDS
    removed = kept = freed = 0
    for entry in sorted(work_root.iterdir()):
        if entry.name == current_label or entry.is_symlink() or not entry.is_dir():
            continue
        newest, size = _newest_mtime_and_size(entry)
        if newest >= cutoff:
            kept += 1
            continue
        shutil.rmtree(entry)
        removed += 1
        freed += size
    return PruneResult(removed=removed, kept=kept, bytes_freed=freed)
