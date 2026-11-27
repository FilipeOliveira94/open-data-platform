"""R1-03: the old PostgreSQL source name survives only in history.

``postgres_oltp`` became ``olist_synth_cdc``, the PostgreSQL schema ``oltp``
became ``olist_synth``, and the Gold domain ``oltp_operations`` became
``olist_synth_operations``. A tracked file that still uses an old name would
write to, or read from, a prefix or table that the migration deleted.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "r1_03_rename_check.sh"

OLD_NAME = re.compile(
    r"postgres_oltp|postgres-oltp|PostgresOltp|POSTGRES_OLTP|(?<![A-Za-z])oltp(?![a-z])"
)

# Dated records keep the names that were true when they were written.
HISTORICAL_FILES = {
    "DECISIONS.md",
    "docs/status-log.md",
    "docs/project-genesis.md",
    "docs/architecture/phase-2-live-run.md",
    "docs/architecture/phase-3-live-run.md",
    "docs/sources/olist-synth-cdc.md",  # the "formerly" line
    "scripts/r1_03_rename_check.sh",  # names the old objects it deletes
    "scripts/r1_03_copy_raw.py",  # copies from the old prefix
    "tests/unit/infrastructure/test_r1_03_rename.py",
}
HISTORICAL_PREFIXES = ("docs/reviews/", "docs/decisions/")
# In these files only the history sections, verified rows, and the R1-03
# entries may keep the old name.
LINE_CHECKED = {"ROADMAP.md", "PROJECT_STATUS.md"}
HISTORY_SECTION = re.compile(r"## (changelog|phase [0-3] )", re.IGNORECASE)


def _tracked() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, check=True, capture_output=True, text=True
    )
    return out.stdout.split()


def _live_lines(path: str, text: str) -> list[tuple[int, str]]:
    lines = text.splitlines()
    if path not in LINE_CHECKED:
        return list(enumerate(lines, 1))
    live: list[tuple[int, str]] = []
    in_history = False
    for number, line in enumerate(lines, 1):
        if line.startswith("## "):
            in_history = bool(HISTORY_SECTION.match(line))
        if in_history or "| Verified |" in line or "R1-03" in line:
            continue
        live.append((number, line))
    return live


def test_no_tracked_file_uses_the_old_source_name() -> None:
    offenders: list[str] = []
    for path in _tracked():
        if path in HISTORICAL_FILES or path.startswith(HISTORICAL_PREFIXES):
            continue
        if OLD_NAME.search(path):
            offenders.append(path)
            continue
        try:
            text = (ROOT / path).read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError):
            continue
        offenders.extend(
            f"{path}:{number}"
            for number, line in _live_lines(path, text)
            if OLD_NAME.search(line)
        )
    assert offenders == []


def _delete(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "R1_03_OUT": str(tmp_path)}
    return subprocess.run(
        ["bash", str(SCRIPT), "delete", *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_delete_refuses_without_a_comparison(tmp_path: Path) -> None:
    result = _delete(tmp_path)
    assert result.returncode == 1
    assert "compare.tsv is missing" in result.stderr


def test_delete_refuses_after_a_failed_comparison(tmp_path: Path) -> None:
    (tmp_path / "compare.tsv").write_text(
        "silver:sellers\t11\t8\tequal\tFAIL\n", encoding="utf-8"
    )
    result = _delete(tmp_path)
    assert result.returncode == 1
    assert "FAIL line" in result.stderr
