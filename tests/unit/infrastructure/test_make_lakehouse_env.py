"""Lakehouse Make recipes keep the sourced env in the shell that does the work."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]

_MODULE = "open_data_platform.lakehouse.medallion_build"

# Marker that must sit in the same recipe shell as `. ./.env`.
_SOURCED_WORK = {
    "spark-ivy-warm": "warm_ivy.sh",
    "live-bronze": f"{_MODULE} bronze-checks",
    "live-silver": f"{_MODULE} silver-checks",
    "live-refresh": f"{_MODULE} dbt-build",
    "stage-quarantine": f"{_MODULE} stage-quarantine",
    "dbt-build": f"{_MODULE} dbt-build",
    "dbt-test-reconciliation": f"{_MODULE} dbt-reconcile",
}


def _recipe_shells(target: str) -> list[str]:
    completed = subprocess.run(
        ["make", "-n", target],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    shells: list[str] = []
    current: list[str] = []
    for line in completed.stdout.splitlines():
        stripped = line.rstrip()
        if stripped.endswith("\\"):
            current.append(stripped[:-1])
            continue
        current.append(stripped)
        shells.append("\n".join(current))
        current = []
    if current:
        shells.append("\n".join(current))
    return shells


def test_sourced_env_reaches_the_lakehouse_command() -> None:
    """A newline inside ensure_spark_ivy must not split source from the command."""
    for target, marker in _SOURCED_WORK.items():
        sourced = [shell for shell in _recipe_shells(target) if ". ./.env" in shell]
        assert sourced, target
        assert any(marker in shell for shell in sourced), target


def test_ivy_chmod_does_not_abort_the_bronze_recipe() -> None:
    """Jars owned by the Spark uid can make chmod fail; live-bronze uses set -e."""
    sourced = [shell for shell in _recipe_shells("live-bronze") if ". ./.env" in shell]
    assert len(sourced) == 1
    shell = sourced[0]
    assert "set -e" in shell
    assert 'chmod -R a+rwX ".local/spark-ivy" || true' in shell


def test_make_uses_one_unique_run_label_per_invocation() -> None:
    shell = next(s for s in _recipe_shells("live-refresh") if ". ./.env" in s)
    labels = {part.split()[0] for part in shell.split("--run-label ")[1:]}
    assert len(labels) == 1
    (label,) = labels
    assert label.startswith("make-") and label != "make"


def test_live_rebuild_passes_full_rebuild() -> None:
    completed = subprocess.run(
        ["make", "-n", "live-rebuild"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "--full-rebuild" in completed.stdout
