"""Spark launchers: UTC session time zone and the P3-10 resource caps."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SPARK = ROOT / "docker" / "spark"
RUNNERS = (SPARK / "run_bronze.sh", SPARK / "run_silver.sh")
_LAUNCH = re.compile(r"\bspark-(sql|submit)\b")
_TIME_ZONE = '--conf "spark.sql.session.timeZone=UTC"'


def _launchers() -> list[Path]:
    """Every script under docker/spark that starts a Spark process."""
    return [
        path
        for path in sorted(SPARK.glob("*.sh"))
        if any(
            _LAUNCH.search(line) and not line.lstrip().startswith("#")
            for line in path.read_text(encoding="utf-8").splitlines()
        )
    ]


def test_launchers_are_found_by_pattern() -> None:
    names = {path.name for path in _launchers()}
    assert {"run_bronze.sh", "run_silver.sh", "warm_ivy.sh"} <= names


@pytest.mark.parametrize("path", _launchers(), ids=lambda path: path.name)
def test_every_spark_launcher_sets_the_session_time_zone(path: Path) -> None:
    assert _TIME_ZONE in path.read_text(encoding="utf-8")


@pytest.mark.parametrize("path", RUNNERS, ids=lambda path: path.name)
def test_runners_cap_cores_and_driver_memory(path: Path) -> None:
    script = path.read_text(encoding="utf-8")
    assert '--master "local[${ODP_SPARK_CORES}]"' in script
    assert '--driver-memory "${ODP_SPARK_DRIVER_MEMORY}"' in script
    assert "local[*]" not in script
    assert ': "${ODP_SPARK_CORES:=2}"' in script
    assert ': "${ODP_SPARK_DRIVER_MEMORY:=2g}"' in script


def _run(path: Path, tmp_path: Path, **overrides: str) -> subprocess.CompletedProcess:
    sql = tmp_path / "step.sql"
    sql.write_text("SELECT 1;\n", encoding="utf-8")
    env = {
        "PATH": os.environ["PATH"],
        "MINIO_ROOT_USER": "placeholder",
        "MINIO_ROOT_PASSWORD": "placeholder",
        "ODP_WAREHOUSE_BUCKET": "odp-warehouse",
        "POSTGRES_USER": "placeholder",
        "POSTGRES_PASSWORD": "placeholder",
        "ICEBERG_CATALOG_DB": "iceberg",
        "ODP_SPARK_SQL": str(sql),
        "TMPDIR": str(tmp_path),
        **overrides,
    }
    return subprocess.run(
        ["bash", str(path)], env=env, capture_output=True, text=True, check=False
    )


@pytest.mark.parametrize("path", RUNNERS, ids=lambda path: path.name)
@pytest.mark.parametrize("cores", ["0", "02", "-1", "2x", "*", " "])
def test_runners_reject_a_bad_core_count(
    path: Path, tmp_path: Path, cores: str
) -> None:
    completed = _run(path, tmp_path, ODP_SPARK_CORES=cores)
    assert completed.returncode == 1
    assert "ODP_SPARK_CORES must be a positive integer" in completed.stderr


@pytest.mark.parametrize("path", RUNNERS, ids=lambda path: path.name)
@pytest.mark.parametrize("memory", ["2", "0g", "2G", "1x5g", "g", "2gb", "-2g"])
def test_runners_reject_a_bad_driver_memory(
    path: Path, tmp_path: Path, memory: str
) -> None:
    completed = _run(path, tmp_path, ODP_SPARK_DRIVER_MEMORY=memory)
    assert completed.returncode == 1
    assert "ODP_SPARK_DRIVER_MEMORY must look like" in completed.stderr
