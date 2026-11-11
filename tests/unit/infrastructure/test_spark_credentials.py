"""Spark entrypoints keep secrets off the process argument list."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SPARK = ROOT / "docker" / "spark"
CREDENTIALS = SPARK / "credentials.sh"
RUNNERS = tuple(sorted(SPARK.glob("run_*.sh")))
_SECRET_CONFS = (
    "spark.sql.catalog.local.jdbc.password=",
    "spark.hadoop.fs.s3a.access.key=",
    "spark.hadoop.fs.s3a.secret.key=",
)


def test_secret_assignments_stay_in_the_properties_writer() -> None:
    """A new shell script under docker/spark must not grow its own --conf secret."""
    offenders: list[str] = []
    for path in sorted(SPARK.glob("*.sh")):
        if path.name == "credentials.sh":
            continue
        text = path.read_text(encoding="utf-8")
        for token in _SECRET_CONFS:
            if token in text:
                offenders.append(f"{path.name}: {token}")
    assert offenders == []


def test_runners_load_secrets_from_a_properties_file() -> None:
    assert RUNNERS
    for path in RUNNERS:
        script = path.read_text(encoding="utf-8")
        assert "credentials.sh" in script
        assert '--properties-file "${SECRET_PROPS}"' in script
        assert "spark.redaction.regex=(?i)secret|password|token|access[.]key" in script
        assert "SimpleAWSCredentialsProvider" in script
        for conf in _SECRET_CONFS:
            assert conf not in script


def test_property_escape_keeps_one_line() -> None:
    completed = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; odp_escape_property "$2"',
            "bash",
            str(CREDENTIALS),
            "p@ss\\word\nline",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout == "p@ss\\\\word\\nline"


def test_secret_properties_file_is_private_and_not_echoed(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.update(
        {
            "TMPDIR": str(tmp_path),
            "MINIO_ROOT_USER": "user",
            "MINIO_ROOT_PASSWORD": "line1\nline2",
            "POSTGRES_USER": "odp",
            "POSTGRES_PASSWORD": "db-pass",
        }
    )
    completed = subprocess.run(
        [
            "bash",
            "-c",
            'set -u; source "$1"; odp_write_secret_properties jdbc',
            "bash",
            str(CREDENTIALS),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert "db-pass" not in completed.stdout
    assert "line1" not in completed.stdout
    path = Path(completed.stdout.strip())
    assert path.parent == tmp_path
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    text = path.read_text(encoding="utf-8")
    assert "spark.sql.catalog.local.jdbc.password=db-pass\n" in text
    assert "spark.hadoop.fs.s3a.access.key=user\n" in text
    assert "spark.hadoop.fs.s3a.secret.key=line1\\nline2\n" in text


def test_s3_properties_omit_the_jdbc_password(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.pop("POSTGRES_PASSWORD", None)
    env.update(
        {
            "TMPDIR": str(tmp_path),
            "MINIO_ROOT_USER": "user",
            "MINIO_ROOT_PASSWORD": "s3-secret",
        }
    )
    completed = subprocess.run(
        [
            "bash",
            "-c",
            'set -u; source "$1"; odp_write_secret_properties s3',
            "bash",
            str(CREDENTIALS),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    text = Path(completed.stdout.strip()).read_text(encoding="utf-8")
    assert "jdbc.password" not in text
    assert "spark.hadoop.fs.s3a.secret.key=s3-secret\n" in text
