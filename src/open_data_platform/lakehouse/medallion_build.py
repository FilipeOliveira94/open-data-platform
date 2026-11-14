"""Run one medallion step inside the platform runtime image.

``make live-bronze``, ``make live-silver``, ``make live-gold``, the dbt Make
targets, and the Airflow transform DAGs all call this module. Each step
renders SQL with the layer module, runs the mounted Spark script once per
file, or runs dbt. The check steps compare Trino results with the expected
values that the layer modules already compute.

The process runs in the ``odp-runtime`` image (Airflow, Spark, and dbt in one
image). Credentials come from the container environment. They are never
command arguments. Output names steps, files, and counts. It does not print
record values.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

DEFAULT_SPARK_DIR = "/opt/odp/spark"
DEFAULT_DBT_BIN = "/opt/dbt-venv/bin/dbt"
DEFAULT_REPO = "/opt/odp/repo"

_IDENTIFIER = re.compile(r"^[a-z0-9_]+$")
_RUN_LABEL = re.compile(r"[^A-Za-z0-9._-]")

STEPS = (
    "ensure-catalog",
    "bronze",
    "bronze-checks",
    "silver",
    "silver-checks",
    "stage-quarantine",
    "dbt-parse",
    "dbt-build",
    "dbt-reconcile",
)


class MedallionBuildError(RuntimeError):
    """A step failed. The message names the step, not record values."""


ProcessRunner = Callable[[Sequence[str], Mapping[str, str]], int]


def run_process(command: Sequence[str], env: Mapping[str, str]) -> int:
    """Run ``command`` without a shell and return the exit status."""
    return subprocess.run(list(command), env=dict(env), check=False).returncode


def run_dir(work_root: str | Path, run_label: str) -> Path:
    """Return ``work_root/<label>`` with unsafe path characters replaced."""
    label = _RUN_LABEL.sub("_", run_label).strip("._")
    if not label:
        msg = "run label is empty"
        raise MedallionBuildError(msg)
    return Path(work_root) / label


def catalog_db(env: Mapping[str, str]) -> str:
    """Return ``ICEBERG_CATALOG_DB`` after the simple-identifier check."""
    name = env.get("ICEBERG_CATALOG_DB", "iceberg")
    if not _IDENTIFIER.fullmatch(name):
        msg = "ICEBERG_CATALOG_DB must be a simple identifier"
        raise MedallionBuildError(msg)
    return name


def ensure_catalog_database(env: Mapping[str, str]) -> None:
    """Create the Iceberg JDBC catalog database when it is missing."""
    import psycopg
    from psycopg import sql

    name = catalog_db(env)
    with psycopg.connect(
        host=env.get("POSTGRES_HOST", "postgres"),
        port=int(env.get("POSTGRES_PORT", "5432")),
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
        dbname="postgres",
        autocommit=True,
    ) as conn:
        found = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (name,)
        ).fetchone()
        if found is None:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            print(f"catalog database created: {name}")
        else:
            print(f"catalog database present: {name}")


def run_spark_scripts(
    sql_dir: Path,
    script: str,
    env: Mapping[str, str],
    run_command: ProcessRunner = run_process,
) -> int:
    """Run ``script`` once per ``*.sql`` file in name order. Return the count."""
    files = sorted(sql_dir.glob("*.sql"))
    if not files:
        msg = f"no Spark SQL files in {sql_dir}"
        raise MedallionBuildError(msg)
    spark_dir = env.get("ODP_SPARK_DIR", DEFAULT_SPARK_DIR)
    for path in files:
        print(f"spark {script}: {path.name}", flush=True)
        status = run_command(
            ["bash", f"{spark_dir}/{script}"],
            {**env, "ODP_SPARK_SQL": str(path)},
        )
        if status != 0:
            msg = f"spark {script} failed for {path.name} with status {status}"
            raise MedallionBuildError(msg)
    return len(files)


def trino_scalar(statement: str, env: Mapping[str, str]) -> str:
    """Return the first column of the last row of ``statement`` as text.

    Uses the Trino HTTP protocol (``/v1/statement``) with the standard library.
    The value is the same text the Trino CLI prints, without quotes.
    """
    host = env.get("ODP_TRINO_HOST", "trino")
    port = env.get("ODP_TRINO_PORT", "8080")
    user = env.get("ODP_TRINO_USER", "odp")
    request = urllib.request.Request(
        f"http://{host}:{port}/v1/statement",
        data=statement.encode("utf-8"),
        headers={"X-Trino-User": user},
        method="POST",
    )
    last: object = None
    while True:
        with urllib.request.urlopen(request, timeout=300) as response:
            payload = json.loads(response.read().decode("utf-8"))
        error = payload.get("error")
        if error:
            name = error.get("errorName", "UNKNOWN")
            msg = f"trino query failed: {name}"
            raise MedallionBuildError(msg)
        for row in payload.get("data") or ():
            last = row[0] if row else None
        next_uri = payload.get("nextUri")
        if not next_uri:
            break
        request = urllib.request.Request(next_uri, headers={"X-Trino-User": user})
    return "" if last is None else str(last)


def run_checks(
    checks: Sequence[tuple[str, str]],
    query: Callable[[str], str],
) -> int:
    """Compare each expected value with the Trino result. Return the count."""
    if not checks:
        msg = "no live checks"
        raise MedallionBuildError(msg)
    for expected, statement in checks:
        got = query(statement)
        if got != expected:
            print("live check failed", file=sys.stderr)
            print(statement, file=sys.stderr)
            print(f"expected {expected}", file=sys.stderr)
            print(f"got {got}", file=sys.stderr)
            msg = "live check failed"
            raise MedallionBuildError(msg)
        print(f"{statement} -> {got}", flush=True)
    return len(checks)


def dbt_command(
    action: str,
    work: Path,
    env: Mapping[str, str],
) -> list[str]:
    """Return the dbt argument vector for ``parse``, ``build``, or ``reconcile``."""
    repo = env.get("ODP_REPO", DEFAULT_REPO)
    base = [env.get("ODP_DBT_BIN", DEFAULT_DBT_BIN)]
    tail = [
        "--project-dir",
        f"{repo}/dbt",
        "--profiles-dir",
        f"{repo}/dbt",
        "--target-path",
        str(work / "dbt-target"),
        "--log-path",
        str(work / "dbt-logs"),
    ]
    if action == "parse":
        return [*base, "parse", *tail]
    if action == "build":
        return [*base, "build", *tail, "--exclude", "tag:reconciliation"]
    if action == "reconcile":
        return [*base, "test", *tail, "--select", "tag:reconciliation"]
    msg = f"unknown dbt action: {action}"
    raise MedallionBuildError(msg)


def _call(main: Callable[[list[str]], int], argv: list[str], step: str) -> None:
    status = main(argv)
    if status != 0:
        msg = f"{step} exited with status {status}"
        raise MedallionBuildError(msg)


def run_step(
    step: str,
    work: Path,
    env: Mapping[str, str],
    run_command: ProcessRunner = run_process,
) -> None:
    """Run one named step with ``work`` as the scratch directory."""
    work.mkdir(parents=True, exist_ok=True)
    if step == "ensure-catalog":
        ensure_catalog_database(env)
    elif step == "bronze":
        from open_data_platform.lakehouse import bronze

        ensure_catalog_database(env)
        sql_dir = work / "bronze-sql"
        _call(
            bronze.main,
            [
                "--all",
                "--sql-out",
                str(sql_dir),
                "--counts-out",
                str(work / "bronze-source-counts.tsv"),
            ],
            step,
        )
        count = run_spark_scripts(sql_dir, "run_bronze.sh", env, run_command)
        print(f"bronze spark files: {count}")
    elif step == "bronze-checks":
        from open_data_platform.contracts.load import load_contracts
        from open_data_platform.lakehouse.reconciliation import (
            bronze_count_checks,
            parse_counts,
        )

        counts_file = work / "bronze-source-counts.tsv"
        if not counts_file.is_file():
            msg = f"missing count file from the bronze step: {counts_file}"
            raise MedallionBuildError(msg)
        checks = bronze_count_checks(
            parse_counts(counts_file.read_text(encoding="utf-8")),
            load_contracts(),
        )
        count = run_checks(checks, lambda statement: trino_scalar(statement, env))
        print(f"bronze checks passed: {count}")
    elif step == "silver":
        from open_data_platform.lakehouse.silver import cli as silver

        ensure_catalog_database(env)
        sql_dir = work / "silver-sql"
        _call(silver.main, ["--all", "--sql-out", str(sql_dir)], step)
        count = run_spark_scripts(sql_dir, "run_silver.sh", env, run_command)
        print(f"silver spark files: {count}")
    elif step == "silver-checks":
        from open_data_platform.lakehouse.silver import cli as silver

        checks = silver.live_checks()
        count = run_checks(checks, lambda statement: trino_scalar(statement, env))
        print(f"silver checks passed: {count}")
    elif step == "stage-quarantine":
        from open_data_platform.lakehouse import quarantine_stage

        ensure_catalog_database(env)
        sql_dir = work / "quarantine-sql"
        _call(quarantine_stage.main, ["--sql-out", str(sql_dir)], step)
        run_spark_scripts(sql_dir, "run_bronze.sh", env, run_command)
    elif step in {"dbt-parse", "dbt-build", "dbt-reconcile"}:
        command = dbt_command(step.removeprefix("dbt-"), work, env)
        status = run_command(command, env)
        if status != 0:
            msg = f"{step} exited with status {status}"
            raise MedallionBuildError(msg)
    else:
        msg = f"unknown step: {step}"
        raise MedallionBuildError(msg)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run one medallion step in the platform runtime image. "
            "Make and the Airflow transform DAGs call this entrypoint."
        ),
    )
    parser.add_argument("step", choices=STEPS)
    parser.add_argument(
        "--work-root",
        default=os.environ.get("ODP_WORK_ROOT", "/opt/odp/work"),
        help="Scratch root for rendered SQL, counts, and dbt output.",
    )
    parser.add_argument(
        "--run-label",
        required=True,
        help="Subdirectory under --work-root. Airflow passes the run id.",
    )
    args = parser.parse_args(argv)
    try:
        work = run_dir(args.work_root, args.run_label)
        run_step(args.step, work, os.environ)
    except MedallionBuildError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyError as exc:
        print(f"missing environment variable: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
