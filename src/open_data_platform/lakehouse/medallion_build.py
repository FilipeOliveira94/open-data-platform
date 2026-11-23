"""Run one medallion step inside the platform runtime image.

``make live-bronze``, ``make live-silver``, ``make live-gold``, the dbt Make
targets, and the Airflow transform DAGs all call this module. Each step
renders SQL with the layer module, runs the mounted Spark script once per
file, or runs dbt. The check steps compare Trino results with the expected
values that the layer modules already compute.

Since P3-13 (ADR-0024) the write steps are incremental. ``bronze`` promotes
only new raw objects, ``silver`` rebuilds only the datasets pending for it,
and ``stage-quarantine`` and ``dbt-build`` run only while Gold is pending.
A step with nothing to do writes no table, prints that, and exits with
``--skip-exit-code`` (0 by default; the Airflow DAG maps its code to a
skipped task). ``--full-rebuild`` rebuilds every table from all complete raw
objects; it is for a contract version change or recovery, never scheduled.

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
import time
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from open_data_platform.lakehouse.promotion_registry import (
        OpenCheck,
        PromotionRegistry,
    )

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
    "dbt-unit",
)


class MedallionBuildError(RuntimeError):
    """A step failed. The message names the step, not record values."""


ProcessRunner = Callable[[Sequence[str], Mapping[str, str]], int]


def parse_dataset_names(values: Sequence[str]) -> frozenset[tuple[str, str]]:
    """``source.dataset`` arguments as identities; reject any other shape."""
    names: set[tuple[str, str]] = set()
    for value in values:
        source, dot, dataset = value.partition(".")
        if not dot or not source or not dataset or "." in dataset:
            msg = f"expected SOURCE.DATASET, got {value!r}"
            raise MedallionBuildError(msg)
        names.add((source, dataset))
    return frozenset(names)


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


def spark_runner(
    script: str,
    env: Mapping[str, str],
    run_command: ProcessRunner = run_process,
) -> Callable[[Path], None]:
    """Return a callable that runs ``script`` for one SQL file or raises."""
    spark_dir = env.get("ODP_SPARK_DIR", DEFAULT_SPARK_DIR)

    def run(path: Path) -> None:
        status = run_command(
            ["bash", f"{spark_dir}/{script}"],
            {**env, "ODP_SPARK_SQL": str(path)},
        )
        if status != 0:
            msg = f"spark {script} failed for {path.name} with status {status}"
            raise MedallionBuildError(msg)

    return run


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
    """Return the dbt argument vector for a ``parse``, ``build``, ``reconcile``,
    or ``unit`` action.

    ``build`` excludes reconciliation and dbt unit tests: a logic test on a
    fixture does not gate promotion of live Gold. ``unit`` runs them alone.
    """
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
        return [
            *base,
            "build",
            *tail,
            "--exclude",
            "tag:reconciliation",
            "test_type:unit",
        ]
    if action == "reconcile":
        return [*base, "test", *tail, "--select", "tag:reconciliation"]
    if action == "unit":
        return [*base, "test", *tail, "--select", "test_type:unit"]
    msg = f"unknown dbt action: {action}"
    raise MedallionBuildError(msg)


def _call(main: Callable[[list[str]], int], argv: list[str], step: str) -> None:
    status = main(argv)
    if status != 0:
        msg = f"{step} exited with status {status}"
        raise MedallionBuildError(msg)


def _registry(env: Mapping[str, str]) -> PromotionRegistry:
    from open_data_platform.lakehouse.promotion_registry import (
        PostgresPromotionRegistry,
    )

    return PostgresPromotionRegistry(env)


def run_step(
    step: str,
    work: Path,
    env: Mapping[str, str],
    run_command: ProcessRunner = run_process,
    *,
    full_rebuild: bool = False,
    registry: PromotionRegistry | None = None,
    allow_volume_drop: frozenset[tuple[str, str]] = frozenset(),
) -> bool:
    """Run one named step with ``work`` as the scratch directory.

    Return ``False`` when the step had nothing to do and wrote nothing.
    ``allow_volume_drop`` applies to the ``silver`` step only.
    """
    from open_data_platform.lakehouse.promotion_registry import RegistryError
    from open_data_platform.lakehouse.refresh import RefreshError

    work.mkdir(parents=True, exist_ok=True)
    try:
        return _run_step(
            step, work, env, run_command, full_rebuild, registry, allow_volume_drop
        )
    except (RefreshError, RegistryError) as exc:
        raise MedallionBuildError(f"{step}: {exc}") from exc


def _bronze_checks(
    work: Path, env: Mapping[str, str], registry: PromotionRegistry
) -> bool:
    """Run every open Bronze check; record each result; fail if any failed.

    The expected count is the registry row total stored with the write that
    opened the check, so a check left open by an earlier run runs again
    without that run's work dir.
    """
    from open_data_platform.contracts.load import load_contracts
    from open_data_platform.lakehouse.promotion_registry import BRONZE
    from open_data_platform.lakehouse.reconciliation import bronze_count_checks

    contracts = load_contracts()
    open_checks = registry.open_checks(BRONZE)
    if not open_checks:
        print("bronze checks: no bronze check is open")
        return False

    def checks_for(dataset: tuple[str, str]) -> Sequence[tuple[str, str]]:
        expected = open_checks[dataset].expected_rows
        if expected is None:
            msg = f"bronze check of {dataset[0]}.{dataset[1]} has no expected count"
            raise MedallionBuildError(msg)
        return bronze_count_checks({f"{dataset[0]}.{dataset[1]}": expected}, contracts)

    return _record_checks(BRONZE, open_checks, checks_for, work.name, env, registry)


def _silver_checks(
    work: Path, env: Mapping[str, str], registry: PromotionRegistry
) -> bool:
    """Run every open Silver check; record each result; fail if any failed."""
    from open_data_platform.lakehouse.promotion_registry import SILVER
    from open_data_platform.lakehouse.reconciliation import silver_live_checks
    from open_data_platform.lakehouse.silver import cli as silver

    open_checks = registry.open_checks(SILVER)
    if not open_checks:
        print("silver checks: no silver check is open")
        return False
    entries = {
        (entry[0].source_id, entry[0].dataset): entry
        for entry in silver.selected_entries(
            source_id=None, datasets=(), all_sources=True
        )
    }

    def checks_for(dataset: tuple[str, str]) -> Sequence[tuple[str, str]]:
        if dataset not in entries:
            msg = f"silver check of {dataset[0]}.{dataset[1]} has no silver contract"
            raise MedallionBuildError(msg)
        return silver_live_checks((entries[dataset],))

    return _record_checks(SILVER, open_checks, checks_for, work.name, env, registry)


def _record_checks(
    layer: str,
    open_checks: Mapping[tuple[str, str], OpenCheck],
    checks_for: Callable[[tuple[str, str]], Sequence[tuple[str, str]]],
    run_label: str,
    env: Mapping[str, str],
    registry: PromotionRegistry,
) -> bool:
    failed: list[str] = []
    passed = 0
    for dataset in sorted(open_checks):
        name = f"{dataset[0]}.{dataset[1]}"
        try:
            count = run_checks(
                checks_for(dataset), lambda statement: trino_scalar(statement, env)
            )
        except MedallionBuildError:
            registry.record_check(
                layer,
                dataset,
                open_checks[dataset].marked_at,
                passed=False,
                run_label=run_label,
            )
            print(f"{layer} check failed: {name}", file=sys.stderr, flush=True)
            failed.append(name)
            continue
        registry.record_check(
            layer,
            dataset,
            open_checks[dataset].marked_at,
            passed=True,
            run_label=run_label,
        )
        passed += count
        print(f"{layer} check passed: {name} ({count} checks)", flush=True)
    if failed:
        msg = f"{layer} checks failed for {', '.join(failed)}"
        raise MedallionBuildError(msg)
    print(f"{layer} checks passed: {passed}")
    return True


def _run_step(
    step: str,
    work: Path,
    env: Mapping[str, str],
    run_command: ProcessRunner,
    full_rebuild: bool,
    registry: PromotionRegistry | None,
    allow_volume_drop: frozenset[tuple[str, str]] = frozenset(),
) -> bool:
    from open_data_platform.lakehouse import refresh

    if step == "ensure-catalog":
        ensure_catalog_database(env)
        return True
    if step == "bronze":
        from open_data_platform.contracts.load import load_contracts
        from open_data_platform.object_storage import client_from_env

        ensure_catalog_database(env)
        registry = registry or _registry(env)
        result = refresh.refresh_bronze(
            client_from_env(env),
            contracts=load_contracts(),
            registry=registry,
            run_label=work.name,
            work=work,
            raw_bucket=env["ODP_RAW_BUCKET"],
            warehouse_bucket=env["ODP_WAREHOUSE_BUCKET"],
            run_spark=spark_runner("run_bronze.sh", env, run_command),
            full_rebuild=full_rebuild,
        )
        if result.wrote:
            return True
        # Nothing new in raw. Later layers or their checks may still be open
        # after a failed run, so the step is skipped only when they are not.
        from open_data_platform.lakehouse.promotion_registry import SILVER

        if (
            registry.pending(SILVER)
            or refresh.gold_mark(registry) is not None
            or refresh.open_checks_summary(registry)
        ):
            print("bronze: no new raw objects; downstream work is pending")
            return True
        print("bronze: no new raw objects and nothing pending; no table written")
        return False
    if step == "bronze-checks":
        return _bronze_checks(work, env, registry or _registry(env))
    if step == "silver":
        from open_data_platform.lakehouse.promotion_registry import SILVER

        ensure_catalog_database(env)
        registry = registry or _registry(env)
        done = refresh.refresh_silver(
            registry=registry,
            run_label=work.name,
            work=work,
            warehouse_bucket=env["ODP_WAREHOUSE_BUCKET"],
            run_spark=spark_runner("run_silver.sh", env, run_command),
            full_rebuild=full_rebuild,
            allow_volume_drop=allow_volume_drop,
        )
        if done:
            return True
        # A Silver check left open by an earlier run still has to run, and
        # the check task only runs after this one succeeds.
        if registry.open_checks(SILVER):
            print("silver: no dataset pending; silver checks are open")
            return True
        print("silver: no dataset pending; no table written")
        return False
    if step == "silver-checks":
        return _silver_checks(work, env, registry or _registry(env))
    if step == "stage-quarantine":
        from open_data_platform.lakehouse import quarantine_stage

        registry = registry or _registry(env)
        if not full_rebuild and refresh.gold_mark(registry) is None:
            print("stage-quarantine: Gold is not pending; no table written")
            return False
        refresh.require_gold_inputs_checked(registry)
        ensure_catalog_database(env)
        sql_dir = work / "quarantine-sql"
        _call(
            quarantine_stage.main,
            ["--sql-out", str(sql_dir), "--run-label", work.name],
            step,
        )
        run_spark_scripts(sql_dir, "run_bronze.sh", env, run_command)
        return True
    if step == "dbt-build":
        registry = registry or _registry(env)
        marked = refresh.gold_mark(registry)
        if not full_rebuild and marked is None:
            print("dbt-build: Gold is not pending; no model built")
            return False
        refresh.require_gold_inputs_checked(registry)
        _run_dbt(step, work, env, run_command)
        refresh.complete_gold(registry, work.name, marked)
        return True
    if step in {"dbt-parse", "dbt-reconcile", "dbt-unit"}:
        _run_dbt(step, work, env, run_command)
        return True
    msg = f"unknown step: {step}"
    raise MedallionBuildError(msg)


def _run_dbt(
    step: str, work: Path, env: Mapping[str, str], run_command: ProcessRunner
) -> None:
    command = dbt_command(step.removeprefix("dbt-"), work, env)
    status = run_command(command, env)
    if status != 0:
        msg = f"{step} exited with status {status}"
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
        help=(
            "Subdirectory under --work-root and the staging key segment. "
            "Airflow passes the run id."
        ),
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help=(
            "Rebuild every table from all complete raw objects and run Gold. "
            "For a contract version change or recovery; never scheduled."
        ),
    )
    parser.add_argument(
        "--allow-volume-drop",
        action="append",
        default=[],
        metavar="SOURCE.DATASET",
        help=(
            "Skip the Silver volume guard of one snapshot or full_refresh "
            "dataset for this run, to accept a real shrink. Repeatable."
        ),
    )
    parser.add_argument(
        "--skip-exit-code",
        type=int,
        default=0,
        help="Exit status when the step had nothing to do (Airflow passes 99).",
    )
    args = parser.parse_args(argv)
    try:
        work = run_dir(args.work_root, args.run_label)
        started = time.monotonic()
        wrote = run_step(
            args.step,
            work,
            os.environ,
            full_rebuild=args.full_rebuild,
            allow_volume_drop=parse_dataset_names(args.allow_volume_drop),
        )
        print(
            f"step {args.step}: {'ran' if wrote else 'skipped'} "
            f"in {time.monotonic() - started:.1f}s",
            flush=True,
        )
    except MedallionBuildError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyError as exc:
        print(f"missing environment variable: {exc}", file=sys.stderr)
        return 1
    return 0 if wrote else args.skip_exit_code


if __name__ == "__main__":
    sys.exit(main())
