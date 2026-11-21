"""Task defaults for thin DAG wrappers (ingestion and transform).

``run_python_args`` executes ``python`` with an argument vector. The
arguments are not passed through a shell. The child starts in its own
session, and the whole process group is stopped when the task ends, times
out, or is killed. This module is not a DAG. ``airflow/dags/.airflowignore``
keeps the dag processor from parsing it.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from datetime import timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset, Param
from airflow.sdk.exceptions import AirflowSkipException

REPO = "/opt/odp/repo"

# Empty uses the Airflow run id, so a new run keeps the previous raw object.
# A non-empty value overwrites the keys of that execution.
EXECUTION_ID = "{{ params.execution_id or run_id }}"

# Three DAGs extract the same PostgreSQL source, and Airflow run ids repeat
# across DAGs: the daily and weekly runs at Monday 00:00Z are both
# ``scheduled__<W>T00:00:00+00:00``, and a backfill of each gives
# ``backfill__<date>``. The same execution id puts both extracts in one raw
# prefix: the second clears the first's ``_COMPLETE`` marker and lists only its
# own object, so Bronze sees the prefix as incomplete. The DAG id keeps the
# executions apart (P3-06).
POSTGRES_EXECUTION_ID = "{{ params.execution_id or (dag.dag_id ~ '__' ~ run_id) }}"

# One slot: tasks that write lakehouse tables and the reconciliation tests
# never run at the same time. Compose ``airflow-init`` creates the pool.
LAKEHOUSE_GOLD_POOL = "lakehouse_gold"

POSTGRES_OLTP = "open_data_platform.ingestion.postgres_oltp"

# ``medallion_build --skip-exit-code``: a step with nothing to do exits with
# this status and the task ends as skipped (ADR-0024).
SKIP_EXIT_CODE = 99

# Raw sources that ingestion DAGs land. Each one is an Airflow Asset; a
# successful ingestion emits an event for the sources it wrote, and
# ``transform_lakehouse`` is scheduled on any of them (ADR-0024).
RAW_SOURCES = (
    "demo_storefront",
    "dummyjson_products",
    "ibge_localidades",
    "olist_brazilian_ecommerce",
    "olist_marketing_funnel",
    "postgres_oltp",
)


def raw_asset(source_id: str) -> Asset:
    """The Asset that stands for new raw objects of one source."""
    if source_id not in RAW_SOURCES:
        msg = f"unknown raw source: {source_id}"
        raise ValueError(msg)
    return Asset(name=f"raw_{source_id}", uri=f"odp-raw://{source_id}")


# Seconds between SIGTERM and SIGKILL for the child process group.
KILL_GRACE_SECONDS = 10.0

DEFAULT_ARGS: dict[str, object] = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def run_python_args(*args: object, skip_exit_code: int | None = None) -> None:
    """Run ``python`` with ``args`` and fail when the process exits non-zero.

    An exception while waiting (Airflow timeout, SIGTERM, or any other) and a
    normal exit both end with the child's process group stopped, so Spark,
    Java, and dbt descendants do not outlive the task. ``skip_exit_code``
    turns that status into a skipped task.
    """
    command = ["python", *(str(arg) for arg in args)]
    process = subprocess.Popen(command, cwd=REPO, start_new_session=True)
    try:
        returncode = process.wait()
    finally:
        _stop_process_group(process)
    if skip_exit_code is not None and returncode == skip_exit_code:
        msg = "entrypoint had nothing to do"
        raise AirflowSkipException(msg)
    if returncode != 0:
        msg = f"entrypoint exited with status {returncode}"
        raise RuntimeError(msg)


def _stop_process_group(process: subprocess.Popen[bytes]) -> None:
    """SIGTERM the child's group, then SIGKILL what is left after the grace."""
    pgid = process.pid
    if not _signal_group(pgid, signal.SIGTERM):
        process.wait()
        return
    deadline = time.monotonic() + KILL_GRACE_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None and not _signal_group(pgid, 0):
            return
        time.sleep(0.1)
    _signal_group(pgid, signal.SIGKILL)
    process.wait()


def _signal_group(pgid: int, sig: int) -> bool:
    """Send ``sig`` to the group. Return False when the group is gone."""
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        return False
    return True


def execution_id_param() -> Param:
    """Return the optional raw execution id."""
    return Param(
        "",
        type="string",
        description=(
            "Raw execution id. Empty uses the Airflow run id, so a new run "
            "keeps the previous object. A repeated id overwrites that object."
        ),
    )


def python_task(
    task_id: str,
    op_args: list[str],
    *,
    pool: str | None = None,
    outlets: list[Asset] | None = None,
    skip_exit_code: int | None = None,
    trigger_rule: str | None = None,
) -> PythonOperator:
    """Return a task that runs ``python`` with the rendered argument vector."""
    extra: dict[str, object] = {}
    if pool is not None:
        extra["pool"] = pool
    if outlets:
        extra["outlets"] = outlets
    if trigger_rule is not None:
        extra["trigger_rule"] = trigger_rule
    return PythonOperator(
        task_id=task_id,
        python_callable=run_python_args,
        op_args=op_args,
        op_kwargs={"skip_exit_code": skip_exit_code},
        show_return_value_in_logs=False,
        **extra,
    )


def postgres_extract_task(
    window_start: str, window_end: str, frequency: str
) -> PythonOperator:
    """Return the P1-06 extract task for the given (templated) window bounds.

    ``frequency`` (``daily``, ``weekly``, or ``manual``) is recorded with the
    DAG id and run id in the coverage ledger (P3-06). A successful extract
    emits the ``postgres_oltp`` raw Asset.
    """
    return python_task(
        "extract",
        [
            "-m",
            POSTGRES_OLTP,
            "extract",
            "--window-start",
            window_start,
            "--window-end",
            window_end,
            "--execution-id",
            POSTGRES_EXECUTION_ID,
            "--frequency",
            frequency,
            "--dag-id",
            "{{ dag.dag_id }}",
            "--airflow-run-id",
            "{{ run_id }}",
        ],
        outlets=[raw_asset("postgres_oltp")],
    )
