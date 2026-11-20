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
from airflow.sdk import Param

REPO = "/opt/odp/repo"

# Empty uses the Airflow run id, so a new run keeps the previous raw object.
# A non-empty value overwrites the keys of that execution.
EXECUTION_ID = "{{ params.execution_id or run_id }}"

# Seconds between SIGTERM and SIGKILL for the child process group.
KILL_GRACE_SECONDS = 10.0

DEFAULT_ARGS: dict[str, object] = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def run_python_args(*args: object) -> None:
    """Run ``python`` with ``args`` and fail when the process exits non-zero.

    An exception while waiting (Airflow timeout, SIGTERM, or any other) and a
    normal exit both end with the child's process group stopped, so Spark,
    Java, and dbt descendants do not outlive the task.
    """
    command = ["python", *(str(arg) for arg in args)]
    process = subprocess.Popen(command, cwd=REPO, start_new_session=True)
    try:
        returncode = process.wait()
    finally:
        _stop_process_group(process)
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


def python_task(task_id: str, op_args: list[str]) -> PythonOperator:
    """Return a task that runs ``python`` with the rendered argument vector."""
    return PythonOperator(
        task_id=task_id,
        python_callable=run_python_args,
        op_args=op_args,
        show_return_value_in_logs=False,
    )
