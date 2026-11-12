"""Task defaults for thin ingestion wrappers.

``run_python_args`` executes ``python`` with an argument vector. The
arguments are not passed through a shell. This module is not a DAG.
``airflow/dags/.airflowignore`` keeps the dag processor from parsing it.
"""

from __future__ import annotations

import subprocess
from datetime import timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

REPO = "/opt/odp/repo"

# Empty uses the Airflow run id, so a new run keeps the previous raw object.
# A non-empty value overwrites the keys of that execution.
EXECUTION_ID = "{{ params.execution_id or run_id }}"

DEFAULT_ARGS: dict[str, object] = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def run_python_args(*args: object) -> None:
    """Run ``python`` with ``args`` and fail when the process exits non-zero."""
    command = ["python", *(str(arg) for arg in args)]
    completed = subprocess.run(command, cwd=REPO, check=False)
    if completed.returncode != 0:
        msg = f"entrypoint exited with status {completed.returncode}"
        raise RuntimeError(msg)


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
