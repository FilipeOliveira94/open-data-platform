"""Smoke DAG for the local scheduler.

The task prints one fixed line so the executor writes a task log. There is
no package import, no schedule, and no SQL. Ingestion and transforms stay
in the Python package. Daily and weekly schedules are later issues.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="platform_health",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
):
    BashOperator(task_id="ready", bash_command="echo platform_health ready")
