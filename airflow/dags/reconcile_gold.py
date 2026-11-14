"""Run the dbt reconciliation tests against Gold once a week.

The task runs ``python -m open_data_platform.lakehouse.medallion_build dbt-reconcile``,
the same entrypoint as ``make dbt-test-reconciliation``. It selects
``tag:reconciliation``. ``transform_lakehouse`` builds Gold and does not run
these tests.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from ingest_common import DEFAULT_ARGS, python_task

with DAG(
    dag_id="reconcile_gold",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
):
    python_task(
        "dbt_reconcile",
        [
            "-m",
            "open_data_platform.lakehouse.medallion_build",
            "dbt-reconcile",
            "--run-label",
            "{{ run_id }}",
        ],
    )
