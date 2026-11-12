"""Extract one PostgreSQL OLTP window into raw storage.

Business logic stays in ``open_data_platform.ingestion.postgres_oltp``.
The caller supplies both window bounds. This DAG does not calculate a
daily or weekly window and it does not seed the tables.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, Param
from ingest_common import DEFAULT_ARGS, EXECUTION_ID, execution_id_param, python_task

with DAG(
    dag_id="ingest_postgres_oltp",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    params={
        "execution_id": execution_id_param(),
        "window_start": Param(
            type="string",
            minLength=1,
            description="Inclusive UTC midnight, ISO-8601.",
        ),
        "window_end": Param(
            type="string",
            minLength=1,
            description="Exclusive UTC midnight, ISO-8601.",
        ),
    },
    doc_md=__doc__,
):
    python_task(
        "extract",
        [
            "-m",
            "open_data_platform.ingestion.postgres_oltp",
            "extract",
            "--window-start",
            "{{ params.window_start }}",
            "--window-end",
            "{{ params.window_end }}",
            "--execution-id",
            EXECUTION_ID,
        ],
    )
