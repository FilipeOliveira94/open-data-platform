"""Fetch DummyJSON product pages and write them to raw storage.

Business logic stays in ``open_data_platform.ingestion.dummyjson_products``.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from ingest_common import DEFAULT_ARGS, EXECUTION_ID, execution_id_param, python_task

with DAG(
    dag_id="ingest_dummyjson_products",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    params={"execution_id": execution_id_param()},
    doc_md=__doc__,
):
    python_task(
        "ingest",
        [
            "-m",
            "open_data_platform.ingestion.dummyjson_products",
            "--execution-id",
            EXECUTION_ID,
        ],
    )
