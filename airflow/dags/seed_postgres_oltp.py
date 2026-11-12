"""Replace the OLTP tables with the synthetic fixture.

This runs ``python -m open_data_platform.ingestion.postgres_oltp seed``.
The package truncates ``oltp.sellers``, ``oltp.customers``, ``oltp.orders``,
and ``oltp.order_items``, then loads the fixture again. The task is not
upstream of ``ingest_postgres_oltp``.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from ingest_common import DEFAULT_ARGS, python_task

with DAG(
    dag_id="seed_postgres_oltp",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
):
    python_task(
        "seed",
        ["-m", "open_data_platform.ingestion.postgres_oltp", "seed"],
    )
