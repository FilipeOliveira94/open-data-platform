"""Fetch DummyJSON product pages and write them to raw storage, weekly.

Business logic stays in ``open_data_platform.ingestion.dummyjson_products``.

Refresh policy (P3-12): the contract is ``full_refresh``. Each run fetches
the whole catalog and closes its execution with a completion marker; Silver
takes survivors only from the latest complete execution, so a product absent
from it leaves Silver. Runs Monday 03:00 UTC. ``catchup`` is false: the run
does not depend on a window, and a missed week is covered by the next full
fetch. A successful run emits the ``raw_dummyjson_products`` Asset, which
schedules ``transform_lakehouse`` (ADR-0024). Manual triggers still work and
accept an explicit ``execution_id``.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, CronTriggerTimetable
from ingest_common import (
    DEFAULT_ARGS,
    EXECUTION_ID,
    execution_id_param,
    python_task,
    raw_asset,
)

with DAG(
    dag_id="ingest_dummyjson_products",
    start_date=datetime(2026, 1, 1),
    schedule=CronTriggerTimetable("0 3 * * 1", timezone="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
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
        outlets=[raw_asset("dummyjson_products")],
    )
