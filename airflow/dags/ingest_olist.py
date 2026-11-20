"""Fetch the Olist snapshots and write them to raw storage.

Business logic stays in ``open_data_platform.ingestion.olist``. Kaggle
credentials come from the container environment, not from task arguments.

Refresh policy (P3-12): manual only. Olist is a fixed historical release
(``snapshot``); a new run lands the same rows, and Silver keeps the latest
complete execution, so its rows and counts do not change. A successful run
emits the ``raw_olist_brazilian_ecommerce`` and ``raw_olist_marketing_funnel``
Assets, which schedule ``transform_lakehouse`` (ADR-0024).
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from ingest_common import (
    DEFAULT_ARGS,
    EXECUTION_ID,
    execution_id_param,
    python_task,
    raw_asset,
)

with DAG(
    dag_id="ingest_olist",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    params={"execution_id": execution_id_param()},
    doc_md=__doc__,
):
    python_task(
        "ingest",
        [
            "-m",
            "open_data_platform.ingestion.olist",
            "--execution-id",
            EXECUTION_ID,
        ],
        outlets=[
            raw_asset("olist_brazilian_ecommerce"),
            raw_asset("olist_marketing_funnel"),
        ],
    )
