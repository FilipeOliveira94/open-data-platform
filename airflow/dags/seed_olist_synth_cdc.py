"""Replace the olist-synth tables with the synthetic fixture.

This runs ``python -m open_data_platform.ingestion.olist_synth_cdc seed``.
The package truncates ``olist_synth.sellers``, ``olist_synth.customers``,
``olist_synth.orders``, and ``olist_synth.order_items``, then loads the fixture
again. The task is not upstream of ``ingest_olist_synth_cdc``.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from ingest_common import DEFAULT_ARGS, python_task

with DAG(
    dag_id="seed_olist_synth_cdc",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
):
    python_task(
        "seed",
        ["-m", "open_data_platform.ingestion.olist_synth_cdc", "seed"],
    )
