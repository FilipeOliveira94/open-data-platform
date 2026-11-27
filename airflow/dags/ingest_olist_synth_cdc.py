"""Extract one olist-synth-cdc window into raw storage.

Business logic stays in ``open_data_platform.ingestion.olist_synth_cdc``.
The caller supplies both window bounds. This DAG does not calculate a
daily or weekly window, and it does not seed the tables. A successful
extract emits the ``raw_olist_synth_cdc`` Asset, which schedules
``transform_lakehouse`` (ADR-0024). The scheduled daily path is
``ingest_olist_synth_cdc_daily``.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, Param
from ingest_common import DEFAULT_ARGS, execution_id_param, postgres_extract_task

with DAG(
    dag_id="ingest_olist_synth_cdc",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
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
    postgres_extract_task(
        "{{ params.window_start }}", "{{ params.window_end }}", "manual"
    )
