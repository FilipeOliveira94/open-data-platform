"""Generate the fictional storefront clickstream and write it to raw storage.

Business logic stays in ``open_data_platform.ingestion.demo_storefront``.
The default arguments match the documented local command.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, Param
from ingest_common import DEFAULT_ARGS, EXECUTION_ID, execution_id_param, python_task

with DAG(
    dag_id="ingest_demo_storefront",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    params={
        "execution_id": execution_id_param(),
        "seed": Param(7, type="integer", minimum=0),
        "count": Param(100, type="integer", minimum=1),
        "event_time_start": Param(
            "2026-10-18T00:00:00Z",
            type="string",
            minLength=1,
            description="Timezone-aware ISO-8601 start of the event clock.",
        ),
    },
    doc_md=__doc__,
):
    python_task(
        "ingest",
        [
            "-m",
            "open_data_platform.ingestion.demo_storefront",
            "--execution-id",
            EXECUTION_ID,
            "--seed",
            "{{ params.seed }}",
            "--count",
            "{{ params.count }}",
            "--event-time-start",
            "{{ params.event_time_start }}",
        ],
    )
