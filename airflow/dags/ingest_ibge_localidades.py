"""Fetch IBGE municipalities and write them to raw storage, monthly.

Business logic stays in ``open_data_platform.ingestion.ibge_localidades``.

Refresh policy (P3-12): the contract is ``full_refresh``. Each run fetches
the full municipality list and closes its execution with a completion
marker; Silver takes survivors only from the latest complete execution, so a
municipality absent from it leaves Silver. Runs on day 1 of each month at
03:00 UTC; the list changes rarely. ``catchup`` is false: the run does not
depend on a window, and a missed month is covered by the next full fetch.
This DAG does not start ``transform_lakehouse``; the next lakehouse refresh
picks the execution up (P3-13 wires Assets).
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, CronTriggerTimetable
from ingest_common import DEFAULT_ARGS, EXECUTION_ID, execution_id_param, python_task

with DAG(
    dag_id="ingest_ibge_localidades",
    start_date=datetime(2026, 1, 1),
    schedule=CronTriggerTimetable("0 3 1 * *", timezone="UTC"),
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
            "open_data_platform.ingestion.ibge_localidades",
            "--execution-id",
            EXECUTION_ID,
        ],
    )
