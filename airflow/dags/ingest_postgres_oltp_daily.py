"""Extract yesterday's PostgreSQL OLTP window every day, then rebuild the lakehouse.

Business logic stays in ``open_data_platform.ingestion.postgres_oltp``. The
window is the Airflow data interval of the run: ``CronDataIntervalTimetable``
at ``0 0 * * *`` in UTC gives ``[D-1 00:00Z, D 00:00Z)`` for the run that
starts at ``D 00:00Z``. The extract rejects a bound that is not UTC
midnight, so the timetable stays in UTC.

After a successful extract, ``trigger_transform`` starts
``transform_lakehouse`` with the run id ``<this DAG id>__<this run id>`` and
does not wait for it. Clearing that task resets the same transform run
instead of creating a second one. The manual ``ingest_postgres_oltp`` DAG
does not trigger the transform.

``catchup`` is true. The window comes only from the schedule, so a day
the scheduler did not run (DAG paused, scheduler down) would otherwise
never be extracted. With catchup, the scheduler creates one run per
missed interval since the last automated run, oldest first, one at a time
(``max_active_runs=1``). ``START_DATE`` is the first window this DAG
covers; windows before it are a backfill (P3-06). A run that is
re-executed keeps its run id, so the extract overwrites the same raw keys
on the same ingestion date. A new run of the same window writes a new
execution, and Silver deduplicates by key.
"""

from __future__ import annotations

from datetime import UTC, datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, CronDataIntervalTimetable
from ingest_common import DEFAULT_ARGS, execution_id_param, postgres_extract_task

DAG_ID = "ingest_postgres_oltp_daily"
TRANSFORM_DAG_ID = "transform_lakehouse"
# First daily window: [2026-11-19, 2026-11-20). Each missed day after it is
# caught up; earlier days are a backfill.
START_DATE = datetime(2026, 11, 19, tzinfo=UTC)

# A run without a data interval renders an empty bound, and the extract
# fails on it before it connects.
WINDOW_START = "{{ data_interval_start.isoformat() if data_interval_start else '' }}"
WINDOW_END = "{{ data_interval_end.isoformat() if data_interval_end else '' }}"

with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    params={"execution_id": execution_id_param()},
    doc_md=__doc__,
):
    extract = postgres_extract_task(WINDOW_START, WINDOW_END)
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id=TRANSFORM_DAG_ID,
        trigger_run_id=f"{DAG_ID}__{{{{ run_id }}}}",
        reset_dag_run=True,
        wait_for_completion=False,
        fail_when_dag_is_paused=True,
    )
    extract >> trigger_transform
