"""Re-extract a trailing seven-day olist-synth-cdc window every Monday.

Purpose: the safety net for late commits. The daily DAG extracts
``[D-1, D)`` at ``D 00:00Z``; a row whose ``updated_at`` falls in that day
but whose transaction commits after the run is never in a daily window.
This DAG re-extracts seven days through the same P1-06 extract
(``postgres_extract_task``), under its own execution id (this DAG's id
and run id; the run id alone repeats the daily one at Monday 00:00Z),
and Silver keeps the latest ``updated_at`` per key, so rows the daily
runs already landed are not duplicated.

Window: the run at Monday ``W 00:00Z`` has the data interval
``[W-7, W)`` (``CronDataIntervalTimetable`` at ``0 0 * * 1`` in UTC). The
extract window is that interval shifted back one day: ``[W-8, W-1)``,
Sunday to Saturday. The lag gives every day in the window at least one
full day after its daily run for late commits to land. Without it,
Sunday would have the same 00:00Z edge as the daily run. Consecutive
windows tile without gaps or overlap. A row committed after the weekly run
that covers its ``updated_at`` is still missed: backfill (P3-06) and CDC
(Phase 4) cover that. A hard delete is never extracted (P3-12).

``catchup`` is true and ``max_active_runs`` is one, as in the daily DAG.
``START_DATE`` 2026-11-16 makes the first scheduled run 2026-11-23, which
extracts ``[2026-11-15, 2026-11-22)``. A successful extract emits the
``raw_olist_synth_cdc`` Asset, which schedules ``transform_lakehouse``
(ADR-0024).
"""

from __future__ import annotations

from datetime import UTC, datetime

from airflow.sdk import DAG, CronDataIntervalTimetable
from ingest_common import DEFAULT_ARGS, execution_id_param, postgres_extract_task

DAG_ID = "ingest_olist_synth_cdc_weekly"
# A Monday. First data interval [2026-11-16, 2026-11-23), extract window
# [2026-11-15, 2026-11-22).
START_DATE = datetime(2026, 11, 16, tzinfo=UTC)

# One day of lag behind the data interval. A run without a data interval
# renders an empty bound, and the extract fails on it before it connects.
WINDOW_START = (
    "{{ data_interval_start.subtract(days=1).isoformat() "
    "if data_interval_start else '' }}"
)
WINDOW_END = (
    "{{ data_interval_end.subtract(days=1).isoformat() if data_interval_end else '' }}"
)

with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule=CronDataIntervalTimetable("0 0 * * 1", timezone="UTC"),
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    params={"execution_id": execution_id_param()},
    doc_md=__doc__,
):
    postgres_extract_task(WINDOW_START, WINDOW_END, "weekly")
