"""Refresh Bronze, Silver, and Gold from new raw objects in MinIO (ADR-0024).

Each task runs ``python -m open_data_platform.lakehouse.medallion_build <step>``, the
same entrypoint as the ``make live-*`` targets. Business logic stays in the
lakehouse package, the Spark scripts under ``docker/spark``, and the dbt
project.

Schedule: any raw Asset that an ingestion DAG emits on success
(``ingest_common.RAW_SOURCES``), with ``batch_asset_events=True`` and
``max_active_runs=1``. While a run is queued or running, the scheduler defers
this DAG and the events stay queued; when the run ends, one follow-up run
consumes all of them. Without batching, Airflow 3.4 creates one run per
event. This replaces the P3-04 ``TriggerDagRunOperator``.
A manual trigger still works.

The refresh is incremental. ``bronze`` promotes only raw objects that the
promotion registry has not seen; ``silver`` rebuilds only the datasets
pending for it; ``stage_quarantine`` and ``dbt_build`` run only while Gold
is pending. A step with nothing to do writes no table, logs that, and ends
as skipped. Each later write step uses the ``none_failed`` trigger rule, so a
skipped step does not stop a layer that is still pending from an earlier
failed run. A check step runs only after its write step succeeded.

Quality gate (P3-16): each write marks its check pending in the promotion
registry. A check step runs every open (pending or failed) check of its layer
and records the result, so a check that failed runs again on the next
refresh even when no new raw object arrives; ``bronze`` and ``silver`` end
as "ran" while a check is open so the check task is not skipped. Silver does
not build a dataset whose Bronze check is open, and Gold does not build while
any check is open. Check steps have ``retries=0``: a failed check is a data
result, not a transient error.
``--full-rebuild`` is a Make target (``make live-rebuild``), never this
schedule.

Tasks run one after another. Each Spark step shares one Ivy cache and one
container, so two Spark steps do not run at the same time. Reconciliation
tests are the separate weekly ``reconcile_gold`` DAG.

The steps that write tables (``bronze``, ``silver``, ``stage_quarantine``,
``dbt_build``) take the one slot of the ``lakehouse_gold`` pool, which
``reconcile_gold`` also uses. A reconciliation run never overlaps one of
those writes. The read-only check steps stay outside the pool.

The Trino service (Compose profile ``lakehouse``) must be up for the check
steps and dbt.

``prune_work`` (P3-10) deletes the scratch folders of earlier runs under
``/opt/odp/work`` once they are older than ``ODP_WORK_RETENTION_DAYS``
(default 7). It never deletes this run's folder, and it touches no Airflow
run, task log, table, or raw object. It has no upstream or downstream task:
it runs beside the refresh, so its result never blocks a layer, and a failed
refresh still fails the DAG run (a leaf task with ``all_done`` would hide it).
"""

from __future__ import annotations

from datetime import datetime
from functools import reduce
from operator import or_

from airflow.sdk import DAG, AssetTriggeredTimetable, chain
from ingest_common import (
    DEFAULT_ARGS,
    LAKEHOUSE_GOLD_POOL,
    RAW_SOURCES,
    SKIP_EXIT_CODE,
    python_task,
    raw_asset,
)

MEDALLION_BUILD = "open_data_platform.lakehouse.medallion_build"
# Rendered SQL, Bronze counts, staging keys, and dbt output use the run id.
RUN_LABEL = ["--run-label", "{{ run_id }}"]
SKIP = ["--skip-exit-code", str(SKIP_EXIT_CODE)]

# (task id, medallion_build step, writes tables)
STEPS = (
    ("bronze", "bronze", True),
    ("bronze_checks", "bronze-checks", False),
    ("silver", "silver", True),
    ("silver_checks", "silver-checks", False),
    ("stage_quarantine", "stage-quarantine", True),
    ("dbt_build", "dbt-build", True),
)

RAW_ASSETS = reduce(or_, (raw_asset(source) for source in RAW_SOURCES))
# ``batch_asset_events``: every event queued while a refresh runs is consumed
# by one follow-up run. Without it, Airflow 3.4 creates one run per event.
SCHEDULE = AssetTriggeredTimetable(RAW_ASSETS, batch_asset_events=True)

with DAG(
    dag_id="transform_lakehouse",
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
):
    python_task(
        "prune_work",
        ["-m", MEDALLION_BUILD, "prune-work", *RUN_LABEL, *SKIP],
        skip_exit_code=SKIP_EXIT_CODE,
    )
    chain(
        *(
            python_task(
                task_id,
                ["-m", MEDALLION_BUILD, step, *RUN_LABEL, *SKIP],
                pool=LAKEHOUSE_GOLD_POOL if writes else None,
                skip_exit_code=SKIP_EXIT_CODE,
                # A write step runs after a skipped step; a check step only
                # after its write step succeeded.
                trigger_rule="none_failed" if writes and index else None,
                # A failed check is a data result, not a transient error: it
                # stays open in the registry and runs on the next refresh.
                retries=None if writes else 0,
            )
            for index, (task_id, step, writes) in enumerate(STEPS)
        )
    )
