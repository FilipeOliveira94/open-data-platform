"""Rebuild Bronze, Silver, and Gold from the raw objects in MinIO.

Each task runs ``python -m open_data_platform.lakehouse.medallion_build <step>``, the
same entrypoint as ``make live-bronze``, ``make live-silver``, and
``make live-gold``. Business logic stays in the lakehouse package, the Spark
scripts under ``docker/spark``, and the dbt project.

Tasks run one after another. Each Spark step shares one Ivy cache and one
container, so two Spark steps do not run at the same time. Reconciliation
tests are the separate weekly ``reconcile_gold`` DAG.

The steps that write tables (``bronze``, ``silver``, ``stage_quarantine``,
``dbt_build``) take the one slot of the ``lakehouse_gold`` pool, which
``reconcile_gold`` also uses. A reconciliation run never overlaps one of
those writes. The read-only check steps stay outside the pool.

``ingest_postgres_oltp_daily`` triggers this DAG after its extract. A manual
trigger still works as before.

The Trino service (Compose profile ``lakehouse``) must be up for the check
steps and dbt.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, chain
from ingest_common import DEFAULT_ARGS, LAKEHOUSE_GOLD_POOL, python_task

MEDALLION_BUILD = "open_data_platform.lakehouse.medallion_build"
# Rendered SQL, Bronze counts, and dbt output go to /opt/odp/work/<run id>.
RUN_LABEL = ["--run-label", "{{ run_id }}"]

# (task id, medallion_build step, writes tables)
STEPS = (
    ("bronze", "bronze", True),
    ("bronze_checks", "bronze-checks", False),
    ("silver", "silver", True),
    ("silver_checks", "silver-checks", False),
    ("stage_quarantine", "stage-quarantine", True),
    ("dbt_build", "dbt-build", True),
)

with DAG(
    dag_id="transform_lakehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
):
    chain(
        *(
            python_task(
                task_id,
                ["-m", MEDALLION_BUILD, step, *RUN_LABEL],
                pool=LAKEHOUSE_GOLD_POOL if writes else None,
            )
            for task_id, step, writes in STEPS
        )
    )
