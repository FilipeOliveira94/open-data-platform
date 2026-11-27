"""Check Python, contracts, MinIO, and the olist-synth database.

The tasks call ``docker/airflow/check_ingest_env.py``.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from ingest_common import DEFAULT_ARGS, python_task

with DAG(
    dag_id="platform_env",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
):
    imports = python_task(
        "imports",
        ["/opt/odp/check_ingest_env.py", "imports"],
    )
    contracts = python_task(
        "contracts",
        ["/opt/odp/check_ingest_env.py", "contracts"],
    )
    minio_raw = python_task(
        "minio_raw",
        ["/opt/odp/check_ingest_env.py", "minio_raw"],
    )
    minio_write = python_task(
        "minio_write",
        ["/opt/odp/check_ingest_env.py", "minio_write"],
    )
    olist_synth_cdc = python_task(
        "olist_synth_cdc",
        ["/opt/odp/check_ingest_env.py", "olist_synth_cdc"],
    )
    imports >> [contracts, minio_raw, minio_write, olist_synth_cdc]
