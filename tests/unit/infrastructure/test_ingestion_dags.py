"""DAG source checks that loading the DagBag does not cover.

test_dagbag.py loads schedules, entrypoints, and rendered windows.
This file checks the directory listing, shell=True, and a SQL SELECT
keyword in the DAG sources.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DAG_DIR = ROOT / "airflow" / "dags"

DAG_FILES = {
    ".airflowignore",
    "ingest_common.py",
    "ingest_demo_storefront.py",
    "ingest_dummyjson_products.py",
    "ingest_ibge_localidades.py",
    "ingest_olist.py",
    "ingest_postgres_oltp.py",
    "ingest_postgres_oltp_daily.py",
    "ingest_postgres_oltp_weekly.py",
    "platform_env.py",
    "platform_health.py",
    "reconcile_gold.py",
    "seed_postgres_oltp.py",
    "transform_lakehouse.py",
}


def test_dag_directory_matches_the_shipped_set() -> None:
    names = {path.name for path in DAG_DIR.iterdir() if path.name != "__pycache__"}
    assert names == DAG_FILES
    ignore = (DAG_DIR / ".airflowignore").read_text(encoding="utf-8")
    assert "ingest_common.py" in ignore.splitlines()


def test_dag_sources_do_not_shell_out_or_embed_sql() -> None:
    for name in sorted(DAG_FILES):
        if not name.endswith(".py"):
            continue
        text = (DAG_DIR / name).read_text(encoding="utf-8")
        assert "shell=True" not in text, name
        # A word boundary so the prose "selects" in reconcile_gold.py stays,
        # and a SQL SELECT statement still fails.
        assert re.search(r"\bSELECT\b", text, re.IGNORECASE) is None, name
