"""Contract checks for the ingestion DAG files (P3-02 to P3-05). No Compose required."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DAG_DIR = ROOT / "airflow" / "dags"

ENTRYPOINTS = {
    "ingest_olist.py": "open_data_platform.ingestion.olist",
    "ingest_dummyjson_products.py": "open_data_platform.ingestion.dummyjson_products",
    "ingest_ibge_localidades.py": "open_data_platform.ingestion.ibge_localidades",
    "ingest_demo_storefront.py": "open_data_platform.ingestion.demo_storefront",
    "ingest_postgres_oltp.py": "open_data_platform.ingestion.postgres_oltp",
    "seed_postgres_oltp.py": "open_data_platform.ingestion.postgres_oltp",
}

DAG_FILES = {
    "platform_health.py",
    "platform_env.py",
    "ingest_common.py",
    ".airflowignore",
    "transform_lakehouse.py",
    "reconcile_gold.py",
    "ingest_postgres_oltp_daily.py",
    "ingest_postgres_oltp_weekly.py",
    *ENTRYPOINTS,
}

# The PostgreSQL extract vector lives in ingest_common.postgres_extract_task.
EXTRACT_HELPER_DAGS = {
    "ingest_postgres_oltp.py",
    "ingest_postgres_oltp_daily.py",
    "ingest_postgres_oltp_weekly.py",
}


def _text(name: str) -> str:
    return (DAG_DIR / name).read_text(encoding="utf-8")


def test_dag_directory_matches_the_p3_02_to_p3_05_set() -> None:
    names = {path.name for path in DAG_DIR.iterdir() if path.name != "__pycache__"}
    assert names == DAG_FILES


def test_shared_task_defaults_are_not_a_dag() -> None:
    text = _text("ingest_common.py")
    ignore = _text(".airflowignore")
    assert "ingest_common.py" in ignore
    assert '"retries": 2' in text
    assert "timedelta(minutes=2)" in text
    assert "timedelta(minutes=30)" in text
    assert "subprocess.Popen(command, cwd=REPO, start_new_session=True)" in text
    assert "os.killpg" in text
    assert "shell=True" not in text
    assert "cwd=REPO" in text
    assert 'EXECUTION_ID = "{{ params.execution_id or run_id }}"' in text
    assert "dag_id" not in text
    assert "SELECT" not in text.upper()


# P3-12: full_refresh sources run on a cron; the rest stay manual.
SCHEDULED_TEXT = {
    "ingest_dummyjson_products.py": 'CronTriggerTimetable("0 3 * * 1", timezone="UTC")',
    "ingest_ibge_localidades.py": 'CronTriggerTimetable("0 3 1 * *", timezone="UTC")',
}


def test_ingestion_dags_call_package_entrypoints() -> None:
    common = _text("ingest_common.py")
    for name, module in ENTRYPOINTS.items():
        text = _text(name)
        if name in EXTRACT_HELPER_DAGS:
            assert f'POSTGRES_OLTP = "{module}"' in common
            assert "postgres_extract_task(" in text
        else:
            assert f'"{module}"' in text
            assert "python_task(" in text
        assert "default_args=DEFAULT_ARGS" in text
        if name not in SCHEDULED_TEXT:
            assert "schedule=None" in text
        else:
            assert SCHEDULED_TEXT[name] in text
        assert "catchup=False" in text
        assert "is_paused_upon_creation=True" in text
        assert "bash_command" not in text
        assert "@daily" not in text
        assert "@weekly" not in text
        assert "SELECT" not in text.upper()
        assert "run-001" not in text
        assert "p3-02-" not in text


def test_execution_id_defaults_to_the_airflow_run() -> None:
    common = _text("ingest_common.py")
    assert '"--execution-id",\n            EXECUTION_ID,' in common
    for name in {*ENTRYPOINTS, *EXTRACT_HELPER_DAGS}:
        if name == "seed_postgres_oltp.py":
            assert "EXECUTION_ID" not in _text(name)
            continue
        if name not in EXTRACT_HELPER_DAGS:
            assert "EXECUTION_ID" in _text(name)
        assert "execution_id_param()" in _text(name)


def test_daily_extract_dag_derives_the_window_and_emits_the_raw_asset() -> None:
    text = _text("ingest_postgres_oltp_daily.py")
    assert 'CronDataIntervalTimetable("0 0 * * *", timezone="UTC")' in text
    assert "catchup=True" in text
    assert "is_paused_upon_creation=True" in text
    assert "data_interval_start.isoformat()" in text
    assert "data_interval_end.isoformat()" in text
    assert "params.window_start" not in text
    # The transform is scheduled on the raw Asset (ADR-0024), not triggered.
    assert "import TriggerDagRunOperator" not in text
    assert "postgres_extract_task(WINDOW_START, WINDOW_END)" in text
    assert "SELECT" not in text.upper()
    manual = _text("ingest_postgres_oltp.py")
    assert "TriggerDagRunOperator" not in manual


def test_weekly_extract_dag_lags_the_interval_and_emits_the_raw_asset() -> None:
    text = _text("ingest_postgres_oltp_weekly.py")
    assert 'CronDataIntervalTimetable("0 0 * * 1", timezone="UTC")' in text
    assert "catchup=True" in text
    assert "max_active_runs=1" in text
    assert "is_paused_upon_creation=True" in text
    assert "data_interval_start.subtract(days=1).isoformat()" in text
    assert "data_interval_end.subtract(days=1).isoformat()" in text
    assert "TriggerDagRunOperator" not in text
    assert "postgres_extract_task(WINDOW_START, WINDOW_END)" in text
    assert "SELECT" not in text.upper()


def test_extract_and_seed_stay_separate() -> None:
    extract = _text("ingest_postgres_oltp.py")
    seed = _text("seed_postgres_oltp.py")
    assert "postgres_extract_task(" in extract
    assert '            "extract",\n' in _text("ingest_common.py")
    assert '"seed"' not in extract
    assert '"seed"' not in _text("ingest_common.py")
    assert "window_start" in extract
    assert "window_end" in extract
    assert "2026-10-17" not in extract
    assert '"seed"' in seed
    assert '"extract"' not in seed
    assert "window_start" not in seed


def test_platform_env_checks_are_parallel_after_imports() -> None:
    text = _text("platform_env.py")
    assert 'dag_id="platform_env"' in text
    assert "schedule=None" in text
    assert "is_paused_upon_creation=True" in text
    assert "default_args=DEFAULT_ARGS" in text
    for step in ("imports", "contracts", "minio_raw", "minio_write", "postgres_oltp"):
        assert f'"/opt/odp/check_ingest_env.py", "{step}"' in text
    assert "imports >> [contracts, minio_raw, minio_write, postgres_oltp]" in text
    assert "SELECT" not in text.upper()
    assert "open_data_platform.ingestion" not in text
