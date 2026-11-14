"""Contract checks for the P3-02 ingestion DAG files. No Compose required."""

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
    *ENTRYPOINTS,
}


def _text(name: str) -> str:
    return (DAG_DIR / name).read_text(encoding="utf-8")


def test_dag_directory_matches_the_p3_02_and_p3_03_set() -> None:
    names = {path.name for path in DAG_DIR.iterdir() if path.name != "__pycache__"}
    assert names == DAG_FILES


def test_shared_task_defaults_are_not_a_dag() -> None:
    text = _text("ingest_common.py")
    ignore = _text(".airflowignore")
    assert "ingest_common.py" in ignore
    assert '"retries": 2' in text
    assert "timedelta(minutes=2)" in text
    assert "timedelta(minutes=30)" in text
    assert "subprocess.run" in text
    assert "shell=True" not in text
    assert "cwd=REPO" in text
    assert 'EXECUTION_ID = "{{ params.execution_id or run_id }}"' in text
    assert "dag_id" not in text
    assert "SELECT" not in text.upper()


def test_ingestion_dags_call_package_entrypoints() -> None:
    for name, module in ENTRYPOINTS.items():
        text = _text(name)
        assert f'"{module}"' in text
        assert "default_args=DEFAULT_ARGS" in text
        assert "schedule=None" in text
        assert "catchup=False" in text
        assert "is_paused_upon_creation=False" in text
        assert "python_task(" in text
        assert "bash_command" not in text
        assert "@daily" not in text
        assert "@weekly" not in text
        assert "SELECT" not in text.upper()
        assert "run-001" not in text
        assert "p3-02-" not in text


def test_execution_id_defaults_to_the_airflow_run() -> None:
    for name in ENTRYPOINTS:
        if name == "seed_postgres_oltp.py":
            assert "EXECUTION_ID" not in _text(name)
            continue
        assert "EXECUTION_ID" in _text(name)
        assert "execution_id_param()" in _text(name)


def test_extract_and_seed_stay_separate() -> None:
    extract = _text("ingest_postgres_oltp.py")
    seed = _text("seed_postgres_oltp.py")
    assert '"extract"' in extract
    assert '"seed"' not in extract
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
    assert "is_paused_upon_creation=False" in text
    assert "default_args=DEFAULT_ARGS" in text
    for step in ("imports", "contracts", "minio_raw", "minio_write", "postgres_oltp"):
        assert f'"/opt/odp/check_ingest_env.py", "{step}"' in text
    assert "imports >> [contracts, minio_raw, minio_write, postgres_oltp]" in text
    assert "SELECT" not in text.upper()
    assert "open_data_platform.ingestion" not in text
