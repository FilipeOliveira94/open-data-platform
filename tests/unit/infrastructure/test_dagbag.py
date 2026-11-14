"""Load every DAG under airflow/dags with the pinned Airflow (P3-02, P3-03).

No Compose services. Airflow uses a temporary home with a SQLite file that
the DagBag does not touch. Run behavior (success, retry, failure) is P3-09.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
DAG_DIR = ROOT / "airflow" / "dags"

EXPECTED_DAGS = {
    "platform_health",
    "platform_env",
    "ingest_olist",
    "ingest_dummyjson_products",
    "ingest_ibge_localidades",
    "ingest_demo_storefront",
    "ingest_postgres_oltp",
    "seed_postgres_oltp",
    "transform_lakehouse",
}

MEDALLION_BUILD = "open_data_platform.lakehouse.medallion_build"
RUN_LABEL = ["--run-label", "{{ run_id }}"]
TRANSFORM_ORDER = (
    ("bronze", "bronze"),
    ("bronze_checks", "bronze-checks"),
    ("silver", "silver"),
    ("silver_checks", "silver-checks"),
    ("stage_quarantine", "stage-quarantine"),
    ("dbt_build", "dbt-build"),
)

INGEST_ENTRYPOINTS = {
    "ingest_olist": ("open_data_platform.ingestion.olist",),
    "ingest_dummyjson_products": ("open_data_platform.ingestion.dummyjson_products",),
    "ingest_ibge_localidades": ("open_data_platform.ingestion.ibge_localidades",),
    "ingest_demo_storefront": ("open_data_platform.ingestion.demo_storefront",),
    "ingest_postgres_oltp": ("open_data_platform.ingestion.postgres_oltp", "extract"),
    "seed_postgres_oltp": ("open_data_platform.ingestion.postgres_oltp", "seed"),
}


@pytest.fixture(scope="module")
def dagbag(tmp_path_factory: pytest.TempPathFactory):
    home = tmp_path_factory.mktemp("airflow-home")
    patch = pytest.MonkeyPatch()
    patch.setenv("AIRFLOW_HOME", str(home))
    patch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    patch.setenv("AIRFLOW__CORE__DAGS_FOLDER", str(DAG_DIR))
    patch.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    # Airflow puts the dags folder on sys.path; the DagBag alone does not.
    patch.syspath_prepend(str(DAG_DIR))
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAG_DIR))
    yield bag
    patch.undo()
    sys.modules.pop("ingest_common", None)


def _vector(task) -> list[str]:
    return [str(arg) for arg in task.op_args]


def test_every_dag_imports_without_errors(dagbag) -> None:
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == EXPECTED_DAGS


def test_wrapper_tasks_share_the_p3_02_defaults(dagbag) -> None:
    for dag_id in EXPECTED_DAGS - {"platform_health"}:
        dag = dagbag.dags[dag_id]
        assert dag.catchup is False
        for task in dag.tasks:
            assert task.retries == 2, (dag_id, task.task_id)
            assert task.retry_delay == timedelta(minutes=2)
            assert task.execution_timeout == timedelta(minutes=30)
            assert task.python_callable.__name__ == "run_python_args"


def test_ingestion_dags_call_their_entrypoints(dagbag) -> None:
    for dag_id, expected in INGEST_ENTRYPOINTS.items():
        dag = dagbag.dags[dag_id]
        assert len(dag.tasks) == 1
        vector = _vector(dag.tasks[0])
        assert vector[:2] == ["-m", expected[0]]
        for word in expected[1:]:
            assert vector[2] == word


def test_transform_runs_bronze_silver_gold_in_order(dagbag) -> None:
    dag = dagbag.dags["transform_lakehouse"]
    assert dag.schedule is None
    assert type(dag.timetable).__name__ == "NullTimetable"
    assert dag.max_active_runs == 1
    assert [task.task_id for task in dag.topological_sort()] == [
        task_id for task_id, _step in TRANSFORM_ORDER
    ]
    previous = None
    for task_id, step in TRANSFORM_ORDER:
        task = dag.get_task(task_id)
        assert _vector(task) == [
            "-m",
            MEDALLION_BUILD,
            step,
            "--run-label",
            "{{ run_id }}",
        ]
        upstream = set(task.upstream_task_ids)
        assert upstream == (set() if previous is None else {previous})
        previous = task_id
    assert "dbt_reconcile" not in dag.task_ids
    assert all("reconcile" not in " ".join(_vector(t)) for t in dag.tasks)


def test_medallion_build_steps_exist(dagbag) -> None:
    from open_data_platform.lakehouse.medallion_build import STEPS

    for task in dagbag.dags["transform_lakehouse"].tasks:
        assert _vector(task)[2] in STEPS
