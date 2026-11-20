"""Load every DAG under airflow/dags with the pinned Airflow (P3-02 to P3-04).

No Compose services. Airflow uses a temporary home with a SQLite file that
the DagBag does not touch. Run behavior (success, retry, failure) is P3-09.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
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
    "reconcile_gold",
    "ingest_postgres_oltp_daily",
}

POOL = "lakehouse_gold"
POOLED_TASKS = {
    ("transform_lakehouse", "bronze"),
    ("transform_lakehouse", "silver"),
    ("transform_lakehouse", "stage_quarantine"),
    ("transform_lakehouse", "dbt_build"),
    ("reconcile_gold", "dbt_reconcile"),
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

DAILY = "ingest_postgres_oltp_daily"

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
        # The daily extract catches up missed windows (P3-04).
        assert dag.catchup is (dag_id == DAILY)
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
    assert type(dag.timetable).__name__ == "AssetTriggeredTimetable"
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
            "--skip-exit-code",
            "99",
        ]
        assert task.op_kwargs == {"skip_exit_code": 99}
        writes = task_id in {"bronze", "silver", "stage_quarantine", "dbt_build"}
        expected_rule = "none_failed" if writes and previous else "all_success"
        assert task.trigger_rule == expected_rule, task_id
        upstream = set(task.upstream_task_ids)
        assert upstream == (set() if previous is None else {previous})
        previous = task_id
    assert "dbt_reconcile" not in dag.task_ids
    assert all("reconcile" not in " ".join(_vector(t)) for t in dag.tasks)


def test_reconcile_gold_is_weekly_and_separate(dagbag) -> None:
    dag = dagbag.dags["reconcile_gold"]
    assert dag.schedule == "@weekly"
    assert dag.max_active_runs == 1
    assert dag.task_ids == ["dbt_reconcile"]
    vector = _vector(dag.get_task("dbt_reconcile"))
    assert vector == [
        "-m",
        MEDALLION_BUILD,
        "dbt-reconcile",
        "--run-label",
        "{{ run_id }}",
    ]


def test_medallion_build_steps_exist(dagbag) -> None:
    from open_data_platform.lakehouse.medallion_build import STEPS

    for dag_id in ("transform_lakehouse", "reconcile_gold"):
        for task in dagbag.dags[dag_id].tasks:
            assert _vector(task)[2] in STEPS


def test_every_dag_is_paused_at_creation(dagbag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.is_paused_upon_creation is True, dag_id


def test_lakehouse_pool_holds_exactly_the_writes_and_reconciliation(dagbag) -> None:
    pooled = {
        (dag_id, task.task_id)
        for dag_id, dag in dagbag.dags.items()
        for task in dag.tasks
        if task.pool == POOL
    }
    assert pooled == POOLED_TASKS
    from ingest_common import LAKEHOUSE_GOLD_POOL

    assert LAKEHOUSE_GOLD_POOL == POOL
    for dag_id, task_id in POOLED_TASKS:
        assert dagbag.dags[dag_id].get_task(task_id).pool_slots == 1
    transform = dagbag.dags["transform_lakehouse"]
    for task_id in ("bronze_checks", "silver_checks"):
        assert transform.get_task(task_id).pool == "default_pool"


def _core_timetable(dag):
    from airflow.serialization.encoders import coerce_to_core_timetable

    return coerce_to_core_timetable(dag.timetable)


def test_daily_dag_runs_at_utc_midnight_with_catchup(dagbag) -> None:
    from airflow.timetables.interval import CronDataIntervalTimetable

    dag = dagbag.dags[DAILY]
    timetable = _core_timetable(dag)
    assert isinstance(timetable, CronDataIntervalTimetable)
    assert timetable.summary == "0 0 * * *"
    assert str(timetable._timezone) == "UTC"
    assert dag.catchup is True
    assert dag.start_date == datetime(2026, 11, 19, tzinfo=UTC)
    assert dag.max_active_runs == 1
    assert set(dag.params.keys()) == {"execution_id"}
    assert dag.task_ids == ["extract"]
    vector = _vector(dag.get_task("extract"))
    assert vector[:3] == ["-m", "open_data_platform.ingestion.postgres_oltp", "extract"]


def test_daily_intervals_are_one_utc_day(dagbag) -> None:
    import pendulum
    from airflow.timetables.base import DataInterval, TimeRestriction

    timetable = _core_timetable(dagbag.dags[DAILY])
    utc = pendulum.timezone("UTC")
    previous = DataInterval(
        pendulum.datetime(2026, 10, 16, tz=utc), pendulum.datetime(2026, 10, 17, tz=utc)
    )
    info = timetable.next_dagrun_info(
        last_automated_data_interval=previous,
        restriction=TimeRestriction(
            earliest=pendulum.datetime(2026, 1, 1, tz=utc), latest=None, catchup=True
        ),
    )
    assert info.data_interval.start == pendulum.datetime(2026, 10, 17, tz=utc)
    assert info.data_interval.end == pendulum.datetime(2026, 10, 18, tz=utc)
    assert info.run_after == info.data_interval.end
    # A manual run gets the last complete UTC day before its run_after.
    for run_after in (
        pendulum.datetime(2026, 11, 20, 0, 31, tz=utc),
        pendulum.datetime(2026, 11, 20, 23, 59, tz=utc),
        pendulum.datetime(2026, 11, 20, tz=utc),
    ):
        manual = timetable.infer_manual_data_interval(run_after=run_after)
        assert manual.start == pendulum.datetime(2026, 11, 19, tz=utc)
        assert manual.end == pendulum.datetime(2026, 11, 20, tz=utc)


def test_daily_dag_runs_every_missed_window_in_order(dagbag) -> None:
    """After a gap the next run is the first missed day, not the latest one."""
    import pendulum
    from airflow.timetables.base import DataInterval, TimeRestriction

    dag = dagbag.dags[DAILY]
    timetable = _core_timetable(dag)
    utc = pendulum.timezone("UTC")
    restriction = TimeRestriction(
        earliest=pendulum.instance(dag.start_date), latest=None, catchup=dag.catchup
    )
    # Last automated run covered 2026-11-19; the scheduler was away for days.
    last = DataInterval(
        pendulum.datetime(2026, 11, 19, tz=utc), pendulum.datetime(2026, 11, 20, tz=utc)
    )
    seen = []
    for _ in range(3):
        info = timetable.next_dagrun_info(
            last_automated_data_interval=last, restriction=restriction
        )
        seen.append((info.data_interval.start.date(), info.data_interval.end.date()))
        last = info.data_interval
    assert [(str(start), str(end)) for start, end in seen] == [
        ("2026-11-20", "2026-11-21"),
        ("2026-11-21", "2026-11-22"),
        ("2026-11-22", "2026-11-23"),
    ]
    # The first run on a fresh database is the start_date window.
    first = timetable.next_dagrun_info(
        last_automated_data_interval=None, restriction=restriction
    )
    assert first.data_interval.start == pendulum.datetime(2026, 11, 19, tz=utc)


def _render_extract(dag, context: dict[str, object]) -> list[str]:
    task = dag.get_task("extract")
    return [str(task.render_template(arg, context)) for arg in task.op_args]


def test_daily_window_renders_into_a_valid_extract_window(dagbag) -> None:
    import pendulum

    from open_data_platform.ingestion.postgres_oltp import (
        validate_window,
        window_object_name,
    )

    dag = dagbag.dags[DAILY]
    run_id = "scheduled__2026-10-17T00:00:00+00:00"
    vector = _render_extract(
        dag,
        {
            "data_interval_start": pendulum.datetime(2026, 10, 17, tz="UTC"),
            "data_interval_end": pendulum.datetime(2026, 10, 18, tz="UTC"),
            "params": {"execution_id": ""},
            "run_id": run_id,
        },
    )
    assert vector[3:] == [
        "--window-start",
        "2026-10-17T00:00:00+00:00",
        "--window-end",
        "2026-10-18T00:00:00+00:00",
        "--execution-id",
        run_id,
    ]
    start = datetime.fromisoformat(vector[4])
    end = datetime.fromisoformat(vector[6])
    assert validate_window(start, end) == (start, end)
    name = window_object_name("sellers", start, end)
    assert name == "sellers_2026-10-17_2026-10-18.json"


def test_daily_window_without_interval_renders_empty_bounds(dagbag) -> None:
    from open_data_platform.ingestion.postgres_oltp import main

    vector = _render_extract(
        dagbag.dags[DAILY],
        {
            "data_interval_start": None,
            "data_interval_end": None,
            "params": {"execution_id": ""},
            "run_id": "manual__x",
        },
    )
    assert vector[4] == ""
    assert vector[6] == ""
    # The extract rejects the empty bound before it connects.
    patch = pytest.MonkeyPatch()
    patch.setenv("ODP_RAW_BUCKET", "odp-raw")
    patch.setenv("POSTGRES_USER", "odp")
    patch.setenv("POSTGRES_PASSWORD", "unused")
    patch.setenv("POSTGRES_DB", "odp")
    try:
        assert main(vector[2:]) == 1
    finally:
        patch.undo()


RAW_OUTLETS = {
    "ingest_olist": {"olist_brazilian_ecommerce", "olist_marketing_funnel"},
    "ingest_dummyjson_products": {"dummyjson_products"},
    "ingest_ibge_localidades": {"ibge_localidades"},
    "ingest_demo_storefront": {"demo_storefront"},
    "ingest_postgres_oltp": {"postgres_oltp"},
    DAILY: {"postgres_oltp"},
}


def test_every_ingestion_dag_emits_its_raw_assets(dagbag) -> None:
    for dag_id, sources in RAW_OUTLETS.items():
        dag = dagbag.dags[dag_id]
        uris = {outlet.uri for task in dag.tasks for outlet in task.outlets}
        assert uris == {f"odp-raw://{source}/" for source in sources}, dag_id


def test_transform_is_scheduled_on_any_raw_asset(dagbag) -> None:
    from airflow.sdk import AssetAny

    dag = dagbag.dags["transform_lakehouse"]
    # One follow-up run consumes every queued event (Airflow 3.4 default
    # is one run per event).
    assert dag.timetable.batch_asset_events is True
    condition = dag.timetable.asset_condition
    assert isinstance(condition, AssetAny)
    emitted = set().union(*RAW_OUTLETS.values())
    assert {asset.uri for asset in condition.objects} == {
        f"odp-raw://{source}/" for source in emitted
    }
    # Only the transform consumes the raw Assets.
    consumers = {
        dag_id
        for dag_id, other in dagbag.dags.items()
        if type(other.timetable).__name__ == "AssetTriggeredTimetable"
    }
    assert consumers == {"transform_lakehouse"}


def test_no_dag_triggers_another_dag_directly(dagbag) -> None:
    triggers = {
        (dag_id, task.task_id)
        for dag_id, dag in dagbag.dags.items()
        for task in dag.tasks
        if type(task).__name__ == "TriggerDagRunOperator"
    }
    assert triggers == set()


# P3-12 source refresh matrix: scheduled sources and their cron, in UTC.
SCHEDULED_FULL_REFRESH = {
    "ingest_dummyjson_products": "0 3 * * 1",
    "ingest_ibge_localidades": "0 3 1 * *",
}
MANUAL_ONLY = ("ingest_olist", "ingest_demo_storefront", "ingest_postgres_oltp")


def test_full_refresh_sources_follow_the_refresh_matrix(dagbag) -> None:
    for dag_id, cron in SCHEDULED_FULL_REFRESH.items():
        dag = dagbag.dags[dag_id]
        assert type(dag.timetable).__name__ == "CronTriggerTimetable"
        assert dag.timetable.expression == cron
        assert str(dag.timetable.timezone) == "UTC"
        # The run does not depend on a window: the next full fetch covers a
        # missed one, so there is nothing to catch up.
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        assert dag.is_paused_upon_creation is True


def test_manual_sources_have_no_schedule(dagbag) -> None:
    for dag_id in MANUAL_ONLY:
        dag = dagbag.dags[dag_id]
        assert type(dag.timetable).__name__ == "NullTimetable"
        assert dag.is_paused_upon_creation is True
