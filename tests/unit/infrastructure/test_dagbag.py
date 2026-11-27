"""Load every DAG under airflow/dags with the pinned Airflow (P3-02 to P3-04).

No Compose services. Airflow uses a temporary home with a SQLite file that
the DagBag does not touch. Run behavior (success, retry, failure) is P3-09.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

# Airflow is the uv `airflow` group, not a default group (P3-10). Without it the
# whole module skips; `make test` and CI sync the group.
pytest.importorskip("airflow", reason="uv group 'airflow' is not installed")

from open_data_platform.ingestion.coverage import CADENCES  # noqa: E402
from open_data_platform.ingestion.coverage import DAILY as DAILY_CADENCE  # noqa: E402
from open_data_platform.ingestion.coverage import WEEKLY as WEEKLY_CADENCE  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
DAG_DIR = ROOT / "airflow" / "dags"

EXPECTED_DAGS = {
    "platform_health",
    "platform_env",
    "ingest_olist",
    "ingest_dummyjson_products",
    "ingest_ibge_localidades",
    "ingest_demo_storefront",
    "ingest_olist_synth_cdc",
    "seed_olist_synth_cdc",
    "transform_lakehouse",
    "reconcile_gold",
    "ingest_olist_synth_cdc_daily",
    "ingest_olist_synth_cdc_weekly",
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
CHECK_TASKS = {"bronze_checks", "silver_checks"}
PRUNE = "prune_work"

TRANSFORM_ORDER = (
    ("bronze", "bronze"),
    ("bronze_checks", "bronze-checks"),
    ("silver", "silver"),
    ("silver_checks", "silver-checks"),
    ("stage_quarantine", "stage-quarantine"),
    ("dbt_build", "dbt-build"),
)

DAILY = "ingest_olist_synth_cdc_daily"
WEEKLY = "ingest_olist_synth_cdc_weekly"

INGEST_ENTRYPOINTS = {
    "ingest_olist": ("open_data_platform.ingestion.olist",),
    "ingest_dummyjson_products": ("open_data_platform.ingestion.dummyjson_products",),
    "ingest_ibge_localidades": ("open_data_platform.ingestion.ibge_localidades",),
    "ingest_demo_storefront": ("open_data_platform.ingestion.demo_storefront",),
    "ingest_olist_synth_cdc": (
        "open_data_platform.ingestion.olist_synth_cdc",
        "extract",
    ),
    "seed_olist_synth_cdc": ("open_data_platform.ingestion.olist_synth_cdc", "seed"),
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
        # The daily and weekly extracts catch up missed windows (P3-04, P3-05).
        assert dag.catchup is (dag_id in {DAILY, WEEKLY})
        for task in dag.tasks:
            if task.task_id == "settle":
                continue  # the daily settle-lag sensor (P3-16)
            expected_retries = 0 if task.task_id in CHECK_TASKS else 2
            assert task.retries == expected_retries, (dag_id, task.task_id)
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
    refresh = [t.task_id for t in dag.topological_sort() if t.task_id != PRUNE]
    assert refresh == [task_id for task_id, _step in TRANSFORM_ORDER]
    assert set(dag.task_ids) == {PRUNE, *refresh}
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


def test_prune_work_runs_beside_the_refresh(dagbag) -> None:
    """P3-10: no edge, so it never blocks a layer or hides a failed refresh."""
    task = dagbag.dags["transform_lakehouse"].get_task(PRUNE)
    assert _vector(task) == [
        "-m",
        MEDALLION_BUILD,
        "prune-work",
        "--run-label",
        "{{ run_id }}",
        "--skip-exit-code",
        "99",
    ]
    assert task.op_kwargs == {"skip_exit_code": 99}
    assert task.upstream_task_ids == set()
    assert task.downstream_task_ids == set()
    assert task.trigger_rule == "all_success"
    assert task.pool == "default_pool"


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


def test_platform_env_runs_the_service_checks_after_imports(dagbag) -> None:
    dag = dagbag.dags["platform_env"]
    checks = {"contracts", "minio_raw", "minio_write", "olist_synth_cdc"}
    assert {task.task_id for task in dag.tasks} == {"imports", *checks}
    assert dag.get_task("imports").upstream_task_ids == set()
    for task_id in checks:
        assert dag.get_task(task_id).upstream_task_ids == {"imports"}, task_id


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
    # The coverage ledger counts gaps from the same first window.
    assert dag.start_date == CADENCES[DAILY_CADENCE].first_start
    assert dag.max_active_runs == 1
    assert set(dag.params.keys()) == {"execution_id"}
    assert sorted(dag.task_ids) == ["extract", "settle"]
    assert dag.get_task("extract").upstream_task_ids == {"settle"}
    vector = _vector(dag.get_task("extract"))
    assert vector[:3] == [
        "-m",
        "open_data_platform.ingestion.olist_synth_cdc",
        "extract",
    ]


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


RUN_AFTER = datetime(2026, 10, 18, tzinfo=UTC)


def _render_extract(dag, context: dict[str, object]) -> list[str]:
    task = dag.get_task("extract")
    context = {"dag": dag, "dag_run": SimpleNamespace(run_after=RUN_AFTER), **context}
    return [str(task.render_template(arg, context)) for arg in task.op_args]


def test_manual_extract_renders_its_own_frequency(dagbag) -> None:
    # A manual backfill must not land in the daily or weekly coverage ledger.
    dag = dagbag.dags["ingest_olist_synth_cdc"]
    run_id = "manual__2026-10-18T00:00:00+00:00"
    vector = _render_extract(
        dag,
        {
            "params": {
                "execution_id": "",
                "window_start": "2026-10-17T00:00:00+00:00",
                "window_end": "2026-10-18T00:00:00+00:00",
            },
            "run_id": run_id,
        },
    )
    flags = dict(zip(vector[3::2], vector[4::2], strict=True))
    assert flags["--window-start"] == "2026-10-17T00:00:00+00:00"
    assert flags["--window-end"] == "2026-10-18T00:00:00+00:00"
    assert flags["--frequency"] == "manual"
    assert flags["--dag-id"] == "ingest_olist_synth_cdc"
    assert flags["--airflow-run-id"] == run_id


def test_daily_window_renders_into_a_valid_extract_window(dagbag) -> None:
    import pendulum

    from open_data_platform.ingestion.olist_synth_cdc import (
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
        f"{DAILY}__{run_id}",
        "--frequency",
        "daily",
        "--dag-id",
        DAILY,
        "--airflow-run-id",
        run_id,
        "--ingestion-ts",
        RUN_AFTER.isoformat(),
    ]
    start = datetime.fromisoformat(vector[4])
    end = datetime.fromisoformat(vector[6])
    assert validate_window(start, end) == (start, end)
    name = window_object_name("sellers", start, end)
    assert name == "sellers_2026-10-17_2026-10-18.json"


def test_daily_window_without_interval_renders_empty_bounds(dagbag) -> None:
    from open_data_platform.ingestion.olist_synth_cdc import main

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


def test_weekly_dag_runs_monday_utc_midnight_with_catchup(dagbag) -> None:
    from airflow.timetables.interval import CronDataIntervalTimetable

    dag = dagbag.dags[WEEKLY]
    timetable = _core_timetable(dag)
    assert isinstance(timetable, CronDataIntervalTimetable)
    assert timetable.summary == "0 0 * * 1"
    assert str(timetable._timezone) == "UTC"
    assert dag.catchup is True
    assert dag.start_date == datetime(2026, 11, 16, tzinfo=UTC)
    # The weekly window lags the interval by one day (Sunday to Sunday).
    assert dag.start_date == CADENCES[WEEKLY_CADENCE].first_start + timedelta(days=1)
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True
    assert set(dag.params.keys()) == {"execution_id"}
    assert dag.task_ids == ["extract"]
    vector = _vector(dag.get_task("extract"))
    assert vector[:3] == [
        "-m",
        "open_data_platform.ingestion.olist_synth_cdc",
        "extract",
    ]


def test_weekly_intervals_are_monday_to_monday_in_order(dagbag) -> None:
    import pendulum
    from airflow.timetables.base import TimeRestriction

    dag = dagbag.dags[WEEKLY]
    timetable = _core_timetable(dag)
    utc = pendulum.timezone("UTC")
    restriction = TimeRestriction(
        earliest=pendulum.instance(dag.start_date), latest=None, catchup=dag.catchup
    )
    first = timetable.next_dagrun_info(
        last_automated_data_interval=None, restriction=restriction
    )
    assert first.data_interval.start == pendulum.datetime(2026, 11, 16, tz=utc)
    assert first.data_interval.end == pendulum.datetime(2026, 11, 23, tz=utc)
    assert first.run_after == first.data_interval.end
    # After a gap the scheduler runs each missed week, oldest first.
    nxt = timetable.next_dagrun_info(
        last_automated_data_interval=first.data_interval, restriction=restriction
    )
    assert nxt.data_interval.start == pendulum.datetime(2026, 11, 23, tz=utc)
    assert nxt.data_interval.end == pendulum.datetime(2026, 11, 30, tz=utc)
    # A manual run gets the last complete week before its run_after.
    manual = timetable.infer_manual_data_interval(
        run_after=pendulum.datetime(2026, 11, 21, 15, 0, tz=utc)
    )
    assert manual.start == pendulum.datetime(2026, 11, 9, tz=utc)
    assert manual.end == pendulum.datetime(2026, 11, 16, tz=utc)


def test_weekly_window_lags_the_interval_by_one_day(dagbag) -> None:
    import pendulum

    from open_data_platform.ingestion.olist_synth_cdc import (
        validate_window,
        window_object_name,
    )

    run_id = "scheduled__2026-11-23T00:00:00+00:00"
    vector = _render_extract(
        dagbag.dags[WEEKLY],
        {
            "data_interval_start": pendulum.datetime(2026, 11, 16, tz="UTC"),
            "data_interval_end": pendulum.datetime(2026, 11, 23, tz="UTC"),
            "params": {"execution_id": ""},
            "run_id": run_id,
        },
    )
    assert vector[3:] == [
        "--window-start",
        "2026-11-15T00:00:00+00:00",
        "--window-end",
        "2026-11-22T00:00:00+00:00",
        "--execution-id",
        f"{WEEKLY}__{run_id}",
        "--frequency",
        "weekly",
        "--dag-id",
        WEEKLY,
        "--airflow-run-id",
        run_id,
        "--ingestion-ts",
        RUN_AFTER.isoformat(),
    ]
    start = datetime.fromisoformat(vector[4])
    end = datetime.fromisoformat(vector[6])
    assert validate_window(start, end) == (start, end)
    assert end - start == timedelta(days=7)
    assert window_object_name("orders", start, end) == (
        "orders_2026-11-15_2026-11-22.json"
    )


def test_weekly_windows_tile_without_gaps(dagbag) -> None:
    import pendulum

    dag = dagbag.dags[WEEKLY]
    bounds = []
    for week in range(3):
        start = pendulum.datetime(2026, 11, 16, tz="UTC").add(weeks=week)
        vector = _render_extract(
            dag,
            {
                "data_interval_start": start,
                "data_interval_end": start.add(weeks=1),
                "params": {"execution_id": ""},
                "run_id": "r",
            },
        )
        bounds.append((vector[4], vector[6]))
    for previous, current in zip(bounds, bounds[1:], strict=False):
        assert previous[1] == current[0]


def test_weekly_window_without_interval_renders_empty_bounds(dagbag) -> None:
    vector = _render_extract(
        dagbag.dags[WEEKLY],
        {
            "data_interval_start": None,
            "data_interval_end": None,
            "params": {"execution_id": ""},
            "run_id": "manual__x",
        },
    )
    assert vector[4] == ""
    assert vector[6] == ""


RAW_OUTLETS = {
    "ingest_olist": {"olist_brazilian_ecommerce", "olist_marketing_funnel"},
    "ingest_dummyjson_products": {"dummyjson_products"},
    "ingest_ibge_localidades": {"ibge_localidades"},
    "ingest_demo_storefront": {"demo_storefront"},
    "ingest_olist_synth_cdc": {"olist_synth_cdc"},
    DAILY: {"olist_synth_cdc"},
    WEEKLY: {"olist_synth_cdc"},
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
MANUAL_ONLY = ("ingest_olist", "ingest_demo_storefront", "ingest_olist_synth_cdc")


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


def test_daily_and_weekly_runs_at_the_same_instant_get_distinct_executions(
    dagbag,
) -> None:
    """Both DAGs run at Monday 00:00Z with the same run id (P3-06 defect)."""
    import pendulum

    monday = pendulum.datetime(2026, 11, 23, tz="UTC")
    run_id = "scheduled__2026-11-23T00:00:00+00:00"
    daily = _render_extract(
        dagbag.dags[DAILY],
        {
            "data_interval_start": monday.subtract(days=1),
            "data_interval_end": monday,
            "params": {"execution_id": ""},
            "run_id": run_id,
        },
    )
    weekly = _render_extract(
        dagbag.dags[WEEKLY],
        {
            "data_interval_start": monday.subtract(weeks=1),
            "data_interval_end": monday,
            "params": {"execution_id": ""},
            "run_id": run_id,
        },
    )
    assert daily[8] != weekly[8]
    explicit = _render_extract(
        dagbag.dags[DAILY],
        {
            "data_interval_start": monday.subtract(days=1),
            "data_interval_end": monday,
            "params": {"execution_id": "rerun-7"},
            "run_id": run_id,
        },
    )
    assert explicit[8] == "rerun-7"


def test_daily_extract_waits_for_the_settle_lag(dagbag) -> None:
    from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
    from ingest_common import SETTLE_LAG

    from open_data_platform.ingestion import coverage

    sensor = dagbag.dags[DAILY].get_task("settle")
    assert isinstance(sensor, TimeDeltaSensor)
    assert sensor.delta == SETTLE_LAG == coverage.SETTLE_LAG
    assert sensor.mode == "reschedule"
    assert sensor.deferrable is False  # no triggerer in local Compose (D-006)


def test_every_try_of_a_run_renders_the_same_ingestion_timestamp(dagbag) -> None:
    """The ``dt=`` prefix comes from run_after, not the try's clock (R14)."""
    import pendulum

    for dag_id in (DAILY, WEEKLY):
        context = {
            "data_interval_start": pendulum.datetime(2026, 10, 17, tz="UTC"),
            "data_interval_end": pendulum.datetime(2026, 10, 18, tz="UTC"),
            "params": {"execution_id": ""},
            "run_id": "scheduled__x",
        }
        first = _render_extract(dagbag.dags[dag_id], context)
        retry = _render_extract(dagbag.dags[dag_id], context)
        flag = first.index("--ingestion-ts")
        assert first[flag + 1] == retry[flag + 1] == RUN_AFTER.isoformat()
