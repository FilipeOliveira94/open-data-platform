"""Incremental refresh (P3-13, ADR-0024). No Spark, Trino, MinIO, or Postgres."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fake_s3 import FakeS3

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.model import DataContract
from open_data_platform.ingestion.writer import write_raw_object
from open_data_platform.lakehouse import medallion_build, refresh
from open_data_platform.lakehouse.bronze import promote_bronze
from open_data_platform.lakehouse.fixtures import land_complete_execution
from open_data_platform.lakehouse.medallion_build import MedallionBuildError
from open_data_platform.lakehouse.promotion_registry import (
    GOLD,
    GOLD_DATASET,
    SILVER,
    InMemoryPromotionRegistry,
)
from open_data_platform.lakehouse.write_mode import WriteMode

RAW = "odp-raw"
WAREHOUSE = "odp-warehouse"
DATASET = ("demo_storefront", "clickstream")
PLANTED = "planted-event-value-xyz"


def _contract(identity: tuple[str, str] = DATASET) -> DataContract:
    return next(
        item for item in load_contracts() if (item.source_id, item.dataset) == identity
    )


def _events(*ids: str) -> bytes:
    return b"".join(
        json.dumps(
            {
                "event_id": event_id,
                "event_time": "2026-11-01T00:00:00.000Z",
                "event_type": "page_view",
                "session_id": "s",
                "user_id": "u",
                "product_id": None,
            }
        ).encode()
        + b"\n"
        for event_id in ids
    )


def _land(store: FakeS3, execution_id: str, *ids: str, day: int = 1) -> str:
    return land_complete_execution(
        store,  # type: ignore[arg-type]
        bucket=RAW,
        body=_events(*ids),
        source_id=DATASET[0],
        dataset=DATASET[1],
        execution_id=execution_id,
        object_name="events.jsonl",
        content_type="application/x-ndjson",
        ingestion_ts=datetime(2026, 11, day, 3, tzinfo=UTC),
    )


class _Spark:
    """Records the SQL files Spark would run; fails on request."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.files: list[str] = []
        self.sql: list[str] = []
        self.fail_on = fail_on

    def __call__(self, path: Path) -> None:
        self.files.append(path.name)
        self.sql.append(path.read_text(encoding="utf-8"))
        if self.fail_on is not None and path.name == self.fail_on:
            msg = f"spark failed for {path.name}"
            raise MedallionBuildError(msg)


def _bronze(
    store: FakeS3,
    registry: InMemoryPromotionRegistry,
    work: Path,
    *,
    spark: _Spark | None = None,
    full_rebuild: bool = False,
    label: str = "run",
) -> tuple[refresh.BronzeRefresh, _Spark]:
    runner = spark or _Spark()
    store.reads.clear()
    store.uploads.clear()
    result = refresh.refresh_bronze(
        store,  # type: ignore[arg-type]
        contracts=(_contract(),),
        registry=registry,
        run_label=label,
        work=work / label,
        raw_bucket=RAW,
        warehouse_bucket=WAREHOUSE,
        run_spark=runner,
        full_rebuild=full_rebuild,
    )
    return result, runner


def test_first_run_bootstraps_then_no_new_objects_writes_nothing(
    tmp_path: Path,
) -> None:
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    _land(store, "run-a", "e1", "e2")

    first, spark = _bronze(store, registry, tmp_path, label="first")
    entry = first.datasets[0]
    assert entry.mode is WriteMode.FULL_OVERWRITE
    assert spark.files == ["demo_storefront__clickstream.sql"]
    assert "CREATE OR REPLACE TABLE bronze." in spark.sql[0]
    assert first.table_rows == {"demo_storefront.clickstream": 2}
    assert set(registry.pending(SILVER)) == {DATASET}
    # Staging is per run label and is removed after registration.
    assert store.uploads == [
        "bronze/_staging/first/demo_storefront/clickstream/rows.jsonl"
    ]
    assert not any(key.startswith("bronze/_staging/") for _b, key in store.objects)

    second, spark = _bronze(store, registry, tmp_path, label="second")
    assert spark.files == []
    assert not second.wrote
    assert store.reads == []  # no object body and no marker read
    assert store.uploads == []
    record = registry.refreshes[("second", *DATASET)]
    assert (record.mode, record.objects_listed, record.objects_promoted) == (
        "none",
        1,
        0,
    )
    counts = (tmp_path / "second" / refresh.COUNTS_FILE).read_text(encoding="utf-8")
    assert counts == ""


def test_new_execution_is_merged_and_reads_only_new_objects(tmp_path: Path) -> None:
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    _land(store, "run-a", "e1", "e2")
    _bronze(store, registry, tmp_path, label="first")

    new_key = _land(store, "run-b", "e3", PLANTED, day=2)
    result, spark = _bronze(store, registry, tmp_path, label="second")
    entry = result.datasets[0]
    assert entry.mode is WriteMode.INCREMENTAL_APPEND
    assert [item.raw_object_key for item in entry.objects] == [new_key]
    assert store.reads == [
        new_key.rsplit("/", 1)[0] + "/_COMPLETE",
        new_key,
    ]
    sql = spark.sql[0]
    assert "MERGE INTO bronze.`demo_storefront__clickstream` AS target" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "NOT MATCHED BY SOURCE" not in sql
    assert "bronze/_staging/second/demo_storefront/clickstream/rows.jsonl" in sql
    assert "CREATE OR REPLACE TABLE" not in sql
    # Row values stay in the staging file, never in the SQL.
    assert PLANTED not in sql
    assert result.table_rows == {"demo_storefront.clickstream": 4}


def test_staging_file_holds_the_planted_value_and_sql_does_not(tmp_path: Path) -> None:
    store = FakeS3()
    _land(store, "run-a", PLANTED)
    promotion = promote_bronze(
        store,  # type: ignore[arg-type]
        raw_bucket=RAW,
        warehouse_bucket=WAREHOUSE,
        contracts=(_contract(),),
        run_label="plant",
        work_dir=tmp_path,
    )
    staged = store.objects[
        (WAREHOUSE, "bronze/_staging/plant/demo_storefront/clickstream/rows.jsonl")
    ]["Body"]
    assert isinstance(staged, bytes)
    assert PLANTED.encode() in staged
    assert PLANTED not in promotion.sql
    assert '"_raw_etag":"' in staged.decode()


def test_incomplete_execution_waits_for_its_marker(tmp_path: Path) -> None:
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    _land(store, "run-a", "e1")
    _bronze(store, registry, tmp_path, label="first")
    write_raw_object(
        store,  # type: ignore[arg-type]
        bucket=RAW,
        body=_events("e9"),
        source_id=DATASET[0],
        dataset=DATASET[1],
        execution_id="run-partial",
        object_name="events.jsonl",
        ingestion_ts=datetime(2026, 11, 2, 3, tzinfo=UTC),
        content_type="application/x-ndjson",
    )
    result, spark = _bronze(store, registry, tmp_path, label="second")
    assert spark.files == []
    assert result.datasets[0].objects_incomplete == 1
    assert len(registry.promoted(*DATASET)) == 1

    _land(store, "run-partial", "e9", day=2)  # the marker lands
    result, spark = _bronze(store, registry, tmp_path, label="third")
    assert len(result.datasets[0].objects) == 1
    assert len(registry.promoted(*DATASET)) == 2


def test_a_rewritten_object_replaces_its_old_rows_in_the_same_merge(
    tmp_path: Path,
) -> None:
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    key = _land(store, "run-a", "e1", "e2")
    _bronze(store, registry, tmp_path, label="first")
    _land(store, "run-a", "e1")  # same execution id: same key, new ETag
    result, spark = _bronze(store, registry, tmp_path, label="second")
    assert result.datasets[0].replaced_keys == (key,)
    sql = spark.sql[0]
    assert "WHEN NOT MATCHED BY SOURCE" in sql
    assert f"target.`raw_object_key` IN ('{key}')" in sql
    assert result.table_rows == {"demo_storefront.clickstream": 1}


def test_spark_failure_leaves_the_dataset_unregistered_and_a_retry_restages(
    tmp_path: Path,
) -> None:
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    _land(store, "run-a", "e1")
    _bronze(store, registry, tmp_path, label="first")
    registry.complete(
        SILVER,
        DATASET,
        registry.pending(SILVER)[DATASET],
        run_label="x",
        mark_next=None,
    )
    _land(store, "run-b", "e2", day=2)
    with pytest.raises(MedallionBuildError):
        _bronze(
            store,
            registry,
            tmp_path,
            label="second",
            spark=_Spark(fail_on="demo_storefront__clickstream.sql"),
        )
    assert len(registry.promoted(*DATASET)) == 1
    assert registry.pending(SILVER) == {}
    assert registry.refreshes[("second", *DATASET)].mode == "incremental_append:failed"

    retry, spark = _bronze(store, registry, tmp_path, label="second")
    assert len(retry.datasets[0].objects) == 1
    assert "MERGE INTO" in spark.sql[0]
    assert len(registry.promoted(*DATASET)) == 2
    assert set(registry.pending(SILVER)) == {DATASET}


def test_full_rebuild_replaces_every_table_from_all_complete_objects(
    tmp_path: Path,
) -> None:
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    _land(store, "run-a", "e1")
    _land(store, "run-b", "e2", day=2)
    _bronze(store, registry, tmp_path, label="first")
    result, spark = _bronze(
        store, registry, tmp_path, label="rebuild", full_rebuild=True
    )
    assert result.datasets[0].mode is WriteMode.FULL_OVERWRITE
    assert len(result.datasets[0].objects) == 2
    assert "CREATE OR REPLACE TABLE bronze." in spark.sql[0]
    assert result.table_rows == {"demo_storefront.clickstream": 2}


def test_silver_rebuilds_only_pending_datasets_and_marks_gold(tmp_path: Path) -> None:
    registry = InMemoryPromotionRegistry()
    work = tmp_path / "run"
    work.mkdir()
    spark = _Spark()
    assert (
        refresh.refresh_silver(
            registry=registry,
            run_label="run",
            work=work,
            warehouse_bucket=WAREHOUSE,
            run_spark=spark,
        )
        == ()
    )
    assert spark.files == []
    assert refresh.read_silver_datasets(work) == ()

    registry.record_promotion(*DATASET, (), run_label="b", replace_dataset=False)
    failing = _Spark(fail_on="demo_storefront__clickstream.sql")
    with pytest.raises(MedallionBuildError):
        refresh.refresh_silver(
            registry=registry,
            run_label="run",
            work=work,
            warehouse_bucket=WAREHOUSE,
            run_spark=failing,
        )
    assert set(registry.pending(SILVER)) == {DATASET}
    assert refresh.gold_mark(registry) is None

    done = refresh.refresh_silver(
        registry=registry,
        run_label="run",
        work=work,
        warehouse_bucket=WAREHOUSE,
        run_spark=spark,
    )
    assert done == (DATASET,)
    assert spark.files == ["demo_storefront__clickstream.sql"]
    assert registry.pending(SILVER) == {}
    assert set(registry.pending(GOLD)) == {GOLD_DATASET}
    assert refresh.read_silver_datasets(work) == (DATASET,)


def test_a_mark_made_during_a_write_survives_its_completion() -> None:
    registry = InMemoryPromotionRegistry()
    registry.record_promotion(*DATASET, (), run_label="a", replace_dataset=False)
    read_before = registry.pending(SILVER)[DATASET]
    registry.record_promotion(*DATASET, (), run_label="b", replace_dataset=False)
    registry.complete(SILVER, DATASET, read_before, run_label="a", mark_next=GOLD)
    assert set(registry.pending(SILVER)) == {DATASET}


class _Recorder:
    def __init__(self, statuses: list[int] | None = None) -> None:
        self.calls: list[list[str]] = []
        self.statuses = list(statuses or [])

    def __call__(self, command, env) -> int:
        del env
        self.calls.append(list(command))
        return self.statuses.pop(0) if self.statuses else 0


def test_gold_steps_run_only_while_gold_is_pending(tmp_path: Path) -> None:
    registry = InMemoryPromotionRegistry()
    recorder = _Recorder()
    assert not medallion_build.run_step(
        "dbt-build", tmp_path, {}, recorder, registry=registry
    )
    assert not medallion_build.run_step(
        "stage-quarantine", tmp_path, {}, recorder, registry=registry
    )
    assert recorder.calls == []

    registry.complete(SILVER, DATASET, None, run_label="s", mark_next=GOLD)
    with pytest.raises(MedallionBuildError, match="dbt-build exited with status 2"):
        medallion_build.run_step(
            "dbt-build", tmp_path, {}, _Recorder([2]), registry=registry
        )
    assert refresh.gold_mark(registry) is not None
    assert medallion_build.run_step(
        "dbt-build", tmp_path, {}, recorder, registry=registry
    )
    assert recorder.calls[0][1] == "build"
    assert refresh.gold_mark(registry) is None


def test_bronze_checks_skip_when_no_table_was_written(tmp_path: Path) -> None:
    (tmp_path / refresh.COUNTS_FILE).write_text("", encoding="utf-8")
    assert not medallion_build.run_step("bronze-checks", tmp_path, {}, _Recorder())


def test_silver_checks_skip_when_silver_wrote_nothing(tmp_path: Path) -> None:
    (tmp_path / refresh.SILVER_DATASETS_FILE).write_text("", encoding="utf-8")
    assert not medallion_build.run_step("silver-checks", tmp_path, {}, _Recorder())


def test_main_returns_the_skip_exit_code_when_nothing_ran(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(medallion_build, "run_step", lambda *a, **k: False)
    argv = ["dbt-build", "--work-root", str(tmp_path), "--run-label", "r"]
    assert medallion_build.main([*argv, "--skip-exit-code", "99"]) == 99
    assert medallion_build.main(argv) == 0


class _ChecksRan(Exception):
    """Raised by the fake Trino query: the check step tried to check."""


@pytest.mark.xfail(
    strict=True,
    reason="review R1: a failed check is bypassed by the next refresh; P3-16",
)
def test_a_failed_bronze_check_is_checked_again_before_silver_builds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run 1 writes Bronze and its checks fail. Run 2 has no new raw object.

    Silver is still pending on the Bronze that failed, so the check step of
    run 2 must check again, not skip. Today it skips, and the DAG's
    ``none_failed`` rule then builds Silver on it.
    """
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    _land(store, "run-a", "e1", "e2")
    first, _spark = _bronze(store, registry, tmp_path, label="first")
    assert first.wrote
    # bronze_checks of "first" failed here; Silver did not run.
    second, _spark = _bronze(store, registry, tmp_path, label="second")
    assert not second.wrote
    assert set(registry.pending(SILVER)) == {DATASET}

    def query(_statement: str, _env: object) -> str:
        raise _ChecksRan

    monkeypatch.setattr(medallion_build, "trino_scalar", query)
    with pytest.raises(_ChecksRan):
        medallion_build.run_step(
            "bronze-checks", tmp_path / "second", {}, _Recorder(), registry=registry
        )
