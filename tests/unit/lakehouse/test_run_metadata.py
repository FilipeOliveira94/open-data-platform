"""Execution metadata and the Bronze partition spec (P3-08, ADR-0026).

No Spark, Trino, MinIO, or Postgres.
"""

from __future__ import annotations

import json
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fake_s3 import FakeS3

from open_data_platform.contracts.load import load_contracts
from open_data_platform.lakehouse import medallion_build, refresh
from open_data_platform.lakehouse.fixtures import land_complete_execution
from open_data_platform.lakehouse.iceberg_catalog import identity_partition_columns
from open_data_platform.lakehouse.medallion_build import (
    MedallionBuildError,
    SparkScriptError,
)
from open_data_platform.lakehouse.promotion_registry import InMemoryPromotionRegistry
from open_data_platform.lakehouse.run_log import InMemoryRunLog, one_line
from open_data_platform.lakehouse.spark_metrics import (
    MetricsCapture,
    SparkMetrics,
    TableIO,
    parse_commit_reports,
    parse_event_log,
    table_name,
)

RAW = "odp-raw"
WAREHOUSE = "odp-warehouse"
DATASET = ("demo_storefront", "clickstream")
TABLE = "bronze.demo_storefront__clickstream"
PLANTED = "planted-event-value-p308"

# A real Iceberg 1.12 commit report line from the local stack, shortened.
COMMIT_LINE = (
    "Received metrics report: CommitReport{tableName=local.bronze."
    "demo_storefront__clickstream, snapshotId=1, sequenceNumber=2, "
    "operation=append, commitMetrics=CommitMetricsResult{totalDuration="
    "TimerResult{timeUnit=NANOSECONDS, totalDuration=PT0.14S, count=1}, "
    "attempts=CounterResult{unit=COUNT, value=1}, addedDataFiles=CounterResult"
    "{unit=COUNT, value=1}, removedDataFiles=null, totalDataFiles=CounterResult"
    "{unit=COUNT, value=4}, addedRecords=CounterResult{unit=COUNT, value=3}, "
    "removedRecords=null, totalRecords=CounterResult{unit=COUNT, value=12}, "
    "addedFilesSizeInBytes=CounterResult{unit=BYTES, value=871}}, metadata={}}"
)
CTAS_LINE = (
    "Received metrics report: CommitReport{tableName=silver.demo_storefront__"
    "clickstream, snapshotId=2, sequenceNumber=1, operation=overwrite, "
    "commitMetrics=CommitMetricsResult{addedDataFiles=CounterResult{unit=COUNT,"
    " value=2}, removedDataFiles=CounterResult{unit=COUNT, value=5}, "
    "addedRecords=CounterResult{unit=COUNT, value=9}, removedRecords="
    "CounterResult{unit=COUNT, value=7}, totalRecords=CounterResult{unit=COUNT,"
    " value=9}}, metadata={}}"
)


def _scan_node(table: str, first_id: int) -> dict:
    names = (
        "number of output rows",
        "number of result data files",
        "number of skipped data files",
        "total data file size (bytes)",
    )
    return {
        "nodeName": f"BatchScan {table}",
        "metrics": [
            {"name": name, "accumulatorId": first_id + index, "metricType": "sum"}
            for index, name in enumerate(names)
        ],
        "children": [],
    }


def _event_log() -> list[str]:
    plan = {
        "nodeName": "WholeStageCodegen (1)",
        "metrics": [],
        "children": [
            _scan_node(f"local.{TABLE}", 10),
            _scan_node("s3a://odp-warehouse/t/metadata/m.json#all_manifests", 20),
        ],
    }
    events = [
        {"Event": "SparkListenerLogStart"},
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 3,
            "description": "MERGE INTO ...",
            "sparkPlanInfo": plan,
        },
        {
            "Event": "org.apache.spark.sql.execution.ui."
            "SparkListenerSQLAdaptiveExecutionUpdate",
            "executionId": 3,
            "sparkPlanInfo": plan,
        },
        {
            "Event": "org.apache.spark.sql.execution.ui."
            "SparkListenerDriverAccumUpdates",
            "executionId": 3,
            "accumUpdates": [[11, 2], [12, 5], [13, 1742], [21, 9]],
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Task End Reason": {"Reason": "Success"},
            "Task Info": {"Accumulables": [{"ID": 10, "Update": "7"}]},
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Task End Reason": {"Reason": "Success"},
            "Task Info": {"Accumulables": [{"ID": 10, "Update": 4}]},
        },
        # A failed attempt is retried; its rows are not counted twice.
        {
            "Event": "SparkListenerTaskEnd",
            "Task End Reason": {"Reason": "ExceptionFailure"},
            "Task Info": {"Accumulables": [{"ID": 10, "Update": 4}]},
        },
    ]
    return [json.dumps(event) + "\n" for event in events]


def test_event_log_gives_scan_files_bytes_and_rows_per_table() -> None:
    scans = parse_event_log(_event_log())
    assert scans == {
        TABLE: {
            "scanned_files": 2,
            "skipped_files": 5,
            "scanned_bytes": 1742,
            "rows_scanned": 11,
            "scans": 1,
        }
    }


def test_commit_reports_give_rows_and_files_written() -> None:
    commits = parse_commit_reports([COMMIT_LINE, "unrelated line\n", CTAS_LINE])
    assert commits[TABLE] == {
        "commits": 1,
        "added_files": 1,
        "removed_files": 0,
        "added_rows": 3,
        "removed_rows": 0,
        "total_rows": 12,
    }
    assert commits["silver.demo_storefront__clickstream"]["removed_files"] == 5


def test_table_names_drop_the_catalog_and_reject_paths() -> None:
    assert table_name("local.bronze.t") == "bronze.t"
    assert table_name("bronze.t") == "bronze.t"
    assert table_name("s3a://bucket/t/metadata/m.json#files") is None
    assert table_name("other.bronze.t") is None


def test_capture_is_private_and_deleted_after_collect(tmp_path: Path) -> None:
    capture = MetricsCapture(tmp_path / ".metrics-x")
    env = capture.prepare()
    assert stat.S_IMODE(capture.directory.stat().st_mode) == 0o700
    Path(env["ODP_SPARK_EVENT_DIR"], "local-1").write_text(
        "".join(_event_log()), encoding="utf-8"
    )
    Path(env["ODP_SPARK_METRICS_FILE"]).write_text(COMMIT_LINE + "\n")
    metrics = capture.collect()
    assert not capture.directory.exists()
    item = metrics.get(TABLE)
    assert item is not None
    assert (item.scanned_files, item.added_rows, item.total_rows) == (2, 3, 12)


def _fake_spark(status: int):
    calls: list[tuple[list[str], dict[str, str]]] = []

    def run(command, env) -> int:
        calls.append((list(command), dict(env)))
        Path(env["ODP_SPARK_EVENT_DIR"], "local-1").write_text("".join(_event_log()))
        Path(env["ODP_SPARK_METRICS_FILE"]).write_text(COMMIT_LINE + "\n")
        return status

    return run, calls


def test_spark_runner_returns_metrics_and_keeps_them_on_failure(
    tmp_path: Path,
) -> None:
    sql = tmp_path / "t.sql"
    sql.write_text("SELECT 1;", encoding="utf-8")
    env = {"MINIO_ROOT_PASSWORD": "secret-p308"}
    run, calls = _fake_spark(0)
    metrics = medallion_build.spark_runner("run_bronze.sh", env, run)(sql)
    assert metrics.get(TABLE) is not None
    # Secrets stay in the environment, never in the argument vector.
    assert "secret-p308" not in " ".join(calls[0][0])
    assert not (tmp_path / ".metrics-t").exists()

    failing, _ = _fake_spark(1)
    with pytest.raises(SparkScriptError) as raised:
        medallion_build.spark_runner("run_bronze.sh", env, failing)(sql)
    assert raised.value.metrics.get(TABLE) is not None


def _land(store: FakeS3, execution_id: str, *ids: str, day: int) -> None:
    body = b"".join(
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
    land_complete_execution(
        store,  # type: ignore[arg-type]
        bucket=RAW,
        body=body,
        source_id=DATASET[0],
        dataset=DATASET[1],
        execution_id=execution_id,
        object_name="events.jsonl",
        content_type="application/x-ndjson",
        ingestion_ts=datetime(2026, 11, day, 3, tzinfo=UTC),
    )


class _Spark:
    def __init__(self, metrics: SparkMetrics | None = None, fail: bool = False):
        self.sql: list[str] = []
        self.metrics = metrics or SparkMetrics()
        self.fail = fail

    def __call__(self, path: Path) -> SparkMetrics:
        self.sql.append(path.read_text(encoding="utf-8"))
        if self.fail:
            raise SparkScriptError("spark failed", self.metrics)
        return self.metrics


def _bronze(store, registry, work, spark, run_log, partitions=("dt",), label="r"):
    contract = next(c for c in load_contracts() if (c.source_id, c.dataset) == DATASET)
    return refresh.refresh_bronze(
        store,
        contracts=(contract,),
        registry=registry,
        run_label=label,
        work=work / label,
        raw_bucket=RAW,
        warehouse_bucket=WAREHOUSE,
        run_spark=spark,
        run_log=run_log,
        partition_columns=lambda _table: partitions,
    )


def test_bronze_is_partitioned_and_merge_scans_only_staged_dates(
    tmp_path: Path,
) -> None:
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    log = InMemoryRunLog()
    _land(store, "run-a", "e1", PLANTED, day=1)
    first = _Spark()
    _bronze(store, registry, tmp_path, first, log, label="first")
    assert "PARTITIONED BY (`dt`)" in first.sql[0]

    _land(store, "run-b", "e3", day=3)
    measured = SparkMetrics(
        {TABLE: TableIO(table=TABLE, scans=1, scanned_files=1, added_rows=1)}
    )
    second = _Spark(measured)
    _bronze(store, registry, tmp_path, second, log, label="second")
    merge = second.sql[0]
    assert "ON target.`dt` IN ('2026-11-03') AND target.`raw_object_key`" in merge
    assert "2026-11-01" not in merge

    run = log.datasets[("second", "bronze", *DATASET)]
    assert (run.status, run.objects_read, run.rows_read) == ("ran", 1, 1)
    assert run.rows_written == 1
    assert run.tables[0].scanned_files == 1
    # No row value reaches the SQL or the recorded metadata.
    for item in log.datasets.values():
        assert PLANTED not in repr(item)
    assert all(PLANTED not in sql for sql in first.sql + second.sql)


def test_merge_into_an_unpartitioned_table_fails_and_is_recorded(
    tmp_path: Path,
) -> None:
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    log = InMemoryRunLog()
    _land(store, "run-a", "e1", day=1)
    _bronze(store, registry, tmp_path, _Spark(), log, label="first")
    _land(store, "run-b", "e2", day=2)
    spark = _Spark()
    with pytest.raises(refresh.RefreshError, match="no dt partition spec"):
        _bronze(store, registry, tmp_path, spark, log, partitions=(), label="second")
    assert spark.sql == []
    assert log.datasets[("second", "bronze", *DATASET)].status == "failed"
    # The objects stay unregistered, so the run after the rebuild stages them.
    assert len(registry.promoted(*DATASET)) == 1


def test_failed_spark_write_records_what_it_measured(tmp_path: Path) -> None:
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    log = InMemoryRunLog()
    _land(store, "run-a", "e1", day=1)
    measured = SparkMetrics({TABLE: TableIO(table=TABLE, scans=1, scanned_files=4)})
    with pytest.raises(SparkScriptError):
        _bronze(store, registry, tmp_path, _Spark(measured, fail=True), log)
    run = log.datasets[("r", "bronze", *DATASET)]
    assert run.status == "failed"
    assert run.tables[0].scanned_files == 4


def test_run_step_records_ran_skipped_and_failed(tmp_path: Path) -> None:
    log = InMemoryRunLog()
    registry = InMemoryPromotionRegistry()
    work = tmp_path / "run-1"
    assert not medallion_build.run_step(
        "bronze-checks", work, {}, registry=registry, run_log=log
    )
    with pytest.raises(MedallionBuildError):
        medallion_build.run_step(
            "dbt-parse", work, {}, lambda _c, _e: 2, registry=registry, run_log=log
        )
    medallion_build.run_step(
        "dbt-parse", work, {}, lambda _c, _e: 0, registry=registry, run_log=log
    )
    assert [(s.step, s.status) for s in log.steps] == [
        ("bronze-checks", "skipped"),
        ("dbt-parse", "failed"),
        ("dbt-parse", "ran"),
    ]
    failed = log.steps[1]
    assert failed.run_label == "run-1"
    assert failed.error_type == "MedallionBuildError"
    assert failed.error_message == "dbt-parse exited with status 2"
    assert all(step.seconds >= 0 for step in log.steps)


def test_one_line_drops_multi_line_messages() -> None:
    assert one_line("bronze: failed") == "bronze: failed"
    assert one_line("line\nwith a value") is None
    assert len(one_line("x" * 900) or "") == 500


def test_dbt_run_results_become_gold_dataset_runs(tmp_path: Path) -> None:
    path = tmp_path / "run_results.json"
    path.write_text(
        json.dumps(
            {
                "results": [
                    {
                        "unique_id": "model.odp.dim_customers",
                        "status": "success",
                        "execution_time": 1.25,
                        "relation_name": '"iceberg"."customer_crm"."dim_customers"',
                        "adapter_response": {"rows_affected": 96096},
                    },
                    {
                        "unique_id": "model.odp.fct_orders",
                        "status": "error",
                        "execution_time": 0.5,
                        "relation_name": (
                            '"iceberg"."marketplace_operations"."fct_orders"'
                        ),
                        "adapter_response": {},
                    },
                    {"unique_id": "test.odp.not_null_x", "status": "pass"},
                    {"unique_id": "model.odp.skipped", "status": "skipped"},
                ]
            }
        ),
        encoding="utf-8",
    )
    runs = refresh.read_dbt_runs(path)
    assert [(r.source_id, r.dataset, r.status, r.rows_written) for r in runs] == [
        ("customer_crm", "dim_customers", "ran", 96096),
        ("marketplace_operations", "fct_orders", "failed", None),
    ]
    assert refresh.read_dbt_runs(tmp_path / "missing.json") == []


def test_identity_partition_columns_of_the_default_spec() -> None:
    metadata = {
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "fields": [{"id": 6, "name": "dt"}]}],
        "default-spec-id": 1,
        "partition-specs": [
            {"spec-id": 0, "fields": []},
            {
                "spec-id": 1,
                "fields": [{"source-id": 6, "transform": "identity", "name": "dt"}],
            },
        ],
    }
    assert identity_partition_columns(metadata) == ("dt",)
    metadata["default-spec-id"] = 0
    assert identity_partition_columns(metadata) == ()
