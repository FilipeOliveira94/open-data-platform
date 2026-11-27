"""P3-07: quarantine exclusion or flag, quality counts, and the volume guard.

SQL is inspected as text; no Spark, Trino, or MinIO.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fake_s3 import FakeS3

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.model import DataContract
from open_data_platform.lakehouse import medallion_build, refresh
from open_data_platform.lakehouse.bronze import (
    BronzeParseError,
    RawObject,
    parse_quarantine_reasons,
    quarantine_key_for,
    rows_from_object,
)
from open_data_platform.lakehouse.fixtures import land_complete_execution
from open_data_platform.lakehouse.medallion_build import MedallionBuildError
from open_data_platform.lakehouse.promotion_registry import (
    BRONZE,
    InMemoryPromotionRegistry,
)
from open_data_platform.lakehouse.reconciliation import (
    silver_quality_checks,
    silver_relation_sql,
)
from open_data_platform.lakehouse.silver.cli import policy_for
from open_data_platform.lakehouse.silver.common import (
    SilverError,
    quality_metrics,
    render_silver_sql,
)

RAW = "odp-raw"
WAREHOUSE = "odp-warehouse"
PLANTED = "planted-quarantine-value-q7"


def _contract(source_id: str, dataset: str) -> DataContract:
    return next(
        item
        for item in load_contracts()
        if (item.source_id, item.dataset) == (source_id, dataset)
    )


def _sql(contract: DataContract, **kwargs: object) -> str:
    return render_silver_sql(contract, WAREHOUSE, policy_for(contract), **kwargs)  # type: ignore[arg-type]


def test_exclude_drops_quarantined_rows_and_flag_keeps_them() -> None:
    excluded = _sql(_contract("olist_synth_cdc", "orders"))
    assert "WHERE `order_id` IS NOT NULL AND NOT `_quarantined`" in excluded
    assert "'quarantine_excluded'" in excluded

    flagged = _sql(_contract("olist_brazilian_ecommerce", "order_reviews"))
    assert "AND NOT `_quarantined`" not in flagged
    assert "WHERE `review_id` IS NOT NULL AND `order_id` IS NOT NULL\n" in flagged
    assert "'quarantine_flagged'" in flagged
    # Both keep the flag and the sidecar reasons as Silver columns.
    for sql in (excluded, flagged):
        assert "`_quarantine_reasons` IS NOT NULL AS `_quarantined`" in sql
        assert "CAST(`_quarantine_reasons` AS STRING) AS `_quarantine_reasons`" in sql


def test_every_typed_column_has_a_cast_null_count() -> None:
    orders = _contract("olist_brazilian_ecommerce", "orders")
    metrics = quality_metrics(orders, policy_for(orders))
    timestamps = [f.name for f in orders.fields if f.field_type == "timestamp"]
    assert [name for name, metric in metrics if metric == "cast_null"] == timestamps
    assert ("order_id", "null_key_dropped") in metrics
    assert ("_quarantine_reasons", "quarantine_flagged") in metrics
    sql = _sql(orders)
    assert f"LATERAL VIEW stack({len(metrics)}, " in sql
    # A present value the cast turned into NULL is what is counted.
    assert (
        "(`order_approved_at` IS NOT NULL AND COALESCE(TRY_TO_TIMESTAMP("
        "`order_approved_at`, 'yyyy-MM-dd HH:mm:ss')"
    ) in sql


def test_json_expansions_count_their_cast_nulls() -> None:
    products = _contract("dummyjson_products", "products")
    names = {
        name
        for name, metric in quality_metrics(products, policy_for(products))
        if metric == "cast_null"
    }
    assert {"price", "dimensions_width", "meta_created_at", "tags"} <= names
    # Plain strings cannot fail a cast and are not counted.
    assert "title" not in names
    assert "meta_barcode" not in names


def test_counts_merge_on_the_run_key_and_replace_a_rerun() -> None:
    sql = _sql(_contract("olist_synth_cdc", "orders"), run_label="run-7")
    assert "MERGE INTO silver_audit.`quality_counts` AS target" in sql
    assert "SELECT 'run-7' AS run_label, 'olist_synth_cdc' AS source_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    # The target is fixed to this run and dataset in the ON clause, with no
    # NOT MATCHED BY SOURCE clause, so other runs' files are pruned (P3-08).
    merge = sql[sql.index("MERGE INTO silver_audit") :]
    assert (
        "ON target.run_label = 'run-7' AND target.source_id = 'olist_synth_cdc' "
        "AND target.dataset = 'orders' AND " in merge
    )
    assert "NOT MATCHED BY SOURCE" not in merge
    # The counts are written after the replace, from the same typed view.
    assert sql.index("CREATE OR REPLACE TABLE silver.") < sql.index("MERGE INTO")
    assert sql.count("FROM `typed_olist_synth_cdc__orders`") == 2


def test_run_label_must_be_one_path_segment() -> None:
    with pytest.raises(SilverError, match="run label"):
        _sql(_contract("olist_synth_cdc", "orders"), run_label="a'; DROP")


@pytest.mark.parametrize(
    ("identity", "ratio"),
    [
        (("olist_brazilian_ecommerce", "orders"), "1.0"),
        (("dummyjson_products", "products"), "0.9"),
        (("ibge_localidades", "municipios"), "0.99"),
    ],
)
def test_guarded_patterns_fail_before_the_replace(
    identity: tuple[str, str], ratio: str
) -> None:
    sql = _sql(_contract(*identity))
    guard = f"previous_rows IS NULL OR latest_rows >= previous_rows * {ratio},"
    assert guard in sql
    assert sql.index(guard) < sql.index("CREATE OR REPLACE TABLE silver.")
    assert "GROUP BY `dt`, `execution_id`" in sql


@pytest.mark.parametrize(
    "identity", [("olist_synth_cdc", "orders"), ("demo_storefront", "clickstream")]
)
def test_unguarded_patterns_have_no_volume_guard(identity: tuple[str, str]) -> None:
    assert "volume guard" not in _sql(_contract(*identity)).lower()


def test_allow_volume_drop_skips_only_the_guard() -> None:
    sql = _sql(_contract("dummyjson_products", "products"), allow_volume_drop=True)
    assert "latest_rows" not in sql
    assert "-- Volume guard skipped for this run (--allow-volume-drop)." in sql
    assert "CREATE OR REPLACE TABLE silver." in sql


def test_a_guarded_contract_without_a_ratio_is_rejected() -> None:
    contract = replace(_contract("dummyjson_products", "products"), min_row_ratio=None)
    with pytest.raises(SilverError, match="volume_guard"):
        _sql(contract)


def test_relation_check_leaves_excluded_rows_out_of_the_bronze_groups() -> None:
    excluded = _contract("olist_synth_cdc", "orders")
    flagged = _contract("olist_brazilian_ecommerce", "orders")
    assert "AND _quarantine_reasons IS NULL GROUP BY" in silver_relation_sql(
        excluded, policy_for(excluded)
    )
    assert "_quarantine_reasons" not in silver_relation_sql(
        flagged, policy_for(flagged)
    )


def test_quality_checks_expect_every_metric_of_the_latest_run() -> None:
    excluded = _contract("olist_synth_cdc", "orders")
    checks = silver_quality_checks(excluded, policy_for(excluded))
    expected = len(quality_metrics(excluded, policy_for(excluded)))
    assert len(checks) == 2
    assert f"WHEN recorded = {expected} THEN 'ok'" in checks[0][1]
    assert "max_by(run_label, recorded_at)" in checks[0][1]
    assert "quarantined_in_silver" in checks[1][1]
    flagged = _contract("olist_brazilian_ecommerce", "orders")
    assert len(silver_quality_checks(flagged, policy_for(flagged))) == 1


def test_sidecar_reasons_are_sorted_and_joined_per_record() -> None:
    body = (
        b'{"record_index":1,"reason":"type_mismatch","record":{}}\n'
        b'{"record_index":1,"reason":"duplicate_primary_key","record":{}}\n'
        b"\n"
        b'{"record_index":0,"reason":"null_violation","record":{}}\n'
    )
    assert parse_quarantine_reasons(body, key="k") == {
        0: "null_violation",
        1: "duplicate_primary_key,type_mismatch",
    }


@pytest.mark.parametrize(
    "line",
    [
        b"not json",
        b"[]",
        b'{"record_index":"0","reason":"null_violation"}',
        b'{"record_index":0,"reason":"' + PLANTED.encode() + b'"}',
    ],
)
def test_a_bad_sidecar_line_fails_without_echoing_its_value(line: bytes) -> None:
    with pytest.raises(BronzeParseError) as caught:
        parse_quarantine_reasons(line + b"\n", key="quarantine/x.jsonl")
    assert PLANTED not in str(caught.value)
    assert "quarantine/x.jsonl: line 0" in str(caught.value)


def test_quarantine_key_mirrors_the_raw_key() -> None:
    raw = "raw/olist_synth_cdc/orders/dt=2026-11-01/execution_id=run-a/orders.json"
    assert quarantine_key_for(raw) == (
        "quarantine/olist_synth_cdc/orders/dt=2026-11-01/execution_id=run-a/"
        "orders.json.jsonl"
    )


def test_a_sidecar_index_past_the_payload_fails_the_object() -> None:
    contract = _contract("demo_storefront", "clickstream")
    raw = RawObject(
        key="raw/demo_storefront/clickstream/dt=2026-11-01/execution_id=r/e.jsonl",
        body=_event_lines("e1"),
        metadata={
            "source_id": "demo_storefront",
            "execution_id": "r",
            "_ingestion_ts": "2026-11-01T03:00:00+00:00",
        },
        quarantine_reasons={3: "null_violation"},
    )
    with pytest.raises(BronzeParseError, match="past the payload"):
        rows_from_object(contract, raw)


def _event_lines(*ids: str, bad: str | None = None) -> bytes:
    return b"".join(
        json.dumps(
            {
                "event_id": event_id,
                "event_time": "2026-11-01T00:00:00.000Z",
                "event_type": bad if event_id == "e2" and bad else "page_view",
                "session_id": "s",
                "user_id": "u",
                "product_id": None,
            }
        ).encode()
        + b"\n"
        for event_id in ids
    )


def test_bronze_stages_the_reason_and_not_the_planted_value(tmp_path: Path) -> None:
    store = FakeS3()
    registry = InMemoryPromotionRegistry()
    key = land_complete_execution(
        store,  # type: ignore[arg-type]
        bucket=RAW,
        body=_event_lines("e1", "e2"),
        source_id="demo_storefront",
        dataset="clickstream",
        execution_id="run-a",
        object_name="events.jsonl",
        content_type="application/x-ndjson",
        ingestion_ts=datetime(2026, 11, 1, 3, tzinfo=UTC),
    )
    finding = {
        "record_index": 1,
        "reason": "type_mismatch",
        "field": "event_time",
        "primary_key": {"event_id": PLANTED},
        "record": {"event_id": PLANTED},
    }
    store.put_object(
        Bucket=RAW,
        Key=quarantine_key_for(key),
        Body=(json.dumps(finding) + "\n").encode(),
    )
    sql: list[str] = []
    staged: list[bytes] = []

    def spark(path: Path) -> None:
        sql.append(path.read_text(encoding="utf-8"))
        for (_bucket, name), item in store.objects.items():
            if name.startswith("bronze/_staging/"):
                staged.append(item["Body"])  # type: ignore[arg-type]

    refresh.refresh_bronze(
        store,  # type: ignore[arg-type]
        contracts=(_contract("demo_storefront", "clickstream"),),
        registry=registry,
        run_label="run",
        work=tmp_path / "run",
        raw_bucket=RAW,
        warehouse_bucket=WAREHOUSE,
        run_spark=spark,
    )
    assert PLANTED not in sql[0]
    assert "_quarantine_reasons" in sql[0]
    rows = [json.loads(line) for line in staged[0].splitlines()]
    assert [row["_quarantine_reasons"] for row in rows] == [None, "type_mismatch"]
    assert PLANTED.encode() not in staged[0]


def test_refresh_rejects_allow_volume_drop_for_an_unguarded_dataset(
    tmp_path: Path,
) -> None:
    with pytest.raises(refresh.RefreshError, match="names no guarded dataset"):
        refresh.refresh_silver(
            registry=InMemoryPromotionRegistry(),
            run_label="run",
            work=tmp_path,
            warehouse_bucket=WAREHOUSE,
            run_spark=lambda _path: None,
            allow_volume_drop=frozenset({("olist_synth_cdc", "orders")}),
        )


def test_refresh_skips_the_guard_only_for_the_named_dataset(tmp_path: Path) -> None:
    registry = InMemoryPromotionRegistry()
    for identity in (
        ("dummyjson_products", "products"),
        ("ibge_localidades", "municipios"),
    ):
        registry.record_promotion(*identity, (), run_label="b", replace_dataset=False)
    for dataset, check in registry.open_checks(BRONZE).items():
        registry.record_check(
            BRONZE, dataset, check.marked_at, passed=True, run_label="c"
        )
    scripts: dict[str, str] = {}

    def spark(path: Path) -> None:
        scripts[path.name] = path.read_text(encoding="utf-8")

    refresh.refresh_silver(
        registry=registry,
        run_label="run-9",
        work=tmp_path,
        warehouse_bucket=WAREHOUSE,
        run_spark=spark,
        allow_volume_drop=frozenset({("dummyjson_products", "products")}),
    )
    assert "latest_rows" not in scripts["dummyjson_products__products.sql"]
    assert "latest_rows" in scripts["ibge_localidades__municipios.sql"]
    assert "'run-9' AS run_label" in scripts["ibge_localidades__municipios.sql"]


@pytest.mark.parametrize("value", ["products", "a.b.c", ".x", "x."])
def test_allow_volume_drop_argument_needs_source_and_dataset(value: str) -> None:
    with pytest.raises(MedallionBuildError, match="SOURCE.DATASET"):
        medallion_build.parse_dataset_names([value])


def test_allow_volume_drop_argument_parses_identities() -> None:
    assert medallion_build.parse_dataset_names(
        ["dummyjson_products.products", "dummyjson_products.products"]
    ) == frozenset({("dummyjson_products", "products")})


def test_staged_findings_carry_the_silver_action_of_their_contract() -> None:
    from open_data_platform.lakehouse.quarantine_stage import (
        QuarantineStageError,
        encode_findings_jsonl,
        iter_quarantine_rows,
        silver_actions,
    )

    actions = silver_actions()
    assert actions[("olist_synth_cdc", "orders")] == "excluded"
    assert actions[("olist_brazilian_ecommerce", "order_reviews")] == "flagged"
    store = FakeS3()
    line = (
        b'{"record_index":0,"reason":"null_violation","field":"x","primary_key":{}}\n'
    )
    olist_synth = (
        "quarantine/olist_synth_cdc/orders/dt=2026-11-01/execution_id=r/o.json.jsonl"
    )
    store.put_object(Bucket=RAW, Key=olist_synth, Body=line)
    rows = tuple(iter_quarantine_rows(store, RAW, [olist_synth]))  # type: ignore[arg-type]
    assert [row.silver_action for row in rows] == ["excluded"]
    assert b'"silver_action":"excluded"' in encode_findings_jsonl(rows)

    unknown = (
        "quarantine/olist_synth_cdc/refunds/dt=2026-11-01/execution_id=r/o.json.jsonl"
    )
    store.put_object(Bucket=RAW, Key=unknown, Body=line)
    with pytest.raises(QuarantineStageError, match="no quarantine policy"):
        tuple(iter_quarantine_rows(store, RAW, [unknown]))  # type: ignore[arg-type]
