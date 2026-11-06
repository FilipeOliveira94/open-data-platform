"""Bronze promotion from raw objects. No Spark, Trino, or MinIO."""

from __future__ import annotations

import io
import json
from typing import Any

import pytest
from botocore.exceptions import ClientError

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.model import ContractField, DataContract
from open_data_platform.lakehouse.bronze import (
    METADATA_COLUMNS,
    BronzeError,
    BronzeParseError,
    RawObject,
    bronze_columns,
    bronze_table_name,
    encode_staging_jsonl,
    list_raw_keys,
    main,
    parse_payload,
    promote_bronze,
    render_bronze_sql,
    require_bucket_name,
    rows_from_object,
)
from open_data_platform.lakehouse.fixtures import (
    ARRAY_EXECUTION_ID,
    CSV_EXECUTION_ID,
    JSONL_EXECUTION_ID,
    LIVE_CLICKSTREAM_COUNT,
    LIVE_DATASETS,
    LIVE_INGESTION_TS_TEXT,
    PAGE_EXECUTION_ID,
    live_checks,
    live_raw_fixtures,
    write_live_fixtures,
)

_TS = LIVE_INGESTION_TS_TEXT


def _field(
    name: str,
    field_type: str = "string",
    *,
    nullable: bool = False,
) -> ContractField:
    return ContractField(
        name=name,
        field_type=field_type,
        nullable=nullable,
        classification="public",
        description=name,
    )


def _contract(**overrides: object) -> DataContract:
    values: dict[str, object] = {
        "contract_version": 1,
        "source_id": "demo_storefront",
        "dataset": "clickstream",
        "description": "test",
        "payload_format": "json",
        "grain": "one row",
        "primary_key": ("id",),
        "fields": (_field("id", "integer"),),
        "on_invalid": "quarantine",
        "on_structural_error": "fail_batch",
        "update_pattern": "snapshot",
        "domain": "product_analysis",
        "owner": "platform",
        "provenance_system": "generator",
        "changelog": (),
    }
    values.update(overrides)
    return DataContract(**values)  # type: ignore[arg-type]


def _object(
    key: str,
    body: bytes,
    *,
    source_id: str = "demo_storefront",
    execution_id: str = "run-001",
    ingestion_ts: str = _TS,
) -> RawObject:
    return RawObject(
        key=key,
        body=body,
        metadata={
            "source_id": source_id,
            "execution_id": execution_id,
            "_ingestion_ts": ingestion_ts,
        },
    )


def _key(
    source_id: str = "demo_storefront",
    dataset: str = "clickstream",
    execution_id: str = "run-001",
    object_name: str = "events.jsonl",
) -> str:
    return (
        f"raw/{source_id}/{dataset}/dt=2026-10-31/"
        f"execution_id={execution_id}/{object_name}"
    )


class _Store:
    """In-memory S3 client covering the calls Bronze and the raw writer make."""

    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[tuple[str, str], dict[str, object]] = {}

    def head_bucket(self, *, Bucket: str) -> dict[str, object]:
        if Bucket not in self.buckets:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadBucket",
            )
        return {}

    def create_bucket(self, *, Bucket: str) -> None:
        self.buckets.add(Bucket)

    def put_object(self, **kwargs: object) -> None:
        bucket = str(kwargs["Bucket"])
        key = str(kwargs["Key"])
        body = kwargs["Body"]
        if not isinstance(body, bytes):
            msg = "body must be bytes"
            raise AssertionError(msg)
        self.buckets.add(bucket)
        metadata = kwargs.get("Metadata") or {}
        self.objects[(bucket, key)] = {
            "Body": body,
            "Metadata": dict(metadata),  # type: ignore[arg-type]
        }

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.objects.pop((Bucket, Key), None)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        item = self.objects[(Bucket, Key)]
        body = item["Body"]
        assert isinstance(body, bytes)
        return {"Body": io.BytesIO(body), "Metadata": item["Metadata"]}

    def list_objects_v2(
        self,
        *,
        Bucket: str,
        Prefix: str,
        ContinuationToken: str | None = None,
    ) -> dict[str, object]:
        del ContinuationToken
        keys = sorted(
            key
            for bucket, key in self.objects
            if bucket == Bucket and key.startswith(Prefix)
        )
        return {
            "Contents": [{"Key": key} for key in keys],
            "IsTruncated": False,
        }


def _loaded() -> dict[tuple[str, str], DataContract]:
    return {(item.source_id, item.dataset): item for item in load_contracts()}


def test_bucket_name_rejects_path_characters() -> None:
    assert require_bucket_name("odp-warehouse") == "odp-warehouse"
    with pytest.raises(BronzeError, match="simple S3 identifier"):
        require_bucket_name("odp/raw")


def test_shipped_contracts_keep_primary_keys_and_drop_the_page_array() -> None:
    loaded = load_contracts()
    assert len(loaded) == 18
    sql = render_bronze_sql([(item, 1) for item in loaded], "odp-warehouse")
    assert sql.count("CREATE TABLE IF NOT EXISTS") == 18
    assert sql.count("'format-version' = '2'") == 18
    assert "s3a://odp-warehouse/catalog" not in sql
    for contract in loaded:
        columns = bronze_columns(contract)
        assert columns[: len(METADATA_COLUMNS)] == METADATA_COLUMNS
        assert len(columns) == len(set(columns))
        assert set(contract.primary_key) <= set(columns)
        assert bronze_table_name(contract.source_id, contract.dataset) in sql
        assert (
            f"s3a://odp-warehouse/bronze/{contract.source_id}/{contract.dataset}" in sql
        )
    products = _loaded()[("dummyjson_products", "products")]
    product_columns = bronze_columns(products)
    assert "products" not in product_columns
    assert {"total", "skip", "limit"} <= set(product_columns)


def test_csv_preserves_text_nulls_duplicates_and_a_bom() -> None:
    from open_data_platform.lakehouse.fixtures import _category_csv_bytes

    contract = _loaded()[
        ("olist_brazilian_ecommerce", "product_category_name_translation")
    ]
    rows = rows_from_object(
        contract,
        _object(
            _key(
                "olist_brazilian_ecommerce",
                "product_category_name_translation",
                CSV_EXECUTION_ID,
                "product_category_name_translation.csv",
            ),
            _category_csv_bytes(),
            source_id="olist_brazilian_ecommerce",
            execution_id=CSV_EXECUTION_ID,
        ),
    )
    assert len(rows) == 3
    assert rows[0]["product_category_name"] == "categoria_teste"
    assert rows[1]["product_category_name"] == "categoria_teste"
    assert rows[1]["product_category_name_english"] == "test_category_again"
    assert rows[2]["product_category_name_english"] is None
    assert rows[0]["_ingestion_ts"] == _TS
    assert rows[0]["record_index"] == "0"
    assert rows[2]["record_index"] == "2"
    assert rows[0]["dt"] == "2026-10-31"


def test_csv_keeps_quoted_text_without_coercing_tokens() -> None:
    contract = _contract(
        payload_format="csv",
        fields=(_field("id"), _field("label")),
    )
    rows = parse_payload(contract, b'id,label\n001,"a,b"\n,True\n')
    assert rows == (
        {"id": "001", "label": "a,b"},
        {"id": None, "label": "True"},
    )


def test_csv_header_and_width_mismatches_fail() -> None:
    contract = _contract(
        payload_format="csv",
        fields=(_field("id"), _field("label")),
    )
    with pytest.raises(BronzeParseError, match="header"):
        parse_payload(contract, b"id,extra\n1,2\n")
    with pytest.raises(BronzeParseError, match="row 0"):
        parse_payload(contract, b"id,label\n1\n")


def test_json_array_keeps_nulls_wrong_types_and_nested_text() -> None:
    contract = _contract(
        fields=(
            _field("id", "integer"),
            _field("flag", "string", nullable=True),
            _field("payload", "json"),
        ),
        primary_key=("id",),
    )
    body = json.dumps(
        [
            {"id": None, "flag": True, "payload": {"k": [1, True]}},
            {"id": "x", "payload": ["demo"]},
        ]
    ).encode()
    rows = parse_payload(contract, body)
    assert rows[0]["id"] is None
    assert rows[0]["flag"] == "true"
    assert rows[0]["payload"] == '{"k":[1,true]}'
    assert rows[1]["id"] == "x"
    assert rows[1]["flag"] is None
    assert rows[1]["payload"] == '["demo"]'


def test_json_array_rejects_extra_keys_and_accepts_an_empty_array() -> None:
    contract = _contract()
    with pytest.raises(BronzeParseError, match="item 0"):
        parse_payload(contract, b'[{"id": 1, "extra": 2}]')
    assert parse_payload(contract, b"[]") == ()


def test_json_page_explodes_records_and_repeats_the_envelope() -> None:
    from open_data_platform.lakehouse.fixtures import _products_page_bytes

    contract = _loaded()[("dummyjson_products", "products")]
    rows = parse_payload(contract, _products_page_bytes(contract))
    assert len(rows) == 2
    assert rows[0]["id"] == "1"
    assert rows[0]["brand"] == "demo-1"
    assert rows[1]["brand"] is None
    assert rows[0]["tags"] == '["demo"]'
    assert rows[0]["total"] == "2"
    assert rows[0]["skip"] == "0"
    assert rows[0]["limit"] == "2"
    assert "products" not in rows[0]


def test_jsonl_clickstream_keeps_null_product_ids() -> None:
    from open_data_platform.ingestion.demo_storefront import (
        generate_clickstream_jsonl,
    )
    from open_data_platform.lakehouse.fixtures import (
        LIVE_EVENT_START,
        LIVE_SEED,
    )

    contract = _loaded()[("demo_storefront", "clickstream")]
    body = generate_clickstream_jsonl(
        seed=LIVE_SEED,
        count=LIVE_CLICKSTREAM_COUNT,
        event_time_start=LIVE_EVENT_START,
    )
    rows = rows_from_object(
        contract,
        _object(
            _key(execution_id=JSONL_EXECUTION_ID),
            body,
            execution_id=JSONL_EXECUTION_ID,
        ),
    )
    assert len(rows) == LIVE_CLICKSTREAM_COUNT
    assert rows[0]["raw_object_key"].endswith("events.jsonl")
    assert any(row["product_id"] is None for row in rows)
    assert rows[-1]["record_index"] == str(LIVE_CLICKSTREAM_COUNT - 1)


def test_metadata_mismatch_and_bad_keys_fail() -> None:
    contract = _loaded()[("demo_storefront", "clickstream")]
    body = (
        b'{"event_id":"e","event_time":"t","event_type":"page_view",'
        b'"session_id":"s","user_id":"u","product_id":null}\n'
    )
    raw = _object(_key(), body)
    broken = RawObject(
        key=raw.key,
        body=body,
        metadata={**raw.metadata, "execution_id": "other"},
    )
    with pytest.raises(BronzeParseError, match="metadata"):
        rows_from_object(contract, broken)
    with pytest.raises(BronzeParseError, match="layout"):
        rows_from_object(
            contract,
            RawObject(key="raw/not-a-key", body=body, metadata={}),
        )
    with pytest.raises(BronzeParseError, match="calendar"):
        rows_from_object(
            contract,
            _object(_key().replace("dt=2026-10-31", "dt=2026-13-40"), body),
        )


def test_two_executions_stay_distinct_and_staging_is_stable() -> None:
    contract = _contract(payload_format="jsonl", fields=(_field("id"),))
    earlier = rows_from_object(
        contract,
        _object(
            _key(execution_id="run-a"),
            b'{"id":"a"}\n',
            execution_id="run-a",
        ),
    )
    later = rows_from_object(
        contract,
        _object(
            _key(execution_id="run-b"),
            b'{"id":"b"}\n',
            execution_id="run-b",
        ),
    )
    combined = earlier + later
    assert [row["execution_id"] for row in combined] == ["run-a", "run-b"]
    encoded = encode_staging_jsonl(bronze_columns(contract), combined)
    assert encoded == encode_staging_jsonl(bronze_columns(contract), combined)
    assert json.loads(encoded.splitlines()[0])["id"] == "a"


def test_empty_prefix_and_parse_errors_do_not_write_staging() -> None:
    store = _Store()
    contract = _loaded()[("demo_storefront", "clickstream")]
    with pytest.raises(BronzeError, match="no raw objects"):
        promote_bronze(
            store,  # type: ignore[arg-type]
            raw_bucket="odp-raw",
            warehouse_bucket="odp-warehouse",
            contracts=(contract,),
        )
    assert store.objects == {}

    good = _key(execution_id="run-a")
    bad = _key(execution_id="run-b")
    good_body = (
        b'{"event_id":"e","event_time":"t","event_type":"page_view",'
        b'"session_id":"s","user_id":"u","product_id":null}\n'
    )
    store.objects[("odp-raw", good)] = {
        "Body": good_body,
        "Metadata": {
            "source_id": "demo_storefront",
            "execution_id": "run-a",
            "_ingestion_ts": _TS,
        },
    }
    store.objects[("odp-raw", bad)] = {
        "Body": b"not-jsonl\n",
        "Metadata": {
            "source_id": "demo_storefront",
            "execution_id": "run-b",
            "_ingestion_ts": _TS,
        },
    }
    with pytest.raises(BronzeParseError):
        promote_bronze(
            store,  # type: ignore[arg-type]
            raw_bucket="odp-raw",
            warehouse_bucket="odp-warehouse",
            contracts=(contract,),
        )
    assert all(bucket != "odp-warehouse" for bucket, _name in store.objects)


def test_quarantine_objects_are_not_promoted() -> None:
    store = _Store()
    contract = _loaded()[
        ("olist_brazilian_ecommerce", "product_category_name_translation")
    ]
    raw_key = _key(
        "olist_brazilian_ecommerce",
        "product_category_name_translation",
        CSV_EXECUTION_ID,
        "product_category_name_translation.csv",
    )
    quarantine_key = raw_key.replace("raw/", "quarantine/", 1) + ".jsonl"
    store.objects[("odp-bronze-live", raw_key)] = {
        "Body": (
            b"\xef\xbb\xbfproduct_category_name,product_category_name_english\n"
            b"categoria_teste,test_category\n"
        ),
        "Metadata": {
            "source_id": "olist_brazilian_ecommerce",
            "execution_id": CSV_EXECUTION_ID,
            "_ingestion_ts": _TS,
        },
    }
    store.objects[("odp-bronze-live", quarantine_key)] = {
        "Body": b'{"record":"QUARANTINE_ONLY"}\n',
        "Metadata": {
            "source_id": "olist_brazilian_ecommerce",
            "execution_id": CSV_EXECUTION_ID,
            "_ingestion_ts": _TS,
        },
    }
    promotion = promote_bronze(
        store,  # type: ignore[arg-type]
        raw_bucket="odp-bronze-live",
        warehouse_bucket="odp-warehouse",
        contracts=(contract,),
    )
    assert promotion.counts == (
        ("olist_brazilian_ecommerce.product_category_name_translation", 1),
    )
    staging = store.objects[
        (
            "odp-warehouse",
            "bronze/_staging/olist_brazilian_ecommerce/"
            "product_category_name_translation/rows.jsonl",
        )
    ]["Body"]
    assert isinstance(staging, bytes)
    assert b"QUARANTINE_ONLY" not in staging
    assert "INSERT OVERWRITE" in promotion.sql
    assert "WHERE 1 = 0" not in promotion.sql


def test_live_fixtures_are_idempotent_and_cover_each_payload_format() -> None:
    store = _Store()
    write_live_fixtures(store, bucket="odp-bronze-live")  # type: ignore[arg-type]
    loaded = _loaded()
    selected = tuple(loaded[identity] for identity in LIVE_DATASETS)
    formats = [loaded[identity].payload_format for identity in LIVE_DATASETS]
    assert formats == ["csv", "jsonl", "json", "json"]
    assert [item.payload_format for item in live_raw_fixtures()] == [
        "csv",
        "jsonl",
        "json-array",
        "json-page",
    ]
    assert loaded[LIVE_DATASETS[2]].record_locator is None
    assert loaded[LIVE_DATASETS[3]].record_locator == "products"
    first = promote_bronze(
        store,  # type: ignore[arg-type]
        raw_bucket="odp-bronze-live",
        warehouse_bucket="odp-warehouse",
        contracts=selected,
    )
    write_live_fixtures(store, bucket="odp-bronze-live")  # type: ignore[arg-type]
    second = promote_bronze(
        store,  # type: ignore[arg-type]
        raw_bucket="odp-bronze-live",
        warehouse_bucket="odp-warehouse",
        contracts=selected,
    )
    assert first.counts == (
        ("olist_brazilian_ecommerce.product_category_name_translation", 3),
        ("demo_storefront.clickstream", LIVE_CLICKSTREAM_COUNT),
        ("ibge_localidades.municipios", 2),
        ("dummyjson_products.products", 2),
    )
    assert second.sql == first.sql
    assert second.counts == first.counts
    assert len(first.scripts) == 4
    assert [name for name, _sql in first.scripts] == [
        "olist_brazilian_ecommerce__product_category_name_translation.sql",
        "demo_storefront__clickstream.sql",
        "ibge_localidades__municipios.sql",
        "dummyjson_products__products.sql",
    ]
    assert all(sql.count("INSERT OVERWRITE") == 1 for _name, sql in first.scripts)
    checks = live_checks()
    statements = "\n".join(statement for _expected, statement in checks)
    assert "São Teste" in statements
    assert '"microrregiao-id"' in statements
    assert '"limit"' in statements
    assert '"skip"' in statements
    assert "p203-csv" in statements
    assert "p203-jsonl" in statements
    assert ARRAY_EXECUTION_ID in statements
    assert PAGE_EXECUTION_ID in statements
    assert all("\t" not in statement for _expected, statement in checks)


def test_list_raw_keys_follows_the_continuation_token() -> None:
    class _Paged:
        def list_objects_v2(
            self,
            *,
            Bucket: str,
            Prefix: str,
            ContinuationToken: str | None = None,
        ) -> dict[str, Any]:
            del Bucket
            if ContinuationToken is None:
                return {
                    "Contents": [{"Key": f"{Prefix}b"}],
                    "IsTruncated": True,
                    "NextContinuationToken": "next",
                }
            return {
                "Contents": [{"Key": f"{Prefix}a"}],
                "IsTruncated": False,
            }

    keys = list_raw_keys(
        _Paged(),  # type: ignore[arg-type]
        bucket="odp-raw",
        source_id="demo_storefront",
        dataset="clickstream",
    )
    assert keys == (
        "raw/demo_storefront/clickstream/a",
        "raw/demo_storefront/clickstream/b",
    )


def test_zero_row_sql_overwrites_with_an_empty_select() -> None:
    contract = _contract(payload_format="json")
    sql = render_bronze_sql([(contract, 0)], "odp-warehouse")
    assert "WHERE 1 = 0" in sql
    assert "CREATE OR REPLACE TEMPORARY VIEW" not in sql
    assert sql.count("CAST(NULL AS STRING)") == len(bronze_columns(contract))


def test_cli_rejects_mixed_selectors(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--all", "--dataset", "demo_storefront/clickstream"]) == 1
    assert "either --all or --dataset" in capsys.readouterr().err
    assert main([]) == 1
    assert "pass --dataset" in capsys.readouterr().err
    assert main(["--print-live-checks", "--sql-out", "x"]) == 1
    assert "do not take other arguments" in capsys.readouterr().err


def test_cli_prints_live_and_fixture_checks(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["--print-live-checks"]) == 0
    live = capsys.readouterr().out
    assert "\tSELECT COUNT(*) AS row_count FROM iceberg.bronze." in live
    assert "olist_brazilian_ecommerce__orders" in live
    assert "99441\t" in live
    assert main(["--print-fixture-checks"]) == 0
    fixtures = capsys.readouterr().out
    assert "demo_storefront__clickstream" in fixtures
    assert "product_category_name_translation" in fixtures
