"""Unit-test fixtures: one synthetic raw object per payload format.

Full-data live runs use ``ODP_RAW_BUCKET`` (default ``odp-raw``) via
``make live-bronze`` / ``make live-silver``. These helpers are only for
deterministic unit tests. Values are generated for the check. They are not
Olist, IBGE, or DummyJSON extracts. The clickstream body comes from the
storefront generator.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from open_data_platform.contracts.load import load_contracts
from open_data_platform.ingestion.demo_storefront import (
    ingest_demo_storefront,
)
from open_data_platform.ingestion.dummyjson_products import page_object_name
from open_data_platform.ingestion.ibge_localidades import OBJECT_NAME as IBGE_OBJECT
from open_data_platform.ingestion.writer import write_raw_object

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from open_data_platform.contracts.model import ContractField, DataContract

LIVE_INGESTION_TS = datetime(2026, 10, 31, 12, 0, tzinfo=UTC)
LIVE_INGESTION_TS_TEXT = "2026-10-31T12:00:00Z"
LIVE_EVENT_START = datetime(2026, 10, 18, tzinfo=UTC)
LIVE_SEED = 7
LIVE_CLICKSTREAM_COUNT = 100

CSV_SOURCE = "olist_brazilian_ecommerce"
CSV_DATASET = "product_category_name_translation"
CSV_EXECUTION_ID = "p203-csv"
CSV_OBJECT = "product_category_name_translation.csv"

JSONL_SOURCE = "demo_storefront"
JSONL_DATASET = "clickstream"
JSONL_EXECUTION_ID = "p203-jsonl"

ARRAY_SOURCE = "ibge_localidades"
ARRAY_DATASET = "municipios"
ARRAY_EXECUTION_ID = "p203-json-array"

PAGE_SOURCE = "dummyjson_products"
PAGE_DATASET = "products"
PAGE_EXECUTION_ID = "p203-json-page"


@dataclass(frozen=True, slots=True)
class LiveRawFixture:
    """One synthetic raw object a later layer can reload by identity."""

    payload_format: str
    source_id: str
    dataset: str
    execution_id: str


def live_raw_fixtures() -> tuple[LiveRawFixture, ...]:
    """The four payload formats, in CSV, JSONL, JSON array, JSON page order."""
    return (
        LiveRawFixture("csv", CSV_SOURCE, CSV_DATASET, CSV_EXECUTION_ID),
        LiveRawFixture("jsonl", JSONL_SOURCE, JSONL_DATASET, JSONL_EXECUTION_ID),
        LiveRawFixture("json-array", ARRAY_SOURCE, ARRAY_DATASET, ARRAY_EXECUTION_ID),
        LiveRawFixture("json-page", PAGE_SOURCE, PAGE_DATASET, PAGE_EXECUTION_ID),
    )


LIVE_DATASETS: tuple[tuple[str, str], ...] = tuple(
    (item.source_id, item.dataset) for item in live_raw_fixtures()
)

_CSV_TABLE = (
    "iceberg.bronze.olist_brazilian_ecommerce__product_category_name_translation"
)
_JSONL_TABLE = "iceberg.bronze.demo_storefront__clickstream"
_ARRAY_TABLE = "iceberg.bronze.ibge_localidades__municipios"
_PAGE_TABLE = "iceberg.bronze.dummyjson_products__products"


def write_live_fixtures(client: BaseClient, *, bucket: str) -> None:
    """Land the four fixture objects. The same execution identity overwrites."""
    contracts = {(item.source_id, item.dataset): item for item in load_contracts()}
    write_raw_object(
        client,
        bucket=bucket,
        body=_category_csv_bytes(),
        source_id=CSV_SOURCE,
        dataset=CSV_DATASET,
        execution_id=CSV_EXECUTION_ID,
        object_name=CSV_OBJECT,
        ingestion_ts=LIVE_INGESTION_TS,
        content_type="text/csv",
    )
    ingest_demo_storefront(
        client,
        bucket=bucket,
        execution_id=JSONL_EXECUTION_ID,
        seed=LIVE_SEED,
        count=LIVE_CLICKSTREAM_COUNT,
        event_time_start=LIVE_EVENT_START,
        ingestion_ts=LIVE_INGESTION_TS,
        contracts=tuple(contracts.values()),
    )
    write_raw_object(
        client,
        bucket=bucket,
        body=_municipios_bytes(contracts[(ARRAY_SOURCE, ARRAY_DATASET)]),
        source_id=ARRAY_SOURCE,
        dataset=ARRAY_DATASET,
        execution_id=ARRAY_EXECUTION_ID,
        object_name=IBGE_OBJECT,
        ingestion_ts=LIVE_INGESTION_TS,
        content_type="application/json",
    )
    write_raw_object(
        client,
        bucket=bucket,
        body=_products_page_bytes(contracts[(PAGE_SOURCE, PAGE_DATASET)]),
        source_id=PAGE_SOURCE,
        dataset=PAGE_DATASET,
        execution_id=PAGE_EXECUTION_ID,
        object_name=page_object_name(0),
        ingestion_ts=LIVE_INGESTION_TS,
        content_type="application/json",
    )


def live_checks() -> tuple[tuple[str, str], ...]:
    """Trino statements and the count each one must return."""
    null_products = _clickstream_null_products()
    metadata = (
        f"source_id = '{{source}}' AND execution_id = '{{execution}}' "
        f"AND \"_ingestion_ts\" = '{LIVE_INGESTION_TS_TEXT}'"
    )
    return (
        ("3", _count(_CSV_TABLE)),
        ("2", _count(_CSV_TABLE, "product_category_name = 'categoria_teste'")),
        ("1", _count(_CSV_TABLE, "product_category_name_english IS NULL")),
        ("1", _count(_CSV_TABLE, "product_category_name_english = 'test_category'")),
        (
            "1",
            _count(
                _CSV_TABLE,
                "product_category_name_english = 'test_category_again'",
            ),
        ),
        (
            "3",
            _count(
                _CSV_TABLE,
                metadata.format(source=CSV_SOURCE, execution=CSV_EXECUTION_ID),
            ),
        ),
        (str(LIVE_CLICKSTREAM_COUNT), _count(_JSONL_TABLE)),
        (str(null_products), _count(_JSONL_TABLE, "product_id IS NULL")),
        (
            str(LIVE_CLICKSTREAM_COUNT),
            _count(
                _JSONL_TABLE,
                metadata.format(source=JSONL_SOURCE, execution=JSONL_EXECUTION_ID),
            ),
        ),
        ("2", _count(_ARRAY_TABLE)),
        ("1", _count(_ARRAY_TABLE, "\"municipio-nome\" = 'São Teste'")),
        ("1", _count(_ARRAY_TABLE, '"microrregiao-id" IS NULL')),
        (
            "2",
            _count(
                _ARRAY_TABLE,
                metadata.format(source=ARRAY_SOURCE, execution=ARRAY_EXECUTION_ID),
            ),
        ),
        ("2", _count(_PAGE_TABLE)),
        ("1", _count(_PAGE_TABLE, "brand IS NULL")),
        ("2", _count(_PAGE_TABLE, """tags = '["demo"]'""")),
        (
            "2",
            _count(
                _PAGE_TABLE,
                "total = '2' AND \"skip\" = '0' AND \"limit\" = '2'",
            ),
        ),
        (
            "2",
            _count(
                _PAGE_TABLE,
                metadata.format(source=PAGE_SOURCE, execution=PAGE_EXECUTION_ID),
            ),
        ),
    )


def _count(table: str, predicate: str | None = None) -> str:
    statement = f"SELECT COUNT(*) AS row_count FROM {table}"
    if predicate is not None:
        statement = f"{statement} WHERE {predicate}"
    return statement


def _category_csv_bytes() -> bytes:
    text = (
        "product_category_name,product_category_name_english\n"
        "categoria_teste,test_category\n"
        "categoria_teste,test_category_again\n"
        "categoria_vazia,\n"
    )
    return "\ufeff".encode("utf-8") + text.encode("utf-8")


def _municipios_bytes(contract: DataContract) -> bytes:
    rows = [
        _municipio(contract, 9000001, name="São Teste", null_regions=True),
        _municipio(contract, 9000002, name="Teste Dois", null_regions=False),
    ]
    return _dump(rows)


def _municipio(
    contract: DataContract,
    municipio_id: int,
    *,
    name: str,
    null_regions: bool,
) -> dict[str, object]:
    row: dict[str, object] = {}
    for field in contract.fields:
        if field.name == "municipio-id":
            row[field.name] = municipio_id
        elif field.name == "municipio-nome":
            row[field.name] = name
        elif null_regions and field.name.startswith(("microrregiao", "mesorregiao")):
            row[field.name] = None
        else:
            row[field.name] = _sample_value(field, municipio_id)
    return row


def _products_page_bytes(contract: DataContract) -> bytes:
    if contract.record_locator != "products":
        msg = "dummyjson_products.products must locate the products array"
        raise ValueError(msg)
    products = [
        _product(contract, 1, include_brand=True),
        _product(contract, 2, include_brand=False),
    ]
    page: dict[str, object] = {}
    for field in contract.envelope_fields:
        if field.name == contract.record_locator:
            page[field.name] = products
        elif field.name == "total":
            page[field.name] = 2
        elif field.name == "skip":
            page[field.name] = 0
        elif field.name == "limit":
            page[field.name] = 2
        else:
            msg = f"unexpected DummyJSON envelope field: {field.name}"
            raise ValueError(msg)
    return _dump(page)


def _product(
    contract: DataContract,
    product_id: int,
    *,
    include_brand: bool,
) -> dict[str, object]:
    row: dict[str, object] = {}
    for field in contract.fields:
        if field.name == "brand" and not include_brand:
            continue
        if field.name == "id":
            row[field.name] = product_id
        else:
            row[field.name] = _sample_value(field, product_id)
    return row


def _sample_value(field: ContractField, token: int) -> object:
    if field.field_type == "string":
        return f"demo-{token}"
    if field.field_type == "integer":
        return token
    if field.field_type == "number":
        return 1.5
    if field.field_type == "boolean":
        return True
    if field.field_type == "json":
        if field.name in {"tags", "images"}:
            return ["demo"]
        if field.name == "reviews":
            return [{"comment": "demo"}]
        return {"demo": token}
    if field.field_type in {"timestamp", "date"}:
        return "2026-10-31T00:00:00Z"
    msg = f"unsupported sample type: {field.field_type}"
    raise ValueError(msg)


def _clickstream_null_products() -> int:
    from open_data_platform.ingestion.demo_storefront import (
        generate_clickstream_jsonl,
    )

    body = generate_clickstream_jsonl(
        seed=LIVE_SEED,
        count=LIVE_CLICKSTREAM_COUNT,
        event_time_start=LIVE_EVENT_START,
    )
    return sum(
        1
        for line in body.decode("utf-8").splitlines()
        if json.loads(line)["product_id"] is None
    )


def _dump(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
