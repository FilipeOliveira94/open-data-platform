"""Record classification. No storage and no live source calls."""

from __future__ import annotations

from decimal import Decimal

import pytest

from open_data_platform.contracts.load import contract_from_mapping, load_contracts
from open_data_platform.contracts.model import DataContract
from open_data_platform.contracts.records import (
    PayloadStructureError,
    classify_records,
    encode_findings,
    load_jsonl_records,
    parsed_csv_record,
)


def _contract() -> DataContract:
    return contract_from_mapping(
        {
            "contract_version": 1,
            "source_id": "demo_storefront",
            "dataset": "clickstream",
            "description": "Classifier fixture.",
            "payload_format": "json",
            "grain": "One row.",
            "primary_key": ["id"],
            "update_pattern": "snapshot",
            "volume_guard": {"min_row_ratio": 1.0},
            "on_invalid": "quarantine_flag",
            "on_structural_error": "fail_batch",
            "governance": {"source_family": "product_analysis", "owner": "platform"},
            "provenance": {"system": "generator"},
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": False,
                    "classification": "public",
                    "description": "Identifier.",
                },
                {
                    "name": "label",
                    "type": "string",
                    "nullable": True,
                    "classification": "public",
                    "description": "Optional label.",
                },
                {
                    "name": "event_time",
                    "type": "timestamp",
                    "nullable": False,
                    "classification": "public",
                    "description": "Event time text.",
                },
                {
                    "name": "active",
                    "type": "boolean",
                    "nullable": True,
                    "classification": "public",
                    "description": "Optional flag.",
                },
            ],
            "changelog": [{"version": 1, "note": "Fixture."}],
        },
        origin="test",
    )


def _row(**overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "id": 1,
        "label": None,
        "event_time": "2026-10-26T00:00:00Z",
        "active": None,
    }
    record.update(overrides)
    return record


def test_nullable_null_and_omitted_nullable_are_valid() -> None:
    contract = _contract()
    omitted = _row()
    del omitted["label"]

    assert classify_records(contract, [_row(id=1), omitted | {"id": 2}]) == ()


def test_null_primary_key_and_bad_type_are_reported() -> None:
    contract = _contract()

    findings = classify_records(
        contract,
        [_row(id=None, event_time=""), _row(id="abc")],
    )

    assert [(item.record_index, item.field, item.reason) for item in findings] == [
        (0, "id", "null_violation"),
        (0, "event_time", "type_mismatch"),
        (1, "id", "type_mismatch"),
    ]


def test_every_row_in_a_duplicate_key_is_reported() -> None:
    contract = _contract()

    findings = classify_records(
        contract,
        [_row(id=None), _row(id=7, label="a"), _row(id=7, label="b")],
    )

    assert [(item.record_index, item.reason) for item in findings] == [
        (0, "null_violation"),
        (1, "duplicate_primary_key"),
        (2, "duplicate_primary_key"),
    ]
    assert findings[1].field is None
    assert findings[1].primary_key == {"id": 7}


def test_empty_primary_key_does_not_quarantine_repeated_values() -> None:
    contract = next(
        item
        for item in load_contracts()
        if item.source_id == "olist_brazilian_ecommerce"
        and item.dataset == "geolocation"
    )
    row = {
        field.name: 1
        if field.field_type == "integer"
        else 1.5
        if field.field_type == "number"
        else "value"
        for field in contract.fields
    }

    assert contract.primary_key == ()
    assert classify_records(contract, [row, dict(row)]) == ()


def test_csv_empty_cell_is_null_and_bad_tokens_keep_their_text() -> None:
    contract = _contract()
    empty = parsed_csv_record(
        contract,
        {"id": "", "label": "", "event_time": "", "active": ""},
    )
    bad_integer = parsed_csv_record(
        contract,
        {"id": "01", "label": "ok", "event_time": "t", "active": "TRUE"},
    )
    parsed_flag = parsed_csv_record(
        contract,
        {"id": "4", "label": "ok", "event_time": "t", "active": "false"},
    )

    assert empty["id"] is None
    assert empty["label"] is None
    empty_findings = classify_records(contract, [empty])
    assert {item.field for item in empty_findings} == {"id", "event_time"}
    assert {item.reason for item in empty_findings} == {"null_violation"}
    assert classify_records(contract, [bad_integer])[0].reason == "type_mismatch"
    assert bad_integer["active"] == "TRUE"
    assert parsed_flag["active"] is False
    assert classify_records(contract, [parsed_flag]) == ()
    published = parsed_csv_record(
        contract,
        {"id": "5", "label": "ok", "event_time": "t", "active": "True"},
    )
    assert published["active"] is True
    assert classify_records(contract, [published]) == ()


def test_jsonl_rejects_a_line_that_is_not_an_object() -> None:
    with pytest.raises(PayloadStructureError, match="line 1 is not JSON"):
        load_jsonl_records(b'{"id": 1}\nnot-json\n')


def _money_contract() -> DataContract:
    return contract_from_mapping(
        {
            "contract_version": 1,
            "source_id": "postgres_oltp",
            "dataset": "order_items",
            "description": "Decimal classifier fixture.",
            "payload_format": "json",
            "grain": "One row.",
            "primary_key": ["id"],
            "update_pattern": "incremental_window",
            "on_invalid": "quarantine_flag",
            "on_structural_error": "fail_batch",
            "governance": {"source_family": "olist_analysis", "owner": "platform"},
            "provenance": {"system": "postgres"},
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": False,
                    "classification": "internal",
                    "description": "Identifier.",
                },
                {
                    "name": "amount",
                    "type": "decimal",
                    "precision": 12,
                    "scale": 2,
                    "nullable": False,
                    "classification": "internal",
                    "description": "Amount in BRL.",
                },
                {
                    "name": "rate",
                    "type": "number",
                    "nullable": True,
                    "classification": "internal",
                    "description": "A double.",
                },
            ],
            "changelog": [{"version": 1, "note": "Fixture."}],
        },
        origin="test",
    )


def test_a_decimal_that_fits_is_valid() -> None:
    rows = [
        {"id": 1, "amount": Decimal("1234567890.12")},
        {"id": 2, "amount": Decimal("0.05")},
        {"id": 3, "amount": 20},
        {"id": 4, "amount": Decimal("10.5")},
        {"id": 5, "amount": Decimal("-0.00")},
    ]

    assert classify_records(_money_contract(), rows) == ()


@pytest.mark.parametrize(
    "amount",
    [
        Decimal("9.999"),
        Decimal("58.900"),
        Decimal("12345678901.00"),
        Decimal("NaN"),
        10.5,
        "10.50",
        True,
    ],
)
def test_a_decimal_that_needs_rounding_or_was_a_float_is_a_type_mismatch(
    amount: object,
) -> None:
    findings = classify_records(_money_contract(), [{"id": 1, "amount": amount}])

    assert [(item.field, item.reason) for item in findings] == [
        ("amount", "type_mismatch")
    ]


def test_csv_decimal_cells_become_exact_decimals() -> None:
    contract = _money_contract()
    parsed = parsed_csv_record(contract, {"id": "1", "amount": "58.90", "rate": "0.1"})

    assert isinstance(parsed["amount"], Decimal)
    assert str(parsed["amount"]) == "58.90"
    assert classify_records(contract, [parsed]) == ()
    for text in ("1e2", "058.90", ".5", "5."):
        cell = parsed_csv_record(contract, {"id": "2", "amount": text, "rate": ""})
        assert cell["amount"] == text
        assert classify_records(contract, [cell])[0].reason == "type_mismatch"
    too_precise = parsed_csv_record(
        contract, {"id": "3", "amount": "58.901", "rate": ""}
    )
    assert too_precise["amount"] == Decimal("58.901")
    assert classify_records(contract, [too_precise])[0].reason == "type_mismatch"


def test_jsonl_numbers_are_read_as_exact_decimals() -> None:
    records = load_jsonl_records(b'{"id": 1, "amount": 10.50, "rate": 0.25}\n')

    assert str(records[0]["amount"]) == "10.50"
    assert classify_records(_money_contract(), records) == ()


def test_a_number_beyond_the_double_range_is_still_a_type_mismatch() -> None:
    row = {"id": 1, "amount": Decimal("1.00"), "rate": Decimal("1E+400")}

    findings = classify_records(_money_contract(), [row])

    assert [(item.field, item.reason) for item in findings] == [
        ("rate", "type_mismatch")
    ]


def test_findings_keep_the_exact_decimal_text() -> None:
    findings = classify_records(
        _money_contract(),
        [{"id": None, "amount": Decimal("10.50"), "rate": Decimal("0.25")}],
    )

    encoded = encode_findings(findings)

    assert b'"amount":10.50' in encoded
    assert b'"rate":0.25' in encoded
