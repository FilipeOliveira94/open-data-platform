"""Exact decimal JSON and the decimal contract rule. No storage."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from open_data_platform.contracts import decimals


def test_json_numbers_with_a_fraction_are_read_as_decimals() -> None:
    payload = decimals.json_loads('{"price": 10.50, "count": 2, "rate": 1e2}')

    assert payload == {"price": Decimal("10.50"), "count": 2, "rate": Decimal("1E+2")}
    assert str(payload["price"]) == "10.50"  # type: ignore[index]


def test_json_dumps_writes_a_decimal_as_its_exact_text() -> None:
    value = {"price": Decimal("10.50"), "nested": [Decimal("0.05"), None, True]}

    assert decimals.json_dumps(value) == '{"price":10.50,"nested":[0.05,null,true]}'


def test_json_dumps_matches_json_dumps_without_decimals() -> None:
    value = {"name": "São Paulo", "n": 3, "x": 1.5, "items": [{"a": None}], "t": (1,)}

    expected = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    assert decimals.json_dumps(value) == expected


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), float("nan")])
def test_json_dumps_rejects_a_number_that_is_not_finite(value: object) -> None:
    with pytest.raises(ValueError):
        decimals.json_dumps({"price": value})


def test_json_dumps_rejects_keys_and_values_json_cannot_hold() -> None:
    with pytest.raises(TypeError):
        decimals.json_dumps({1: "a"})
    with pytest.raises(TypeError):
        decimals.json_dumps({"a": object()})


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1234567890.12", True),
        ("0.05", True),
        ("-0.00", True),
        ("10.5", True),
        ("1E+2", True),
        ("0", True),
        ("9.999", False),
        ("58.900", False),
        ("12345678901", False),
        ("NaN", False),
        ("Infinity", False),
    ],
)
def test_fits_counts_digits_as_written(text: str, expected: bool) -> None:
    assert decimals.fits(Decimal(text), 12, 2) is expected


def test_a_value_with_no_integer_digits_fits_a_scale_only_decimal() -> None:
    assert decimals.fits(Decimal("0.05"), 2, 2)
    assert not decimals.fits(Decimal("1.05"), 2, 2)


def test_plain_text_has_no_exponent() -> None:
    assert decimals.plain_text(Decimal("1E+2")) == "100"
    assert decimals.plain_text(Decimal("10.50")) == "10.50"
    assert decimals.plain_text(Decimal("-0.00")) == "-0.00"
