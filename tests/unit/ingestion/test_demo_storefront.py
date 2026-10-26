"""Unit tests for the fictional storefront clickstream generator."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.records import classify_records, load_jsonl_records
from open_data_platform.ingestion.demo_storefront import (
    EVENTS_PER_SECOND,
    DemoStorefrontError,
    generate_clickstream_jsonl,
    ingest_demo_storefront,
    main,
)

_START = datetime(2026, 10, 18, tzinfo=UTC)
_INGESTION_TS = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)


def _lines(body: bytes) -> list[dict[str, Any]]:
    text = body.decode("utf-8")
    assert text.endswith("\n")
    return [json.loads(line) for line in text.splitlines()]


def test_same_inputs_produce_the_same_bytes() -> None:
    kwargs: dict[str, Any] = {
        "seed": 7,
        "count": 20,
        "event_time_start": _START,
    }
    assert generate_clickstream_jsonl(**kwargs) == generate_clickstream_jsonl(**kwargs)


def test_another_seed_produces_another_body() -> None:
    first = generate_clickstream_jsonl(seed=7, count=5, event_time_start=_START)
    second = generate_clickstream_jsonl(seed=8, count=5, event_time_start=_START)
    assert first != second


def test_each_line_is_one_storefront_action() -> None:
    records = _lines(
        generate_clickstream_jsonl(seed=0, count=6, event_time_start=_START)
    )
    assert len(records) == 6
    previous: datetime | None = None
    for index, record in enumerate(records):
        assert set(record) == {
            "event_id",
            "event_time",
            "event_type",
            "session_id",
            "user_id",
            "product_id",
        }
        assert record["event_id"] == f"00000000-{index:08d}"
        assert record["event_type"] in {"page_view", "add_to_cart", "purchase"}
        parsed = datetime.fromisoformat(record["event_time"].replace("Z", "+00:00"))
        if previous is not None:
            assert parsed - previous == timedelta(milliseconds=2)
        previous = parsed
    assert records[0]["event_type"] == "page_view"
    assert records[0]["product_id"] is None
    assert records[1]["event_type"] == "add_to_cart"
    assert records[1]["product_id"] == "p-0001"


def test_five_hundred_actions_span_one_second_of_event_time() -> None:
    records = _lines(
        generate_clickstream_jsonl(
            seed=3,
            count=EVENTS_PER_SECOND,
            event_time_start=_START,
        )
    )
    first = datetime.fromisoformat(records[0]["event_time"].replace("Z", "+00:00"))
    last = datetime.fromisoformat(records[-1]["event_time"].replace("Z", "+00:00"))
    assert last - first == timedelta(milliseconds=998)


def test_keys_are_sorted_and_the_body_ends_with_a_newline() -> None:
    body = generate_clickstream_jsonl(seed=1, count=1, event_time_start=_START)
    line = body.decode("utf-8").splitlines()[0]
    assert line == (
        '{"event_id":"00000001-00000000","event_time":"2026-10-18T00:00:00.000Z",'
        '"event_type":"add_to_cart","product_id":"p-0001",'
        '"session_id":"s-0001","user_id":"u-0001"}'
    )


@pytest.mark.parametrize(
    ("seed", "count"),
    [(-1, 1), (2**32, 1), (True, 1), (1, 0), (1, True)],
)
def test_seed_and_count_are_rejected(seed: int, count: int) -> None:
    with pytest.raises(DemoStorefrontError):
        generate_clickstream_jsonl(seed=seed, count=count, event_time_start=_START)


def test_count_above_ten_thousand_stays_one_batch() -> None:
    records = _lines(
        generate_clickstream_jsonl(seed=1, count=10_001, event_time_start=_START)
    )
    assert len(records) == 10_001
    assert records[-1]["event_id"] == "00000001-00010000"


def test_naive_event_time_is_rejected() -> None:
    with pytest.raises(DemoStorefrontError, match="timezone"):
        generate_clickstream_jsonl(
            seed=1,
            count=1,
            event_time_start=datetime(2026, 10, 18),
        )


def test_generator_output_has_no_invalid_records() -> None:
    contract = next(
        item
        for item in load_contracts()
        if item.source_id == "demo_storefront" and item.dataset == "clickstream"
    )
    body = generate_clickstream_jsonl(seed=4, count=20, event_time_start=_START)

    assert classify_records(contract, load_jsonl_records(body)) == ()


def test_valid_clickstream_deletes_a_stale_quarantine_object() -> None:
    client = MagicMock()
    client.head_bucket.return_value = {}

    ingest_demo_storefront(
        client,
        bucket="odp-raw",
        execution_id="run-001",
        seed=4,
        count=2,
        event_time_start=_START,
        ingestion_ts=_INGESTION_TS,
    )

    keys = [call.kwargs["Key"] for call in client.put_object.call_args_list]
    assert keys == [
        "raw/demo_storefront/clickstream/dt=2026-10-25/execution_id=run-001/events.jsonl"
    ]
    deleted = client.delete_object.call_args.kwargs["Key"]
    assert deleted == (
        "quarantine/demo_storefront/clickstream/dt=2026-10-25/"
        "execution_id=run-001/events.jsonl.jsonl"
    )


def test_malformed_clickstream_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "open_data_platform.ingestion.demo_storefront.generate_clickstream_jsonl",
        lambda **_kwargs: b"[\n",
    )
    client = MagicMock()

    with pytest.raises(DemoStorefrontError, match="not JSON"):
        ingest_demo_storefront(
            client,
            bucket="odp-raw",
            execution_id="run-001",
            seed=1,
            count=1,
            event_time_start=_START,
            ingestion_ts=_INGESTION_TS,
        )

    assert client.put_object.call_count == 0
    assert client.delete_object.call_count == 0


def test_clickstream_key_mismatch_writes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "open_data_platform.ingestion.demo_storefront.generate_clickstream_jsonl",
        lambda **_kwargs: b"{}\n",
    )
    client = MagicMock()

    with pytest.raises(DemoStorefrontError, match="does not match the contract"):
        ingest_demo_storefront(
            client,
            bucket="odp-raw",
            execution_id="run-001",
            seed=1,
            count=1,
            event_time_start=_START,
            ingestion_ts=_INGESTION_TS,
        )

    assert client.put_object.call_count == 0
    assert client.delete_object.call_count == 0


def test_write_uses_the_ingestion_date_and_metadata() -> None:
    client = MagicMock()
    client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadBucket",
    )
    result = ingest_demo_storefront(
        client,
        bucket="odp-raw",
        execution_id="run-001",
        seed=4,
        count=2,
        event_time_start=_START,
        ingestion_ts=_INGESTION_TS,
    )
    assert result.key == (
        "raw/demo_storefront/clickstream/dt=2026-10-25/"
        "execution_id=run-001/events.jsonl"
    )
    client.put_object.assert_called_once()
    payload = client.put_object.call_args.kwargs
    assert payload["ContentType"] == "application/x-ndjson"
    assert payload["Metadata"]["_ingestion_ts"] == "2026-10-25T12:00:00Z"
    assert payload["Body"].count(b"\n") == 2


def test_same_execution_overwrites_and_a_new_execution_keeps_another_key() -> None:
    client = MagicMock()
    client.head_bucket.return_value = {}
    first = ingest_demo_storefront(
        client,
        bucket="odp-raw",
        execution_id="run-001",
        seed=1,
        count=1,
        event_time_start=_START,
        ingestion_ts=_INGESTION_TS,
    )
    second = ingest_demo_storefront(
        client,
        bucket="odp-raw",
        execution_id="run-001",
        seed=2,
        count=1,
        event_time_start=_START,
        ingestion_ts=_INGESTION_TS,
    )
    third = ingest_demo_storefront(
        client,
        bucket="odp-raw",
        execution_id="run-002",
        seed=1,
        count=1,
        event_time_start=_START,
        ingestion_ts=_INGESTION_TS,
    )
    assert first.key == second.key
    assert third.key != first.key
    bodies = [call.kwargs["Body"] for call in client.put_object.call_args_list]
    assert bodies[0] != bodies[1]


def test_main_rejects_a_bad_seed_before_opening_a_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_client(_env: object) -> None:
        raise AssertionError("client_from_env should not be called")

    monkeypatch.setenv("ODP_RAW_BUCKET", "odp-raw")
    monkeypatch.setattr(
        "open_data_platform.ingestion.demo_storefront.client_from_env",
        fail_client,
    )
    code = main(
        [
            "--execution-id",
            "run-001",
            "--seed",
            "-1",
            "--count",
            "1",
            "--event-time-start",
            "2026-10-18T00:00:00Z",
        ]
    )
    assert code == 1


def test_main_requires_the_raw_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ODP_RAW_BUCKET", raising=False)
    code = main(
        [
            "--execution-id",
            "run-001",
            "--seed",
            "1",
            "--count",
            "1",
            "--event-time-start",
            "2026-10-18T00:00:00Z",
        ]
    )
    assert code == 1
