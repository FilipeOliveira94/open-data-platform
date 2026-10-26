"""Generate a fictional storefront clickstream and land it in raw storage.

One run writes ``events.jsonl`` under ``raw/demo_storefront/clickstream/``.
Each line is one user action. The body is a pure function of ``seed``,
``count``, and ``event_time_start``. Ingestion time stays on object metadata.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.records import (
    PayloadStructureError,
    load_jsonl_records,
)
from open_data_platform.ingestion.quarantine import (
    InvalidRecordsError,
    prepare_quarantine,
    write_quarantine_object,
)
from open_data_platform.ingestion.writer import RawWriteResult, write_raw_object
from open_data_platform.object_storage import client_from_env

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from open_data_platform.contracts.model import DataContract

SOURCE_ID = "demo_storefront"
DATASET = "clickstream"
OBJECT_NAME = "events.jsonl"
CONTENT_TYPE = "application/x-ndjson"
EVENTS_PER_SECOND = 500
MAX_SEED = 0xFFFFFFFF
EVENT_TYPES: tuple[str, ...] = ("page_view", "add_to_cart", "purchase")
_EVENT_INTERVAL = timedelta(milliseconds=1000 // EVENTS_PER_SECOND)
_RECORD_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_time",
    "event_type",
    "session_id",
    "user_id",
    "product_id",
)


class DemoStorefrontError(ValueError):
    """The clickstream batch cannot be generated or written."""


def generate_clickstream_jsonl(
    *,
    seed: int,
    count: int,
    event_time_start: datetime,
) -> bytes:
    """Return deterministic JSONL for one fictional-storefront batch.

    Actions are spaced at ``EVENTS_PER_SECOND`` on the event clock. The
    function does not sleep. ``event_time_start`` is truncated to milliseconds
    in UTC, then each next action adds 2 milliseconds.
    """
    checked_seed = _require_seed(seed)
    checked_count = _require_count(count)
    start = _require_event_time_start(event_time_start)
    lines = [
        json.dumps(
            _event_record(checked_seed, index, start),
            sort_keys=True,
            separators=(",", ":"),
        )
        for index in range(checked_count)
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def ingest_demo_storefront(
    client: BaseClient,
    *,
    bucket: str,
    execution_id: str,
    seed: int,
    count: int,
    event_time_start: datetime,
    ingestion_ts: datetime | None = None,
    contracts: tuple[DataContract, ...] | None = None,
) -> RawWriteResult:
    """Write one clickstream object. The same execution identity overwrites it."""
    _require_token("bucket", bucket)
    _require_token("execution_id", execution_id)
    if ingestion_ts is not None and ingestion_ts.tzinfo is None:
        msg = "ingestion_ts must include a timezone"
        raise DemoStorefrontError(msg)
    body = generate_clickstream_jsonl(
        seed=seed,
        count=count,
        event_time_start=event_time_start,
    )
    contract = _require_contract(contracts)
    records = _clickstream_records(body, contract)
    ts = datetime.now(UTC) if ingestion_ts is None else ingestion_ts
    try:
        sidecar = prepare_quarantine(contract, records)
    except InvalidRecordsError as exc:
        raise DemoStorefrontError(str(exc)) from exc
    result = write_raw_object(
        client,
        bucket=bucket,
        body=body,
        source_id=SOURCE_ID,
        dataset=DATASET,
        execution_id=execution_id,
        object_name=OBJECT_NAME,
        ingestion_ts=ts,
        content_type=CONTENT_TYPE,
    )
    write_quarantine_object(
        client,
        bucket=bucket,
        source_id=SOURCE_ID,
        dataset=DATASET,
        execution_id=execution_id,
        object_name=OBJECT_NAME,
        ingestion_ts=result.metadata.ingestion_ts,
        body=sidecar,
    )
    return result


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint. Arguments are checked before any storage client."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate a fictional storefront clickstream and write it to raw storage."
        ),
    )
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--count", required=True, type=int)
    parser.add_argument(
        "--event-time-start",
        required=True,
        help="Timezone-aware ISO-8601 start of the event clock.",
    )
    parser.add_argument(
        "--ingestion-ts",
        default=None,
        help="Timezone-aware ISO-8601 write time. The same value overwrites the key.",
    )
    args = parser.parse_args(argv)
    try:
        _run_cli(
            execution_id=args.execution_id,
            seed=args.seed,
            count=args.count,
            event_time_start=args.event_time_start,
            ingestion_ts=args.ingestion_ts,
        )
    except (DemoStorefrontError, KeyError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _run_cli(
    *,
    execution_id: str,
    seed: int,
    count: int,
    event_time_start: str,
    ingestion_ts: str | None,
) -> None:
    _require_token("execution_id", execution_id)
    _require_seed(seed)
    _require_count(count)
    parsed_start = _require_event_time_start(
        _parse_timestamp("event-time-start", event_time_start)
    )
    parsed_ingestion = (
        _parse_timestamp("ingestion-ts", ingestion_ts) if ingestion_ts else None
    )
    env = os.environ
    bucket = env.get("ODP_RAW_BUCKET", "").strip()
    if not bucket:
        msg = "ODP_RAW_BUCKET is required"
        raise DemoStorefrontError(msg)
    client = client_from_env(env)
    ingest_demo_storefront(
        client,
        bucket=bucket,
        execution_id=execution_id,
        seed=seed,
        count=count,
        event_time_start=parsed_start,
        ingestion_ts=parsed_ingestion,
    )


def _event_record(seed: int, index: int, start: datetime) -> dict[str, str | None]:
    event_type = EVENT_TYPES[(seed + index) % len(EVENT_TYPES)]
    product_id: str | None = f"p-{(seed + index) % 50:04d}"
    if event_type == "page_view" and (seed + index) % 2 == 0:
        product_id = None
    event_time = start + (_EVENT_INTERVAL * index)
    return {
        "event_id": f"{seed:08x}-{index:08d}",
        "event_time": _format_event_time(event_time),
        "event_type": event_type,
        "session_id": f"s-{(seed + index // 5) % 1000:04d}",
        "user_id": f"u-{(seed + index // 20) % 200:04d}",
        "product_id": product_id,
    }


def _format_event_time(value: datetime) -> str:
    utc = value.astimezone(UTC)
    millis = utc.microsecond // 1000
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}Z"


def _require_contract(contracts: tuple[DataContract, ...] | None) -> DataContract:
    loaded = load_contracts() if contracts is None else contracts
    matches = [
        item
        for item in loaded
        if item.source_id == SOURCE_ID and item.dataset == DATASET
    ]
    if len(matches) != 1:
        msg = "missing contract for demo_storefront.clickstream"
        raise DemoStorefrontError(msg)
    contract = matches[0]
    if contract.payload_format != "jsonl":
        msg = "demo_storefront.clickstream contract must describe JSONL"
        raise DemoStorefrontError(msg)
    names = tuple(field.name for field in contract.fields)
    if names != _RECORD_FIELDS:
        msg = "demo_storefront.clickstream contract fields do not match the generator"
        raise DemoStorefrontError(msg)
    return contract


def _clickstream_records(
    body: bytes,
    contract: DataContract,
) -> tuple[dict[str, object], ...]:
    try:
        loaded = load_jsonl_records(body)
    except PayloadStructureError as exc:
        raise DemoStorefrontError(str(exc)) from exc
    required = {field.name for field in contract.fields if not field.nullable}
    allowed = {field.name for field in contract.fields}
    for index, item in enumerate(loaded):
        keys = set(item)
        if not required <= keys or not keys <= allowed:
            msg = f"clickstream line {index} does not match the contract"
            raise DemoStorefrontError(msg)
    return loaded


def _parse_timestamp(name: str, value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = f"{name} must be ISO-8601"
        raise DemoStorefrontError(msg) from exc
    if parsed.tzinfo is None:
        msg = f"{name} must include a timezone"
        raise DemoStorefrontError(msg)
    return parsed


def _require_event_time_start(value: datetime) -> datetime:
    if value.tzinfo is None:
        msg = "event_time_start must include a timezone"
        raise DemoStorefrontError(msg)
    utc = value.astimezone(UTC)
    return utc.replace(microsecond=(utc.microsecond // 1000) * 1000)


def _require_seed(seed: int) -> int:
    if isinstance(seed, bool) or not isinstance(seed, int):
        msg = "seed must be an integer"
        raise DemoStorefrontError(msg)
    if seed < 0 or seed > MAX_SEED:
        msg = "seed must be between 0 and 4294967295"
        raise DemoStorefrontError(msg)
    return seed


def _require_count(count: int) -> int:
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        msg = "count must be an integer >= 1"
        raise DemoStorefrontError(msg)
    return count


def _require_token(name: str, value: str) -> str:
    if not isinstance(value, str) or value.strip() != value or not value:
        msg = f"{name} must be a non-empty string"
        raise DemoStorefrontError(msg)
    return value
