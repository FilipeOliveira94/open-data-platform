"""P3-12 live check: land raw executions through the real ingestion code.

Each step calls the shipped ingestion function (DummyJSON pages, storefront
generator) against a dedicated raw bucket, with a scripted HTTP transport in
place of the public API. The completion markers are the ones the ingestion
writes. Run from the repository root:

    uv run python scripts/p3_12_land_fixtures.py <step>

Steps:

    reset          delete every object under the two check prefixes
    products-a     full_refresh run A: 3 products on two pages, complete
    products-b     full_refresh run B: 2 products (the third removed), complete
    products-c     full_refresh run C: page 0 lands, page 2 fails (incomplete)
    products-d     run D: the same 2 products as B (re-ingest of one snapshot)
    clicks-a       append_only batch A: 50 events
    clicks-a-again the same 50 events under a new execution id (duplicates)
    clicks-late    batch L: 20 events whose event_time precedes batch A

Prints keys and counts only, never record values.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from open_data_platform.contracts.load import load_contracts
from open_data_platform.ingestion.demo_storefront import ingest_demo_storefront
from open_data_platform.ingestion.dummyjson_products import ingest_dummyjson_products
from open_data_platform.ingestion.http import HttpRequest, HttpResponse
from open_data_platform.object_storage import client_from_env, ensure_bucket

FIXTURES = Path("tests/fixtures/dummyjson_products")
PREFIXES = ("raw/dummyjson_products/products/", "raw/demo_storefront/clickstream/")
CLICK_START = datetime(2026, 11, 10, tzinfo=UTC)
LATE_START = datetime(2026, 11, 1, tzinfo=UTC)


def _page(name: str, *, total: int | None = None) -> bytes:
    page = json.loads((FIXTURES / name).read_bytes())
    if total is not None:
        page["total"] = total
    return json.dumps(page).encode()


class _Transport:
    """Serve a page per ``skip``; ``None`` answers with HTTP 404."""

    def __init__(self, pages: dict[int, bytes | None]) -> None:
        self.pages = pages

    def __call__(self, request: HttpRequest, timeout: float) -> HttpResponse:
        del timeout
        skip = int(request.url.split("skip=", 1)[1].split("&", 1)[0])
        body = self.pages.get(skip)
        if body is None:
            return HttpResponse(status=404, headers={}, body=b"")
        return HttpResponse(status=200, headers={}, body=body)


def _products(client: object, bucket: str, step: str) -> None:
    runs = {
        "products-a": (
            "p312-a",
            datetime(2026, 11, 20, 1, tzinfo=UTC),
            {0: _page("page-skip-0.json"), 2: _page("page-skip-2.json")},
        ),
        "products-b": (
            "p312-b",
            datetime(2026, 11, 20, 2, tzinfo=UTC),
            {0: _page("page-skip-0.json", total=2)},
        ),
        "products-c": (
            "p312-c",
            datetime(2026, 11, 20, 3, tzinfo=UTC),
            {0: _page("page-skip-0.json"), 2: None},
        ),
        "products-d": (
            "p312-d",
            datetime(2026, 11, 20, 4, tzinfo=UTC),
            {0: _page("page-skip-0.json", total=2)},
        ),
    }
    execution_id, ingestion_ts, pages = runs[step]
    try:
        results = ingest_dummyjson_products(
            client,
            bucket=bucket,
            execution_id=execution_id,
            ingestion_ts=ingestion_ts,
            limit=2,
            base_url="https://dummyjson.invalid",
            transport=_Transport(pages),
            sleep=lambda _seconds: None,
            max_attempts=1,
        )
    except Exception as exc:  # noqa: BLE001 - scripted failure, any type
        print(f"{step}: ingestion failed as scripted: {type(exc).__name__}")
        return
    print(f"{step}: {len(results)} page objects, execution {execution_id}")


def _clicks(client: object, bucket: str, step: str) -> None:
    runs = {
        "clicks-a": ("p312-clicks-a", 11, CLICK_START, 50),
        "clicks-a-again": ("p312-clicks-a2", 11, CLICK_START, 50),
        "clicks-late": ("p312-clicks-late", 12, LATE_START, 20),
    }
    execution_id, seed, start, count = runs[step]
    contracts = load_contracts()
    result = ingest_demo_storefront(
        client,
        bucket=bucket,
        execution_id=execution_id,
        seed=seed,
        count=count,
        event_time_start=start,
        ingestion_ts=datetime(2026, 11, 20, 5, tzinfo=UTC),
        contracts=contracts,
    )
    print(f"{step}: {count} events at {result.key}")


def _reset(client: object, bucket: str) -> None:
    ensure_bucket(client, bucket)  # type: ignore[arg-type]
    deleted = 0
    for prefix in PREFIXES:
        token: str | None = None
        while True:
            kwargs: dict[str, object] = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            response = client.list_objects_v2(**kwargs)  # type: ignore[attr-defined]
            for item in response.get("Contents", []):
                client.delete_object(Bucket=bucket, Key=item["Key"])  # type: ignore[attr-defined]
                deleted += 1
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
    print(f"reset: {deleted} objects deleted from {bucket}")


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    step = argv[0]
    bucket = os.environ["ODP_P3_12_RAW_BUCKET"]
    if bucket == os.environ.get("ODP_RAW_BUCKET"):
        print("refusing to write check data into ODP_RAW_BUCKET", file=sys.stderr)
        return 2
    client = client_from_env(os.environ)
    if step == "reset":
        _reset(client, bucket)
    elif step.startswith("products-"):
        _products(client, bucket, step)
    elif step.startswith("clicks-"):
        _clicks(client, bucket, step)
    else:
        print(f"unknown step: {step}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
