"""Fetch DummyJSON ``/products`` pages and land each page in raw storage.

The client calls ``GET {origin}/products?limit={n}&skip={n}`` until
``skip + limit >= total``. One response becomes one object named
``products-skip-{skip:06d}.json`` under ``raw/dummyjson_products/products/``.
The stored bytes are the response body. Unit tests inject a transport and
do not call the public API. The GET itself goes through ``ingestion.http``
with this client's retry and redirect policy.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import urlencode

from open_data_platform import __version__
from open_data_platform.contracts.load import load_contracts
from open_data_platform.ingestion.http import (
    HttpRequest,
    RedirectPolicy,
    RetryPolicy,
    Transport,
    get_with_retries,
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

SOURCE_ID = "dummyjson_products"
DATASET = "products"
DEFAULT_BASE_URL = "https://dummyjson.com"
USER_AGENT = f"open-data-platform/{__version__} (dummyjson-products)"
DEFAULT_PAGE_LIMIT = 30
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_ATTEMPTS = 3
MAX_RESPONSE_BYTES = 5 * 1024 * 1024
MAX_RETRY_AFTER_SECONDS = 30.0
MAX_PAGES = 1000
_PRODUCTS_REDIRECT = RedirectPolicy(follow=False, max_redirects=0)


class DummyJsonProductsError(ValueError):
    """A DummyJSON products page could not be fetched or landed."""


@dataclass(frozen=True, slots=True)
class _Page:
    """Envelope values needed to decide the next request."""

    total: int
    count: int
    records: tuple[dict[str, object], ...]


def page_object_name(skip: int) -> str:
    """Return the raw object name for the page that starts at ``skip``."""
    if isinstance(skip, bool) or not isinstance(skip, int) or skip < 0:
        msg = "skip must be a non-negative integer"
        raise DummyJsonProductsError(msg)
    return f"products-skip-{skip:06d}.json"


def fetch_products_page(
    *,
    limit: int,
    skip: int,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_response_bytes: int = MAX_RESPONSE_BYTES,
    transport: Transport | None = None,
    sleep: Callable[[float], None] | None = None,
) -> bytes:
    """GET one products page.

    Retries cover timeouts, HTTP 429, and HTTP 5xx. The pause is 1s, then
    2s, then 4s. A numeric ``Retry-After`` on 429 replaces that pause and
    is capped at ``MAX_RETRY_AFTER_SECONDS``. An HTTP-date ``Retry-After``
    keeps the backoff. Other statuses fail on the first response. The error
    text does not include the response body.
    """
    page_limit = _require_positive_int("limit", limit)
    page_skip = _require_skip(skip)
    attempts = _require_positive_int("max_attempts", max_attempts)
    _require_positive_int("max_response_bytes", max_response_bytes)
    _require_timeout(timeout)
    origin = _origin(base_url)
    query = urlencode((("limit", str(page_limit)), ("skip", str(page_skip))))
    url = f"{origin}/products?{query}"
    return get_with_retries(
        HttpRequest(
            url=url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        ),
        timeout=float(timeout),
        max_response_bytes=max_response_bytes,
        retry=RetryPolicy(
            max_attempts=attempts,
            retry_timeouts=True,
            retry_status_codes=frozenset({429}),
            retry_status_at_least=500,
            retry_after_status_codes=frozenset({429}),
            retry_after_cap_seconds=MAX_RETRY_AFTER_SECONDS,
        ),
        redirect=_PRODUCTS_REDIRECT,
        error_type=DummyJsonProductsError,
        message_prefix="DummyJSON products",
        transport=transport,
        sleep=sleep,
    )


def ingest_dummyjson_products(
    client: BaseClient,
    *,
    bucket: str,
    execution_id: str,
    ingestion_ts: datetime | None = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_pages: int = MAX_PAGES,
    max_response_bytes: int = MAX_RESPONSE_BYTES,
    transport: Transport | None = None,
    sleep: Callable[[float], None] | None = None,
    contracts: tuple[DataContract, ...] | None = None,
) -> tuple[RawWriteResult, ...]:
    """Fetch every products page and write it to the raw layout.

    The same ``execution_id`` and ``ingestion_ts`` overwrite the same keys.
    A structural failure stops the remaining pages. Pages already written
    for this execution stay in place. A reported ``total`` that would need
    more than ``max_pages`` requests fails before the first write.
    """
    _require_token("bucket", bucket)
    _require_token("execution_id", execution_id)
    page_limit = _require_positive_int("limit", limit)
    page_budget = _require_positive_int("max_pages", max_pages)
    if ingestion_ts is not None and ingestion_ts.tzinfo is None:
        msg = "ingestion_ts must include a timezone"
        raise DummyJsonProductsError(msg)
    contract = _products_contract(contracts)
    ts = datetime.now(UTC) if ingestion_ts is None else ingestion_ts
    skip = 0
    seen_total: int | None = None
    results: list[RawWriteResult] = []
    while True:
        body = fetch_products_page(
            limit=page_limit,
            skip=skip,
            base_url=base_url,
            timeout=timeout,
            max_attempts=max_attempts,
            max_response_bytes=max_response_bytes,
            transport=transport,
            sleep=sleep,
        )
        page = _parse_page(
            body,
            contract,
            requested_skip=skip,
            requested_limit=page_limit,
        )
        if seen_total is None:
            if page.total > page_budget * page_limit:
                msg = "DummyJSON products total exceeds the page limit"
                raise DummyJsonProductsError(msg)
            seen_total = page.total
        elif page.total != seen_total:
            msg = "DummyJSON products total changed between pages"
            raise DummyJsonProductsError(msg)
        _require_page_count(page, skip=skip, limit=page_limit)
        object_name = page_object_name(skip)
        try:
            sidecar = prepare_quarantine(contract, page.records)
        except InvalidRecordsError as exc:
            raise DummyJsonProductsError(str(exc)) from exc
        result = write_raw_object(
            client,
            bucket=bucket,
            body=body,
            source_id=SOURCE_ID,
            dataset=DATASET,
            execution_id=execution_id,
            object_name=object_name,
            ingestion_ts=ts,
            content_type="application/json",
        )
        write_quarantine_object(
            client,
            bucket=bucket,
            source_id=SOURCE_ID,
            dataset=DATASET,
            execution_id=execution_id,
            object_name=object_name,
            ingestion_ts=result.metadata.ingestion_ts,
            body=sidecar,
        )
        results.append(result)
        next_skip = skip + page_limit
        if next_skip >= seen_total:
            break
        skip = next_skip
    return tuple(results)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint. Settings are checked before any storage client."""
    parser = argparse.ArgumentParser(
        description="Fetch DummyJSON product pages and write them to raw storage.",
    )
    parser.add_argument("--execution-id", required=True)
    parser.add_argument(
        "--ingestion-ts",
        default=None,
        help="UTC ISO-8601 timestamp. The same value overwrites the same keys.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_PAGE_LIMIT,
        help="Page size sent as limit. Values below 1 are rejected.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="API origin. The client calls {base-url}/products.",
    )
    args = parser.parse_args(argv)
    try:
        _run_cli(
            execution_id=args.execution_id,
            ingestion_ts=args.ingestion_ts,
            limit=args.limit,
            base_url=args.base_url,
        )
    except (DummyJsonProductsError, KeyError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _run_cli(
    *,
    execution_id: str,
    ingestion_ts: str | None,
    limit: int,
    base_url: str,
) -> None:
    env = os.environ
    bucket = env.get("ODP_RAW_BUCKET", "").strip()
    if not bucket:
        msg = "ODP_RAW_BUCKET is required"
        raise DummyJsonProductsError(msg)
    _require_positive_int("limit", limit)
    _origin(base_url)
    parsed = _parse_ingestion_ts(ingestion_ts) if ingestion_ts else None
    client = client_from_env(env)
    ingest_dummyjson_products(
        client,
        bucket=bucket,
        execution_id=execution_id,
        ingestion_ts=parsed,
        limit=limit,
        base_url=base_url,
    )


def _parse_ingestion_ts(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = "ingestion-ts must be ISO-8601"
        raise DummyJsonProductsError(msg) from exc
    if parsed.tzinfo is None:
        msg = "ingestion-ts must include a timezone"
        raise DummyJsonProductsError(msg)
    return parsed


def _products_contract(contracts: tuple[DataContract, ...] | None) -> DataContract:
    loaded = load_contracts() if contracts is None else contracts
    matches = [
        item
        for item in loaded
        if item.source_id == SOURCE_ID and item.dataset == DATASET
    ]
    if len(matches) != 1:
        msg = "missing contract for dummyjson_products.products"
        raise DummyJsonProductsError(msg)
    contract = matches[0]
    if contract.payload_format != "json" or contract.record_locator != DATASET:
        msg = "dummyjson_products.products contract must describe a JSON page"
        raise DummyJsonProductsError(msg)
    if not contract.envelope_fields or not contract.fields:
        msg = "dummyjson_products.products contract has no fields"
        raise DummyJsonProductsError(msg)
    return contract


def _parse_page(
    body: bytes,
    contract: DataContract,
    *,
    requested_skip: int,
    requested_limit: int,
) -> _Page:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        msg = "DummyJSON products page is not UTF-8 JSON"
        raise DummyJsonProductsError(msg) from exc
    if not isinstance(payload, dict):
        msg = "DummyJSON products page must be a JSON object"
        raise DummyJsonProductsError(msg)
    expected_envelope = {field.name for field in contract.envelope_fields}
    if set(payload) != expected_envelope:
        msg = "DummyJSON products page envelope does not match the contract"
        raise DummyJsonProductsError(msg)
    products = payload["products"]
    if not isinstance(products, list):
        msg = "DummyJSON products page field products must be an array"
        raise DummyJsonProductsError(msg)
    total = _json_non_negative_int("total", payload["total"])
    skip = _json_non_negative_int("skip", payload["skip"])
    limit = _json_non_negative_int("limit", payload["limit"])
    if skip != requested_skip or limit > requested_limit:
        msg = "DummyJSON products page skip or limit does not match the request"
        raise DummyJsonProductsError(msg)
    if limit != len(products) and limit != requested_limit:
        msg = "DummyJSON products page skip or limit does not match the request"
        raise DummyJsonProductsError(msg)
    required_keys = {field.name for field in contract.fields if not field.nullable}
    allowed_keys = {field.name for field in contract.fields}
    records: list[dict[str, object]] = []
    for index, item in enumerate(products):
        if not isinstance(item, dict):
            msg = f"DummyJSON products item {index} does not match the contract"
            raise DummyJsonProductsError(msg)
        keys = set(item)
        if not required_keys <= keys or not keys <= allowed_keys:
            msg = f"DummyJSON products item {index} does not match the contract"
            raise DummyJsonProductsError(msg)
        records.append(item)
    return _Page(total=total, count=len(products), records=tuple(records))


def _require_page_count(page: _Page, *, skip: int, limit: int) -> None:
    remaining = page.total - skip
    expected = remaining if remaining < limit else limit
    if page.count != expected:
        msg = "DummyJSON products page size does not match total"
        raise DummyJsonProductsError(msg)


def _origin(base_url: str) -> str:
    if (
        base_url.strip() != base_url
        or "\\" in base_url
        or not (base_url.startswith("https://") or base_url.startswith("http://"))
    ):
        msg = "base_url must be an http or https origin"
        raise DummyJsonProductsError(msg)
    return base_url.rstrip("/")


def _require_positive_int(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        msg = f"{name} must be an integer >= 1"
        raise DummyJsonProductsError(msg)
    return value


def _require_skip(skip: int) -> int:
    if isinstance(skip, bool) or not isinstance(skip, int) or skip < 0:
        msg = "skip must be a non-negative integer"
        raise DummyJsonProductsError(msg)
    return skip


def _require_timeout(timeout: float) -> float:
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        msg = "timeout must be a positive number"
        raise DummyJsonProductsError(msg)
    if timeout <= 0:
        msg = "timeout must be a positive number"
        raise DummyJsonProductsError(msg)
    return float(timeout)


def _require_token(name: str, value: str) -> str:
    if not isinstance(value, str) or value.strip() != value or not value:
        msg = f"{name} must be non-empty"
        raise DummyJsonProductsError(msg)
    return value


def _json_non_negative_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        msg = f"DummyJSON products page field {name} must be a non-negative integer"
        raise DummyJsonProductsError(msg)
    return value
