"""Fetch DummyJSON ``/products`` pages and land each page in raw storage.

The client calls ``GET {origin}/products?limit={n}&skip={n}`` until
``skip + limit >= total``. One response becomes one object named
``products-skip-{skip:06d}.json`` under ``raw/dummyjson_products/products/``.
The stored bytes are the response body. Unit tests inject a transport and
do not call the public API.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import urlencode

from open_data_platform import __version__
from open_data_platform.contracts.load import load_contracts
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
_CHUNK_BYTES = 64 * 1024
_RETRY_AFTER_RE = re.compile(r"\d+(?:\.\d+)?")


class DummyJsonProductsError(ValueError):
    """A DummyJSON products page could not be fetched or landed."""


@dataclass(frozen=True, slots=True)
class PageRequest:
    """One GET issued by the products client. Redirects are not followed."""

    url: str
    headers: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class PageResponse:
    """HTTP response visible to the retry loop."""

    status: int
    headers: Mapping[str, str]
    body: bytes


Transport = Callable[[PageRequest, float], PageResponse]


@dataclass(frozen=True, slots=True)
class _Page:
    """Envelope values needed to decide the next request."""

    total: int
    count: int


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
    pause = time.sleep if sleep is None else sleep
    active = _urllib_transport if transport is None else transport
    query = urlencode((("limit", str(page_limit)), ("skip", str(page_skip))))
    url = f"{origin}/products?{query}"
    attempt = 0
    while True:
        request = PageRequest(
            url=url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        try:
            response = active(request, float(timeout))
        except TimeoutError:
            attempt = _pause_for_retry(
                attempt=attempt,
                max_attempts=attempts,
                pause=pause,
                delay=_backoff(attempt + 1),
                message="DummyJSON products request timed out",
            )
            continue
        except DummyJsonProductsError:
            raise
        except OSError as exc:
            msg = "DummyJSON products request failed"
            raise DummyJsonProductsError(msg) from exc

        if response.status == 200:
            if len(response.body) > max_response_bytes:
                msg = "DummyJSON products response exceeded the size limit"
                raise DummyJsonProductsError(msg)
            return response.body

        if response.status == 429 or response.status >= 500:
            retry_after = None
            if response.status == 429:
                retry_after = _retry_after_seconds(response.headers)
            delay = _backoff(attempt + 1) if retry_after is None else retry_after
            attempt = _pause_for_retry(
                attempt=attempt,
                max_attempts=attempts,
                pause=pause,
                delay=delay,
                message=(
                    f"DummyJSON products request failed with status {response.status}"
                ),
            )
            continue

        msg = f"DummyJSON products request failed with status {response.status}"
        raise DummyJsonProductsError(msg)


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
        results.append(
            write_raw_object(
                client,
                bucket=bucket,
                body=body,
                source_id=SOURCE_ID,
                dataset=DATASET,
                execution_id=execution_id,
                object_name=page_object_name(skip),
                ingestion_ts=ts,
                content_type="application/json",
            )
        )
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
    for index, item in enumerate(products):
        if not isinstance(item, dict):
            msg = f"DummyJSON products item {index} does not match the contract"
            raise DummyJsonProductsError(msg)
        keys = set(item)
        if not required_keys <= keys or not keys <= allowed_keys:
            msg = f"DummyJSON products item {index} does not match the contract"
            raise DummyJsonProductsError(msg)
    return _Page(total=total, count=len(products))


def _require_page_count(page: _Page, *, skip: int, limit: int) -> None:
    remaining = page.total - skip
    expected = remaining if remaining < limit else limit
    if page.count != expected:
        msg = "DummyJSON products page size does not match total"
        raise DummyJsonProductsError(msg)


def _pause_for_retry(
    *,
    attempt: int,
    max_attempts: int,
    pause: Callable[[float], None],
    delay: float,
    message: str,
) -> int:
    attempt += 1
    if attempt >= max_attempts:
        raise DummyJsonProductsError(message)
    pause(delay)
    return attempt


def _backoff(failed_attempt: int) -> float:
    return float(2 ** (failed_attempt - 1))


def _retry_after_seconds(headers: Mapping[str, str]) -> float | None:
    raw = _header(headers, "retry-after")
    if raw is None:
        return None
    text = raw.strip()
    if _RETRY_AFTER_RE.fullmatch(text) is None:
        return None
    return min(float(text), MAX_RETRY_AFTER_SECONDS)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value
    return None


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


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Surface 3xx to the caller instead of following it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)


def _urllib_transport(request: PageRequest, timeout: float) -> PageResponse:
    opener = urllib.request.build_opener(_NoRedirectHandler)
    outgoing = urllib.request.Request(
        request.url,
        headers=dict(request.headers),
        method="GET",
    )
    try:
        with opener.open(outgoing, timeout=timeout) as response:
            body = _read_limited(response, MAX_RESPONSE_BYTES)
            status = int(getattr(response, "status", 200))
            return PageResponse(
                status=status,
                headers=_header_map(response.headers),
                body=body,
            )
    except TimeoutError:
        raise
    except urllib.error.HTTPError as exc:
        try:
            body = _read_limited(exc, MAX_RESPONSE_BYTES)
        except DummyJsonProductsError:
            exc.close()
            raise
        status = int(exc.code)
        headers = _header_map(exc.headers)
        exc.close()
        return PageResponse(status=status, headers=headers, body=body)
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, TimeoutError):
            raise TimeoutError("DummyJSON products request timed out") from exc
        msg = "DummyJSON products request failed"
        raise DummyJsonProductsError(msg) from exc


def _read_limited(stream: object, limit: int) -> bytes:
    read = getattr(stream, "read", None)
    if not callable(read):
        msg = "DummyJSON products request failed"
        raise DummyJsonProductsError(msg)
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = read(_CHUNK_BYTES)
        if not chunk:
            break
        if not isinstance(chunk, bytes):
            msg = "DummyJSON products request failed"
            raise DummyJsonProductsError(msg)
        total += len(chunk)
        if total > limit:
            msg = "DummyJSON products response exceeded the size limit"
            raise DummyJsonProductsError(msg)
        chunks.append(chunk)
    return b"".join(chunks)


def _header_map(headers: object) -> dict[str, str]:
    items = getattr(headers, "items", None)
    if not callable(items):
        return {}
    return {str(key).lower(): str(value) for key, value in items()}
