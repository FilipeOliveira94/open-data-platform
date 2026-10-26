"""Unit tests for the DummyJSON products client. No live API calls."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlsplit

import pytest

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.model import ContractField, DataContract
from open_data_platform.ingestion import ingest_dummyjson_products as exported_ingest
from open_data_platform.ingestion.dummyjson_products import (
    MAX_RETRY_AFTER_SECONDS,
    USER_AGENT,
    DummyJsonProductsError,
    fetch_products_page,
    ingest_dummyjson_products,
    main,
    page_object_name,
)
from open_data_platform.ingestion.http import (
    HttpRequest,
    HttpResponse,
    Transport,
)

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "dummyjson_products"
_TS = datetime(2026, 10, 24, 18, 0, tzinfo=UTC)
_PREFIX = "raw/dummyjson_products/products/dt=2026-10-24/execution_id=run-001/"


def _fixture(name: str) -> bytes:
    return (_FIXTURES / name).read_bytes()


def _client() -> MagicMock:
    client = MagicMock()
    client.head_bucket.return_value = {}
    return client


class _Pages:
    """Return a fixture body for the requested skip."""

    def __init__(self, pages: dict[int, bytes]) -> None:
        self.pages = pages
        self.urls: list[str] = []

    def __call__(self, request: HttpRequest, timeout: float) -> HttpResponse:
        del timeout
        self.urls.append(request.url)
        skip = int(parse_qs(urlsplit(request.url).query)["skip"][0])
        return HttpResponse(status=200, headers={}, body=self.pages[skip])


class _Steps:
    """Return or raise the next scripted step."""

    def __init__(self, steps: list[HttpResponse | Exception]) -> None:
        self.steps = steps
        self.calls = 0

    def __call__(self, request: HttpRequest, timeout: float) -> HttpResponse:
        del request, timeout
        step = self.steps[self.calls]
        self.calls += 1
        if isinstance(step, Exception):
            raise step
        return step


def _fetch(
    transport: Transport,
    *,
    limit: int = 2,
    skip: int = 0,
    max_attempts: int = 3,
    max_response_bytes: int = 64,
    sleeps: list[float] | None = None,
) -> bytes:
    def pause(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)

    return fetch_products_page(
        limit=limit,
        skip=skip,
        base_url="https://dummyjson.test/",
        timeout=5,
        max_attempts=max_attempts,
        max_response_bytes=max_response_bytes,
        transport=transport,
        sleep=pause,
    )


def _ingest(
    client: MagicMock,
    transport: Transport,
    **overrides: Any,
) -> tuple[Any, ...]:
    kwargs: dict[str, Any] = {
        "bucket": "odp-raw",
        "execution_id": "run-001",
        "ingestion_ts": _TS,
        "limit": 2,
        "base_url": "https://dummyjson.test",
        "transport": transport,
        "sleep": lambda _seconds: None,
    }
    kwargs.update(overrides)
    return ingest_dummyjson_products(client, **kwargs)


def _contract() -> DataContract:
    return next(
        item
        for item in load_contracts()
        if item.source_id == "dummyjson_products" and item.dataset == "products"
    )


def _sample(field: ContractField) -> object:
    if field.field_type == "integer":
        return 1
    if field.field_type == "number":
        return 1.5
    if field.field_type == "string":
        return "fixture"
    if field.field_type == "json":
        return []
    return "fixture"


def _product(contract: DataContract, **overrides: object) -> dict[str, object]:
    item = {field.name: _sample(field) for field in contract.fields}
    item.update(overrides)
    return item


def _page(
    contract: DataContract,
    *,
    skip: int,
    limit: int,
    total: int,
    products: list[dict[str, object]],
) -> bytes:
    payload = {
        "products": products,
        "total": total,
        "skip": skip,
        "limit": limit,
    }
    return json.dumps(payload).encode()


def test_package_exports_the_products_ingest() -> None:
    assert exported_ingest is ingest_dummyjson_products


def test_two_pages_land_under_dummyjson_products() -> None:
    contract = _contract()
    client = _client()
    transport = _Pages(
        {
            0: _fixture("page-skip-0.json"),
            2: _fixture("page-skip-2.json"),
        }
    )

    results = _ingest(client, transport)

    assert contract.domain == "product_analysis"
    assert [item.metadata.source_id for item in results] == [
        "dummyjson_products",
        "dummyjson_products",
    ]
    assert transport.urls == [
        "https://dummyjson.test/products?limit=2&skip=0",
        "https://dummyjson.test/products?limit=2&skip=2",
    ]
    puts = client.put_object.call_args_list
    assert [call.kwargs["Key"] for call in puts] == [
        _PREFIX + "products-skip-000000.json",
        _PREFIX + "products-skip-000002.json",
    ]
    assert [call.kwargs["Body"] for call in puts] == [
        _fixture("page-skip-0.json"),
        _fixture("page-skip-2.json"),
    ]
    assert puts[0].kwargs["ContentType"] == "application/json"
    assert puts[0].kwargs["Metadata"] == {
        "source_id": "dummyjson_products",
        "execution_id": "run-001",
        "_ingestion_ts": "2026-10-24T18:00:00Z",
    }


def test_total_zero_writes_one_empty_page() -> None:
    client = _client()
    transport = _Pages({0: _fixture("page-total-0.json")})

    results = _ingest(client, transport)

    assert len(results) == 1
    assert results[0].key.endswith("/products-skip-000000.json")
    assert transport.urls == ["https://dummyjson.test/products?limit=2&skip=0"]
    assert client.put_object.call_args.kwargs["Body"] == _fixture("page-total-0.json")


def test_same_execution_overwrites_the_page_key() -> None:
    contract = _contract()
    client = _client()
    first = _page(
        contract,
        skip=0,
        limit=30,
        total=1,
        products=[_product(contract, id=1, title="first")],
    )
    second = _page(
        contract,
        skip=0,
        limit=30,
        total=1,
        products=[_product(contract, id=1, title="second")],
    )

    first_result = _ingest(client, _Pages({0: first}), limit=30)
    second_result = _ingest(client, _Pages({0: second}), limit=30)

    assert first_result[0].key == second_result[0].key
    assert first_result[0].key.endswith("/" + page_object_name(0))
    puts = client.put_object.call_args_list
    assert puts[0].kwargs["Key"] == puts[1].kwargs["Key"]
    assert puts[0].kwargs["Body"] == first
    assert puts[1].kwargs["Body"] == second


def test_null_and_duplicate_ids_stay_in_the_page() -> None:
    contract = _contract()
    client = _client()
    body = _page(
        contract,
        skip=0,
        limit=30,
        total=3,
        products=[
            _product(contract, id=None, title="blank"),
            _product(contract, id=1, title="copy"),
            _product(contract, id=1, title="again"),
        ],
    )

    _ingest(client, _Pages({0: body}), limit=30)

    raw = next(
        call.kwargs
        for call in client.put_object.call_args_list
        if call.kwargs["Key"].endswith("/products-skip-000000.json")
    )
    stored = json.loads(raw["Body"])
    assert [item["id"] for item in stored["products"]] == [None, 1, 1]
    assert raw["Body"] == body
    sidecar = next(
        call.kwargs
        for call in client.put_object.call_args_list
        if call.kwargs["Key"].startswith("quarantine/")
    )
    findings = [json.loads(line) for line in sidecar["Body"].splitlines()]
    assert [(item["record_index"], item["reason"]) for item in findings] == [
        (0, "null_violation"),
        (1, "duplicate_primary_key"),
        (2, "duplicate_primary_key"),
    ]
    assert findings[0]["field"] == "id"


def test_limit_below_one_makes_no_request() -> None:
    transport = _Pages({})
    with pytest.raises(DummyJsonProductsError, match="limit must be an integer >= 1"):
        _ingest(_client(), transport, limit=0)
    assert transport.urls == []


def test_envelope_mismatch_writes_nothing() -> None:
    payload = json.loads(_fixture("page-skip-0.json"))
    del payload["total"]
    client = _client()
    transport = _Pages({0: json.dumps(payload).encode()})

    with pytest.raises(DummyJsonProductsError, match="envelope does not match"):
        _ingest(client, transport)

    assert client.put_object.call_count == 0


def test_bad_second_page_stops_before_the_next_put() -> None:
    client = _client()
    payload = json.loads(_fixture("page-skip-2.json"))
    del payload["products"][0]["title"]
    transport = _Pages(
        {
            0: _fixture("page-skip-0.json"),
            2: json.dumps(payload).encode(),
        }
    )

    with pytest.raises(DummyJsonProductsError, match="item 0 does not match"):
        _ingest(client, transport)

    assert client.put_object.call_count == 1
    assert client.put_object.call_args.kwargs["Key"].endswith(
        "products-skip-000000.json"
    )


def test_short_final_page_accepts_the_returned_count_as_limit() -> None:
    contract = _contract()
    assert contract.contract_version == 2
    client = _client()
    body = _page(
        contract,
        skip=0,
        limit=1,
        total=1,
        products=[_product(contract, id=1)],
    )

    results = _ingest(client, _Pages({0: body}), limit=30)

    assert len(results) == 1
    assert client.put_object.call_args.kwargs["Body"] == body


def test_omitted_brand_stays_in_the_stored_page() -> None:
    contract = _contract()
    product = _product(contract, id=1)
    del product["brand"]
    body = _page(
        contract,
        skip=0,
        limit=30,
        total=1,
        products=[product],
    )
    client = _client()

    _ingest(client, _Pages({0: body}), limit=30)

    stored = json.loads(client.put_object.call_args.kwargs["Body"])
    assert "brand" not in stored["products"][0]


def test_page_size_must_match_total() -> None:
    contract = _contract()
    body = _page(
        contract,
        skip=0,
        limit=2,
        total=3,
        products=[_product(contract, id=1)],
    )
    client = _client()

    with pytest.raises(DummyJsonProductsError, match="page size does not match"):
        _ingest(client, _Pages({0: body}))

    assert client.put_object.call_count == 0


def test_changed_total_stops_the_run() -> None:
    first = json.loads(_fixture("page-skip-0.json"))
    second = json.loads(_fixture("page-skip-2.json"))
    second["total"] = 4
    client = _client()

    with pytest.raises(DummyJsonProductsError, match="total changed"):
        _ingest(
            client,
            _Pages({0: json.dumps(first).encode(), 2: json.dumps(second).encode()}),
        )

    assert client.put_object.call_count == 1


def test_total_above_the_page_budget_writes_nothing() -> None:
    contract = _contract()
    body = _page(
        contract,
        skip=0,
        limit=2,
        total=3,
        products=[_product(contract, id=1), _product(contract, id=2)],
    )
    client = _client()

    with pytest.raises(DummyJsonProductsError, match="exceeds the page limit"):
        _ingest(client, _Pages({0: body}), max_pages=1)

    assert client.put_object.call_count == 0


@pytest.mark.parametrize(
    ("steps", "sleeps", "calls"),
    [
        (
            [TimeoutError("slow"), TimeoutError("slow"), HttpResponse(200, {}, b"{}")],
            [1.0, 2.0],
            3,
        ),
        (
            [
                HttpResponse(500, {}, b"no"),
                HttpResponse(200, {}, b"{}"),
            ],
            [1.0],
            2,
        ),
    ],
)
def test_transient_failures_retry(
    steps: list[HttpResponse | Exception],
    sleeps: list[float],
    calls: int,
) -> None:
    transport = _Steps(steps)
    seen: list[float] = []

    assert _fetch(transport, sleeps=seen) == b"{}"
    assert seen == sleeps
    assert transport.calls == calls


def test_timeout_exhausts_three_attempts() -> None:
    transport = _Steps(
        [TimeoutError("slow"), TimeoutError("slow"), TimeoutError("slow")]
    )
    seen: list[float] = []

    with pytest.raises(DummyJsonProductsError, match="timed out"):
        _fetch(transport, sleeps=seen)

    assert transport.calls == 3
    assert seen == [1.0, 2.0]


def test_server_error_backoff_includes_four_seconds_on_a_fourth_attempt() -> None:
    transport = _Steps([HttpResponse(500, {}, b"no")] * 4)
    seen: list[float] = []

    with pytest.raises(DummyJsonProductsError, match="status 500"):
        _fetch(transport, max_attempts=4, sleeps=seen)

    assert transport.calls == 4
    assert seen == [1.0, 2.0, 4.0]


def test_retry_after_seconds_replace_the_backoff_and_are_capped() -> None:
    transport = _Steps(
        [
            HttpResponse(429, {"Retry-After": "120"}, b"later"),
            HttpResponse(200, {}, b"{}"),
        ]
    )
    seen: list[float] = []

    assert _fetch(transport, sleeps=seen) == b"{}"
    assert seen == [MAX_RETRY_AFTER_SECONDS]


def test_http_date_retry_after_keeps_the_numeric_backoff() -> None:
    transport = _Steps(
        [
            HttpResponse(
                429,
                {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"},
                b"later",
            ),
            HttpResponse(200, {}, b"{}"),
        ]
    )
    seen: list[float] = []

    assert _fetch(transport, sleeps=seen) == b"{}"
    assert seen == [1.0]


@pytest.mark.parametrize("status", [301, 400, 404])
def test_non_retryable_status_fails_immediately(status: int) -> None:
    marker = "super-secret-body"
    transport = _Steps([HttpResponse(status, {}, marker.encode())])
    seen: list[float] = []

    with pytest.raises(DummyJsonProductsError) as caught:
        _fetch(transport, sleeps=seen)

    assert str(caught.value) == (
        f"DummyJSON products request failed with status {status}"
    )
    assert marker not in str(caught.value)
    assert transport.calls == 1
    assert seen == []


def test_connection_error_is_not_retried() -> None:
    transport = _Steps([OSError("down")])
    seen: list[float] = []

    with pytest.raises(DummyJsonProductsError) as caught:
        _fetch(transport, sleeps=seen)

    assert str(caught.value) == "DummyJSON products request failed"
    assert "down" not in str(caught.value)
    assert transport.calls == 1
    assert seen == []


def test_oversized_success_body_is_not_retried() -> None:
    transport = _Steps([HttpResponse(200, {}, b"abcdef")])
    seen: list[float] = []

    with pytest.raises(DummyJsonProductsError, match="size limit"):
        _fetch(transport, max_response_bytes=4, sleeps=seen)

    assert transport.calls == 1
    assert seen == []


def test_fetch_requests_limit_and_skip() -> None:
    seen: list[HttpRequest] = []

    def transport(request: HttpRequest, timeout: float) -> HttpResponse:
        del timeout
        seen.append(request)
        return HttpResponse(status=200, headers={}, body=b"{}")

    assert _fetch(transport, limit=30, skip=0) == b"{}"
    assert seen[0].url == "https://dummyjson.test/products?limit=30&skip=0"
    assert seen[0].headers["Accept"] == "application/json"
    assert seen[0].headers["User-Agent"] == USER_AGENT


def test_cli_requires_the_raw_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ODP_RAW_BUCKET", raising=False)
    assert main(["--execution-id", "run-001"]) == 1


def test_cli_rejects_limit_below_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODP_RAW_BUCKET", "odp-raw")
    assert main(["--execution-id", "run-001", "--limit", "0"]) == 1


def test_cli_rejects_a_naive_ingestion_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ODP_RAW_BUCKET", "odp-raw")
    code = main(["--execution-id", "run-001", "--ingestion-ts", "2026-10-24T18:00:00"])
    assert code == 1


def test_default_transport_retries_429_on_localhost() -> None:
    hits = {"n": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            hits["n"] += 1
            if hits["n"] == 1:
                self.send_response(429)
                self.send_header("Retry-After", "0")
                self.end_headers()
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    seen: list[float] = []
    try:
        body = fetch_products_page(
            limit=1,
            skip=0,
            base_url=f"http://127.0.0.1:{port}",
            timeout=5,
            sleep=seen.append,
        )
    finally:
        server.shutdown()
        server.server_close()

    assert body == b"{}"
    assert hits["n"] == 2
    assert seen == [0.0]
