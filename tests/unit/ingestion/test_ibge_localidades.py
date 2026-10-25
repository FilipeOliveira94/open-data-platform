"""Unit tests for the IBGE municipalities client. No live API calls."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.model import ContractField, DataContract
from open_data_platform.ingestion import ingest_ibge_localidades as exported_ingest
from open_data_platform.ingestion.http import HttpRequest, HttpResponse, Transport
from open_data_platform.ingestion.ibge_localidades import (
    MAX_RESPONSE_BYTES,
    MAX_RETRY_AFTER_SECONDS,
    USER_AGENT,
    IbgeLocalidadesError,
    fetch_municipios,
    ingest_ibge_localidades,
    main,
)

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "ibge_localidades"
_TS = datetime(2026, 10, 25, 18, 0, tzinfo=UTC)
_PREFIX = "raw/ibge_localidades/municipios/dt=2026-10-25/execution_id=run-001/"
_URL = "https://ibge.test/api/v1/localidades/municipios?orderBy=nome&view=nivelado"


def _fixture(name: str) -> bytes:
    return (_FIXTURES / name).read_bytes()


def _client() -> MagicMock:
    client = MagicMock()
    client.head_bucket.return_value = {}
    return client


class _Body:
    """Return one scripted body and record the request."""

    def __init__(self, body: bytes) -> None:
        self.body = body
        self.requests: list[HttpRequest] = []

    def __call__(self, request: HttpRequest, timeout: float) -> HttpResponse:
        del timeout
        self.requests.append(request)
        return HttpResponse(status=200, headers={}, body=self.body)


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
    max_attempts: int = 3,
    max_response_bytes: int = 64,
    sleeps: list[float] | None = None,
) -> bytes:
    def pause(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)

    return fetch_municipios(
        base_url="https://ibge.test/",
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
) -> Any:
    kwargs: dict[str, Any] = {
        "bucket": "odp-raw",
        "execution_id": "run-001",
        "ingestion_ts": _TS,
        "base_url": "https://ibge.test",
        "transport": transport,
        "sleep": lambda _seconds: None,
    }
    kwargs.update(overrides)
    return ingest_ibge_localidades(client, **kwargs)


def _contract() -> DataContract:
    return next(
        item
        for item in load_contracts()
        if item.source_id == "ibge_localidades" and item.dataset == "municipios"
    )


def _sample(field: ContractField) -> object:
    if field.field_type == "integer":
        return 1
    if field.field_type == "string":
        return "fixture"
    return "fixture"


def _municipality(contract: DataContract, **overrides: object) -> dict[str, object]:
    item = {field.name: _sample(field) for field in contract.fields}
    item.update(overrides)
    return item


def _array(*items: dict[str, object]) -> bytes:
    return json.dumps(list(items)).encode()


def test_package_exports_the_municipios_ingest() -> None:
    assert exported_ingest is ingest_ibge_localidades


def test_response_limit_is_64_mib() -> None:
    assert MAX_RESPONSE_BYTES == 64 * 1024 * 1024


def test_fixture_lands_under_ibge_localidades() -> None:
    contract = _contract()
    client = _client()
    body = _fixture("municipios.json")
    transport = _Body(body)

    result = _ingest(client, transport)

    assert contract.domain == "olist_analysis"
    assert contract.update_pattern == "full_refresh"
    assert result.metadata.source_id == "ibge_localidades"
    assert result.key == _PREFIX + "municipios.json"
    request = transport.requests[0]
    assert request.url == _URL
    assert request.headers["Accept"] == "application/json"
    assert request.headers["User-Agent"] == USER_AGENT
    put = client.put_object.call_args.kwargs
    assert put["Body"] == body
    assert put["ContentType"] == "application/json"
    assert put["Metadata"] == {
        "source_id": "ibge_localidades",
        "execution_id": "run-001",
        "_ingestion_ts": "2026-10-25T18:00:00Z",
    }
    stored = json.loads(put["Body"])
    assert stored[1]["microrregiao-id"] is None
    assert stored[1]["mesorregiao-nome"] is None


def test_same_execution_overwrites_and_a_new_execution_keeps_its_own_key() -> None:
    contract = _contract()
    client = _client()
    first = _array(_municipality(contract, **{"municipio-nome": "first"}))
    second = _array(_municipality(contract, **{"municipio-nome": "second"}))

    first_result = _ingest(client, _Body(first))
    second_result = _ingest(client, _Body(second))
    other = _ingest(client, _Body(first), execution_id="run-002")

    assert first_result.key == second_result.key
    assert other.key != first_result.key
    assert other.key.endswith("/execution_id=run-002/municipios.json")
    puts = client.put_object.call_args_list
    assert puts[0].kwargs["Body"] == first
    assert puts[1].kwargs["Body"] == second
    assert puts[1].kwargs["Key"] == puts[0].kwargs["Key"]


def test_null_and_duplicate_ids_stay_in_the_array() -> None:
    contract = _contract()
    client = _client()
    body = _array(
        _municipality(contract, **{"municipio-id": None, "municipio-nome": "blank"}),
        _municipality(contract, **{"municipio-id": 1, "municipio-nome": "copy"}),
        _municipality(contract, **{"municipio-id": 1, "municipio-nome": "again"}),
    )

    _ingest(client, _Body(body))

    stored = json.loads(client.put_object.call_args.kwargs["Body"])
    assert [item["municipio-id"] for item in stored] == [None, 1, 1]


def test_omitted_nullable_region_keys_stay_omitted() -> None:
    contract = _contract()
    item = _municipality(contract)
    for name in (
        "microrregiao-id",
        "microrregiao-nome",
        "mesorregiao-id",
        "mesorregiao-nome",
    ):
        del item[name]
    client = _client()

    _ingest(client, _Body(_array(item)))

    stored = json.loads(client.put_object.call_args.kwargs["Body"])
    assert "microrregiao-id" not in stored[0]
    assert "mesorregiao-nome" not in stored[0]


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        ("drop_required", "item 0 does not match"),
        ("extra_key", "item 0 does not match"),
        ("not_object", "item 0 does not match"),
    ],
)
def test_structural_mismatch_writes_nothing(mutate: str, match: str) -> None:
    contract = _contract()
    if mutate == "drop_required":
        item = _municipality(contract)
        del item["municipio-nome"]
        body = _array(item)
    elif mutate == "extra_key":
        body = _array(_municipality(contract, extra="nope"))
    else:
        body = b"[1]"
    client = _client()

    with pytest.raises(IbgeLocalidadesError, match=match):
        _ingest(client, _Body(body))

    assert client.put_object.call_count == 0


@pytest.mark.parametrize(
    ("body", "match"),
    [
        (b"\xff", "not UTF-8 JSON"),
        (b"{", "not UTF-8 JSON"),
        (b"{}", "must be a JSON array"),
        (b"[]", "payload is empty"),
    ],
)
def test_unreadable_payload_writes_nothing(body: bytes, match: str) -> None:
    client = _client()

    with pytest.raises(IbgeLocalidadesError, match=match):
        _ingest(client, _Body(body))

    assert client.put_object.call_count == 0


def test_missing_contract_makes_no_request() -> None:
    transport = _Body(b"[]")
    client = _client()

    with pytest.raises(IbgeLocalidadesError, match="missing contract"):
        _ingest(client, transport, contracts=())

    assert transport.requests == []
    assert client.put_object.call_count == 0


def test_timeouts_retry_with_the_ibge_message() -> None:
    transport = _Steps(
        [TimeoutError("slow"), TimeoutError("slow"), TimeoutError("slow")]
    )
    seen: list[float] = []

    with pytest.raises(
        IbgeLocalidadesError,
        match="IBGE localidades request timed out",
    ):
        _fetch(transport, sleeps=seen)

    assert transport.calls == 3
    assert seen == [1.0, 2.0]


def test_retry_after_seconds_are_capped() -> None:
    transport = _Steps(
        [
            HttpResponse(429, {"Retry-After": "120"}, b"later"),
            HttpResponse(200, {}, b"[]"),
        ]
    )
    seen: list[float] = []

    assert _fetch(transport, sleeps=seen) == b"[]"
    assert seen == [MAX_RETRY_AFTER_SECONDS]


def test_server_error_retries_once() -> None:
    transport = _Steps([HttpResponse(500, {}, b"no"), HttpResponse(200, {}, b"[]")])
    seen: list[float] = []

    assert _fetch(transport, sleeps=seen) == b"[]"
    assert seen == [1.0]
    assert transport.calls == 2


def test_redirect_and_other_client_errors_fail_immediately() -> None:
    marker = "super-secret-body"
    transport = _Steps([HttpResponse(301, {"Location": "/elsewhere"}, marker.encode())])
    seen: list[float] = []

    with pytest.raises(IbgeLocalidadesError) as caught:
        _fetch(transport, sleeps=seen)

    assert str(caught.value) == "IBGE localidades request failed with status 301"
    assert marker not in str(caught.value)
    assert transport.calls == 1
    assert seen == []


def test_connection_error_is_not_retried() -> None:
    transport = _Steps([OSError("down")])
    seen: list[float] = []

    with pytest.raises(IbgeLocalidadesError) as caught:
        _fetch(transport, sleeps=seen)

    assert str(caught.value) == "IBGE localidades request failed"
    assert "down" not in str(caught.value)
    assert transport.calls == 1


def test_oversized_success_body_is_not_retried() -> None:
    transport = _Steps([HttpResponse(200, {}, b"abcdef")])
    seen: list[float] = []

    with pytest.raises(IbgeLocalidadesError, match="size limit"):
        _fetch(transport, max_response_bytes=4, sleeps=seen)

    assert transport.calls == 1
    assert seen == []


def test_cli_requires_the_raw_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ODP_RAW_BUCKET", raising=False)
    assert main(["--execution-id", "run-001"]) == 1


def test_cli_rejects_a_naive_ingestion_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ODP_RAW_BUCKET", "odp-raw")
    code = main(["--execution-id", "run-001", "--ingestion-ts", "2026-10-25T18:00:00"])
    assert code == 1
