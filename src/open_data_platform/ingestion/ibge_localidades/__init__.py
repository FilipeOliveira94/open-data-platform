"""Fetch IBGE municipalities and land the response in raw storage.

The client calls
``GET {origin}/api/v1/localidades/municipios?orderBy=nome&view=nivelado``.
One response becomes ``municipios.json`` under
``raw/ibge_localidades/municipios/``. The stored bytes are the response
body. Unit tests inject a transport and do not call the public API. The
GET goes through ``ingestion.http`` with this client's retry and redirect
policy. The response size limit is 64 MiB.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
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

SOURCE_ID = "ibge_localidades"
DATASET = "municipios"
OBJECT_NAME = "municipios.json"
DEFAULT_BASE_URL = "https://servicodados.ibge.gov.br"
USER_AGENT = f"open-data-platform/{__version__} (ibge-localidades)"
MUNICIPIOS_PATH = "/api/v1/localidades/municipios"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_ATTEMPTS = 3
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_RETRY_AFTER_SECONDS = 30.0
_MUNICIPIOS_REDIRECT = RedirectPolicy(follow=False, max_redirects=0)


class IbgeLocalidadesError(ValueError):
    """An IBGE municipalities response could not be fetched or landed."""


def fetch_municipios(
    *,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_response_bytes: int = MAX_RESPONSE_BYTES,
    transport: Transport | None = None,
    sleep: Callable[[float], None] | None = None,
) -> bytes:
    """GET the nivelado municipality list.

    Retries cover timeouts, HTTP 429, and HTTP 5xx. The pause is 1s, then
    2s, then 4s. A numeric ``Retry-After`` on 429 replaces that pause and
    is capped at ``MAX_RETRY_AFTER_SECONDS``. An HTTP-date ``Retry-After``
    keeps the backoff. Other statuses fail on the first response. Redirects
    are not followed. The error text does not include the response body.
    """
    attempts = _require_positive_int("max_attempts", max_attempts)
    limit = _require_positive_int("max_response_bytes", max_response_bytes)
    _require_timeout(timeout)
    origin = _origin(base_url)
    query = urlencode((("orderBy", "nome"), ("view", "nivelado")))
    url = f"{origin}{MUNICIPIOS_PATH}?{query}"
    return get_with_retries(
        HttpRequest(
            url=url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        ),
        timeout=float(timeout),
        max_response_bytes=limit,
        retry=RetryPolicy(
            max_attempts=attempts,
            retry_timeouts=True,
            retry_status_codes=frozenset({429}),
            retry_status_at_least=500,
            retry_after_status_codes=frozenset({429}),
            retry_after_cap_seconds=MAX_RETRY_AFTER_SECONDS,
        ),
        redirect=_MUNICIPIOS_REDIRECT,
        error_type=IbgeLocalidadesError,
        message_prefix="IBGE localidades",
        transport=transport,
        sleep=sleep,
    )


def ingest_ibge_localidades(
    client: BaseClient,
    *,
    bucket: str,
    execution_id: str,
    ingestion_ts: datetime | None = None,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_response_bytes: int = MAX_RESPONSE_BYTES,
    transport: Transport | None = None,
    sleep: Callable[[float], None] | None = None,
    contracts: tuple[DataContract, ...] | None = None,
) -> RawWriteResult:
    """Fetch the municipality list and write it to the raw layout.

    The same ``execution_id`` and ``ingestion_ts`` overwrite ``municipios.json``.
    A structural failure writes nothing. Invalid rows stay in the array and
    are copied to the quarantine key.
    """
    _require_token("bucket", bucket)
    _require_token("execution_id", execution_id)
    if ingestion_ts is not None and ingestion_ts.tzinfo is None:
        msg = "ingestion_ts must include a timezone"
        raise IbgeLocalidadesError(msg)
    contract = _municipios_contract(contracts)
    ts = datetime.now(UTC) if ingestion_ts is None else ingestion_ts
    body = fetch_municipios(
        base_url=base_url,
        timeout=timeout,
        max_attempts=max_attempts,
        max_response_bytes=max_response_bytes,
        transport=transport,
        sleep=sleep,
    )
    records = _require_municipios(body, contract)
    try:
        sidecar = prepare_quarantine(contract, records)
    except InvalidRecordsError as exc:
        raise IbgeLocalidadesError(str(exc)) from exc
    result = write_raw_object(
        client,
        bucket=bucket,
        body=body,
        source_id=SOURCE_ID,
        dataset=DATASET,
        execution_id=execution_id,
        object_name=OBJECT_NAME,
        ingestion_ts=ts,
        content_type="application/json",
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
    """CLI entrypoint. Settings are checked before any storage client."""
    parser = argparse.ArgumentParser(
        description="Fetch IBGE municipalities and write them to raw storage.",
    )
    parser.add_argument("--execution-id", required=True)
    parser.add_argument(
        "--ingestion-ts",
        default=None,
        help="UTC ISO-8601 timestamp. The same value overwrites the same key.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="API origin. The client calls {base-url}/api/v1/localidades/municipios.",
    )
    args = parser.parse_args(argv)
    try:
        _run_cli(
            execution_id=args.execution_id,
            ingestion_ts=args.ingestion_ts,
            base_url=args.base_url,
        )
    except (IbgeLocalidadesError, KeyError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _run_cli(
    *,
    execution_id: str,
    ingestion_ts: str | None,
    base_url: str,
) -> None:
    env = os.environ
    bucket = env.get("ODP_RAW_BUCKET", "").strip()
    if not bucket:
        msg = "ODP_RAW_BUCKET is required"
        raise IbgeLocalidadesError(msg)
    _origin(base_url)
    parsed = _parse_ingestion_ts(ingestion_ts) if ingestion_ts else None
    client = client_from_env(env)
    ingest_ibge_localidades(
        client,
        bucket=bucket,
        execution_id=execution_id,
        ingestion_ts=parsed,
        base_url=base_url,
    )


def _parse_ingestion_ts(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = "ingestion-ts must be ISO-8601"
        raise IbgeLocalidadesError(msg) from exc
    if parsed.tzinfo is None:
        msg = "ingestion-ts must include a timezone"
        raise IbgeLocalidadesError(msg)
    return parsed


def _municipios_contract(
    contracts: tuple[DataContract, ...] | None,
) -> DataContract:
    loaded = load_contracts() if contracts is None else contracts
    matches = [
        item
        for item in loaded
        if item.source_id == SOURCE_ID and item.dataset == DATASET
    ]
    if len(matches) != 1:
        msg = "missing contract for ibge_localidades.municipios"
        raise IbgeLocalidadesError(msg)
    contract = matches[0]
    if contract.payload_format != "json" or contract.record_locator is not None:
        msg = "ibge_localidades.municipios contract must describe a JSON array"
        raise IbgeLocalidadesError(msg)
    if not contract.fields:
        msg = "ibge_localidades.municipios contract has no fields"
        raise IbgeLocalidadesError(msg)
    return contract


def _require_municipios(
    body: bytes,
    contract: DataContract,
) -> list[dict[str, object]]:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        msg = "IBGE localidades municipios payload is not UTF-8 JSON"
        raise IbgeLocalidadesError(msg) from exc
    if not isinstance(payload, list):
        msg = "IBGE localidades municipios payload must be a JSON array"
        raise IbgeLocalidadesError(msg)
    if not payload:
        msg = "IBGE localidades municipios payload is empty"
        raise IbgeLocalidadesError(msg)
    required_keys = {field.name for field in contract.fields if not field.nullable}
    allowed_keys = {field.name for field in contract.fields}
    records: list[dict[str, object]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            msg = (
                f"IBGE localidades municipios item {index} does not match the contract"
            )
            raise IbgeLocalidadesError(msg)
        keys = set(item)
        if not required_keys <= keys or not keys <= allowed_keys:
            msg = (
                f"IBGE localidades municipios item {index} does not match the contract"
            )
            raise IbgeLocalidadesError(msg)
        records.append(item)
    return records


def _origin(base_url: str) -> str:
    if (
        base_url.strip() != base_url
        or "\\" in base_url
        or not (base_url.startswith("https://") or base_url.startswith("http://"))
    ):
        msg = "base_url must be an http or https origin"
        raise IbgeLocalidadesError(msg)
    return base_url.rstrip("/")


def _require_positive_int(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        msg = f"{name} must be an integer >= 1"
        raise IbgeLocalidadesError(msg)
    return value


def _require_timeout(timeout: float) -> float:
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        msg = "timeout must be a positive number"
        raise IbgeLocalidadesError(msg)
    if timeout <= 0:
        msg = "timeout must be a positive number"
        raise IbgeLocalidadesError(msg)
    return float(timeout)


def _require_token(name: str, value: str) -> str:
    if not isinstance(value, str) or value.strip() != value or not value:
        msg = f"{name} must be non-empty"
        raise IbgeLocalidadesError(msg)
    return value
