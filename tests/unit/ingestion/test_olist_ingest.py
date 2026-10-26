"""Offline tests for Olist historical ingestion."""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.model import DataContract
from open_data_platform.ingestion.kaggle_download import (
    DownloadRequest,
    DownloadResponse,
    KaggleCredentials,
    KaggleDownloadError,
    credentials_from_env,
)
from open_data_platform.ingestion.olist import (
    ECOMMERCE,
    FUNNEL,
    OLIST_SOURCES,
    OlistFile,
    OlistIngestError,
    OlistSource,
    fetch_olist_snapshots,
    ingest_olist_historical,
    main,
    olist_staging_dirs,
    require_seller_join,
    run_olist_historical,
)
from open_data_platform.ingestion.paths import build_raw_object_key

_TS = datetime(2026, 10, 22, 15, 0, tzinfo=UTC)


def _contracts() -> dict[tuple[str, str], DataContract]:
    return {(item.source_id, item.dataset): item for item in load_contracts()}


def _csv_bytes(names: list[str], *, row: list[str] | None = None) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(names)
    if row is not None:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def _matching_bytes(contract: DataContract) -> bytes:
    return _csv_bytes([field.name for field in contract.fields])


def _stage_source(
    directory: Path,
    source: OlistSource,
    contracts: dict[tuple[str, str], DataContract],
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for spec in source.files:
        contract = contracts[(source.source_id, spec.dataset)]
        (directory / spec.object_name).write_bytes(_matching_bytes(contract))


def _stage_all(
    ecommerce_dir: Path,
    funnel_dir: Path,
) -> dict[tuple[str, str], DataContract]:
    contracts = _contracts()
    _stage_source(ecommerce_dir, ECOMMERCE, contracts)
    _stage_source(funnel_dir, FUNNEL, contracts)
    return contracts


def _zip(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return buffer.getvalue()


def _source_zip(
    source: OlistSource,
    contracts: dict[tuple[str, str], DataContract],
) -> bytes:
    members = {
        spec.object_name: _matching_bytes(contracts[(source.source_id, spec.dataset)])
        for spec in source.files
    }
    return _zip(members)


def _client() -> MagicMock:
    client = MagicMock()
    client.head_bucket.return_value = {}
    return client


def test_file_map_covers_olist_contracts() -> None:
    contracts = _contracts()
    mapped = {
        (source.source_id, spec.dataset)
        for source in OLIST_SOURCES
        for spec in source.files
    }
    olist = {identity for identity in contracts if identity[0].startswith("olist_")}
    assert mapped == olist
    names = [spec.object_name for source in OLIST_SOURCES for spec in source.files]
    assert len(names) == len(set(names)) == 11


def test_seller_join_contracts_record_both_ecommerce_targets() -> None:
    contracts = _contracts()
    require_seller_join(contracts)
    closed = contracts[("olist_marketing_funnel", "closed_deals")]
    sellers = contracts[("olist_brazilian_ecommerce", "sellers")]
    items = contracts[("olist_brazilian_ecommerce", "order_items")]
    assert closed.contract_version == 3
    assert sellers.contract_version == 3
    assert items.contract_version == 2
    closed_seller = next(field for field in closed.fields if field.name == "seller_id")
    assert "sellers.seller_id" in closed_seller.description
    assert "order_items.seller_id" in closed_seller.description
    assert contracts[("olist_brazilian_ecommerce", "orders")].contract_version == 1
    leads = contracts[("olist_marketing_funnel", "qualified_leads")]
    assert all(field.name != "seller_id" for field in leads.fields)


def test_require_seller_join_rejects_a_missing_column() -> None:
    contracts = _contracts()
    sellers = contracts[("olist_brazilian_ecommerce", "sellers")]
    contracts[("olist_brazilian_ecommerce", "sellers")] = replace(
        sellers,
        fields=tuple(field for field in sellers.fields if field.name != "seller_id"),
    )
    with pytest.raises(OlistIngestError, match="seller_id"):
        require_seller_join(contracts)


def test_default_staging_dirs_are_repo_relative(tmp_path: Path) -> None:
    ecommerce, funnel = olist_staging_dirs({}, repo_root=tmp_path)
    assert ecommerce == tmp_path / "data/raw/olist_brazilian_ecommerce"
    assert funnel == tmp_path / "data/raw/olist_marketing_funnel"


def test_staging_dirs_use_environment_paths(tmp_path: Path) -> None:
    ecommerce, funnel = olist_staging_dirs(
        {
            "ODP_OLIST_ECOMMERCE_DIR": str(tmp_path / "ecom"),
            "ODP_OLIST_FUNNEL_DIR": str(tmp_path / "funnel"),
        },
        repo_root=tmp_path,
    )
    assert ecommerce == tmp_path / "ecom"
    assert funnel == tmp_path / "funnel"


def test_fetch_skips_complete_staging_until_forced(tmp_path: Path) -> None:
    ecommerce = tmp_path / "ecom"
    funnel = tmp_path / "funnel"
    _stage_all(ecommerce, funnel)
    calls = 0

    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del request, timeout
        nonlocal calls
        calls += 1
        raise AssertionError("download attempted")

    credentials_env = {"KAGGLE_API_TOKEN": "token-value"}
    first = fetch_olist_snapshots(
        ecommerce_dir=ecommerce,
        funnel_dir=funnel,
        credentials=credentials_from_env(credentials_env),
        transport=transport,
    )
    assert first.skipped == (ECOMMERCE.slug, FUNNEL.slug)
    assert first.downloaded == ()
    assert calls == 0

    def forced(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del timeout
        nonlocal calls
        calls += 1
        contracts = _contracts()
        if ECOMMERCE.slug in request.url:
            return DownloadResponse(200, {}, _source_zip(ECOMMERCE, contracts))
        return DownloadResponse(200, {}, _source_zip(FUNNEL, contracts))

    (ecommerce / "olist_orders_dataset.csv").write_bytes(b"stale")
    second = fetch_olist_snapshots(
        ecommerce_dir=ecommerce,
        funnel_dir=funnel,
        credentials=credentials_from_env(credentials_env),
        force=True,
        transport=forced,
    )
    assert second.downloaded == (ECOMMERCE.slug, FUNNEL.slug)
    assert calls == 2
    assert b"order_id" in (ecommerce / "olist_orders_dataset.csv").read_bytes()


def test_fetch_stops_before_raw_when_the_second_archive_is_rejected(
    tmp_path: Path,
) -> None:
    contracts = _contracts()

    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del timeout
        if ECOMMERCE.slug in request.url:
            return DownloadResponse(200, {}, _source_zip(ECOMMERCE, contracts))
        return DownloadResponse(401, {}, b"nope")

    with pytest.raises(KaggleDownloadError, match="status 401"):
        fetch_olist_snapshots(
            ecommerce_dir=tmp_path / "ecom",
            funnel_dir=tmp_path / "funnel",
            credentials=KaggleCredentials(token="token-value"),
            transport=transport,
        )

    assert (tmp_path / "ecom" / "olist_orders_dataset.csv").is_file()
    assert not (tmp_path / "funnel").exists()


def test_ingest_writes_eleven_traceable_objects(tmp_path: Path) -> None:
    ecommerce = tmp_path / "ecom"
    funnel = tmp_path / "funnel"
    _stage_all(ecommerce, funnel)
    client = _client()

    results = ingest_olist_historical(
        client,
        bucket="odp-raw",
        execution_id="run-001",
        ingestion_ts=_TS,
        ecommerce_dir=ecommerce,
        funnel_dir=funnel,
    )

    assert len(results) == 11
    assert client.put_object.call_count == 11
    keys = {result.key for result in results}
    assert (
        build_raw_object_key(
            source_id="olist_brazilian_ecommerce",
            dataset="orders",
            execution_id="run-001",
            object_name="olist_orders_dataset.csv",
            ingestion_date=_TS.date(),
        )
        in keys
    )
    assert (
        build_raw_object_key(
            source_id="olist_marketing_funnel",
            dataset="closed_deals",
            execution_id="run-001",
            object_name="olist_closed_deals_dataset.csv",
            ingestion_date=_TS.date(),
        )
        in keys
    )
    orders = next(
        item
        for item in results
        if item.metadata.source_id.endswith("ecommerce")
        and item.key.endswith("olist_orders_dataset.csv")
    )
    assert orders.metadata.to_object_metadata() == {
        "source_id": "olist_brazilian_ecommerce",
        "execution_id": "run-001",
        "_ingestion_ts": "2026-10-22T15:00:00Z",
    }
    put = client.put_object.call_args_list[0].kwargs
    assert put["ContentType"] == "text/csv"
    assert put["Metadata"]["source_id"]


def test_ingest_same_identity_overwrites_the_same_key(tmp_path: Path) -> None:
    ecommerce = tmp_path / "ecom"
    funnel = tmp_path / "funnel"
    contracts = _stage_all(ecommerce, funnel)
    client = _client()
    kwargs: dict[str, Any] = {
        "bucket": "odp-raw",
        "execution_id": "run-001",
        "ingestion_ts": _TS,
        "ecommerce_dir": ecommerce,
        "funnel_dir": funnel,
    }
    first = ingest_olist_historical(client, **kwargs)
    orders = next(spec for spec in ECOMMERCE.files if spec.dataset == "orders")
    contract = contracts[(ECOMMERCE.source_id, orders.dataset)]
    (ecommerce / orders.object_name).write_bytes(
        _csv_bytes([field.name for field in contract.fields], row=["x"])
    )
    second = ingest_olist_historical(client, **kwargs)

    assert [item.key for item in first] == [item.key for item in second]
    bodies = [
        call.kwargs["Body"]
        for call in client.put_object.call_args_list
        if call.kwargs["Key"].endswith("/olist_orders_dataset.csv")
    ]
    assert bodies[0] != bodies[1]
    assert bodies[1].splitlines()[-1] == b"x"


def test_different_execution_id_uses_a_different_key(tmp_path: Path) -> None:
    ecommerce = tmp_path / "ecom"
    funnel = tmp_path / "funnel"
    _stage_all(ecommerce, funnel)
    client = _client()
    first = ingest_olist_historical(
        client,
        bucket="odp-raw",
        execution_id="run-001",
        ingestion_ts=_TS,
        ecommerce_dir=ecommerce,
        funnel_dir=funnel,
    )
    second = ingest_olist_historical(
        client,
        bucket="odp-raw",
        execution_id="run-002",
        ingestion_ts=_TS,
        ecommerce_dir=ecommerce,
        funnel_dir=funnel,
    )
    assert first[0].key != second[0].key


def _put_keys(client: MagicMock) -> list[str]:
    return [call.kwargs["Key"] for call in client.put_object.call_args_list]


def test_header_mismatch_stops_at_that_file(tmp_path: Path) -> None:
    ecommerce = tmp_path / "ecom"
    funnel = tmp_path / "funnel"
    _stage_all(ecommerce, funnel)
    (ecommerce / "olist_orders_dataset.csv").write_text("extra\n", encoding="utf-8")
    client = _client()

    with pytest.raises(OlistIngestError, match="header"):
        ingest_olist_historical(
            client,
            bucket="odp-raw",
            execution_id="run-001",
            ingestion_ts=_TS,
            ecommerce_dir=ecommerce,
            funnel_dir=funnel,
        )

    keys = _put_keys(client)
    assert len(keys) == 5
    assert all(not key.endswith("/olist_orders_dataset.csv") for key in keys)
    assert keys[-1].endswith("/olist_order_reviews_dataset.csv")


def test_missing_file_stops_at_that_file(tmp_path: Path) -> None:
    ecommerce = tmp_path / "ecom"
    funnel = tmp_path / "funnel"
    _stage_all(ecommerce, funnel)
    (ecommerce / "olist_orders_dataset.csv").unlink()
    client = _client()

    with pytest.raises(OlistIngestError, match="missing staged file"):
        ingest_olist_historical(
            client,
            bucket="odp-raw",
            execution_id="run-001",
            ingestion_ts=_TS,
            ecommerce_dir=ecommerce,
            funnel_dir=funnel,
        )

    keys = _put_keys(client)
    assert len(keys) == 5
    assert all(not key.endswith("/olist_orders_dataset.csv") for key in keys)


def test_utf8_bom_matches_the_contract_and_raw_keeps_the_bom(tmp_path: Path) -> None:
    ecommerce = tmp_path / "ecom"
    funnel = tmp_path / "funnel"
    _stage_all(ecommerce, funnel)
    path = ecommerce / "product_category_name_translation.csv"
    original = path.read_bytes()
    path.write_bytes(b"\xef\xbb\xbf" + original)
    client = _client()

    ingest_olist_historical(
        client,
        bucket="odp-raw",
        execution_id="run-001",
        ingestion_ts=_TS,
        ecommerce_dir=ecommerce,
        funnel_dir=funnel,
    )

    body = next(
        call.kwargs["Body"]
        for call in client.put_object.call_args_list
        if call.kwargs["Key"].endswith("/product_category_name_translation.csv")
    )
    assert body.startswith(b"\xef\xbb\xbf")
    assert body == b"\xef\xbb\xbf" + original


def test_invalid_utf8_stops_at_that_file(tmp_path: Path) -> None:
    ecommerce = tmp_path / "ecom"
    funnel = tmp_path / "funnel"
    _stage_all(ecommerce, funnel)
    (funnel / "olist_closed_deals_dataset.csv").write_bytes(b"\xff")
    client = _client()

    with pytest.raises(OlistIngestError, match="UTF-8"):
        ingest_olist_historical(
            client,
            bucket="odp-raw",
            execution_id="run-001",
            ingestion_ts=_TS,
            ecommerce_dir=ecommerce,
            funnel_dir=funnel,
        )

    keys = _put_keys(client)
    assert len(keys) == 10
    assert all(not key.endswith("/olist_closed_deals_dataset.csv") for key in keys)
    assert keys[-1].endswith("/olist_marketing_qualified_leads_dataset.csv")


def test_run_stops_before_the_funnel_download_when_ecommerce_fails(
    tmp_path: Path,
) -> None:
    seen: list[str] = []

    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del timeout
        if ECOMMERCE.slug in request.url:
            seen.append(request.url)
            members = {
                spec.object_name: b"not_a_contract_column\n" for spec in ECOMMERCE.files
            }
            return DownloadResponse(200, {}, _zip(members))
        raise AssertionError(request.url)

    with pytest.raises(OlistIngestError, match="header"):
        run_olist_historical(
            _client(),
            bucket="odp-raw",
            execution_id="run-001",
            ingestion_ts=_TS,
            ecommerce_dir=tmp_path / "ecom",
            funnel_dir=tmp_path / "funnel",
            env={"KAGGLE_API_TOKEN": "token-value"},
            transport=transport,
        )

    assert len(seen) == 1
    assert not (tmp_path / "funnel").exists()


def test_run_fetches_then_writes_without_a_joined_object(tmp_path: Path) -> None:
    contracts = _contracts()
    calls = 0

    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del timeout
        nonlocal calls
        calls += 1
        if ECOMMERCE.slug in request.url:
            assert request.headers["Authorization"] == "Bearer token-value"
            return DownloadResponse(200, {}, _source_zip(ECOMMERCE, contracts))
        if FUNNEL.slug in request.url:
            return DownloadResponse(200, {}, _source_zip(FUNNEL, contracts))
        raise AssertionError(request.url)

    client = _client()
    results = run_olist_historical(
        client,
        bucket="odp-raw",
        execution_id="run-001",
        ingestion_ts=_TS,
        ecommerce_dir=tmp_path / "ecom",
        funnel_dir=tmp_path / "funnel",
        env={"KAGGLE_API_TOKEN": "token-value"},
        transport=transport,
    )

    assert calls == 2
    assert len(results) == 11
    assert all("/joined" not in result.key for result in results)
    datasets = {result.key.split("/")[2] for result in results}
    assert "closed_deals" in datasets
    assert "sellers" in datasets
    assert "order_items" in datasets
    assert "qualified_leads" in datasets


def test_run_missing_credentials_does_not_download(tmp_path: Path) -> None:
    def transport(request: DownloadRequest, timeout: float) -> DownloadResponse:
        del request, timeout
        raise AssertionError("download attempted")

    with pytest.raises(KaggleDownloadError, match="missing"):
        run_olist_historical(
            _client(),
            bucket="odp-raw",
            execution_id="run-001",
            ecommerce_dir=tmp_path / "ecom",
            funnel_dir=tmp_path / "funnel",
            env={},
            transport=transport,
        )


def test_main_missing_credentials_does_not_open_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.delenv("KAGGLE_USERNAME", raising=False)
    monkeypatch.delenv("KAGGLE_KEY", raising=False)

    def boom(env: object) -> None:
        del env
        raise AssertionError("storage client created")

    monkeypatch.setattr(
        "open_data_platform.ingestion.olist.client_from_env",
        boom,
    )
    assert main(["--execution-id", "run-001"]) == 1


def test_spec_objects_are_the_published_names() -> None:
    by_dataset = {
        spec.dataset: spec.object_name
        for source in OLIST_SOURCES
        for spec in source.files
    }
    assert by_dataset["orders"] == "olist_orders_dataset.csv"
    assert (
        by_dataset["qualified_leads"] == "olist_marketing_qualified_leads_dataset.csv"
    )
    assert isinstance(by_dataset["sellers"], str)
    assert OlistFile("olist_sellers_dataset.csv", "sellers") in ECOMMERCE.files
