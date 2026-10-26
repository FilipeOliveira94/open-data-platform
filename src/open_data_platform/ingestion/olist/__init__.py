"""Fetch Olist historical snapshots and land them in the raw layout.

E-commerce and the marketing funnel are one historical family. The published
join is ``closed_deals.seller_id`` = ``sellers.seller_id`` or
``order_items.seller_id``. This module checks that those columns exist and
writes each CSV as its own raw object. It does not build a joined table.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from open_data_platform.contracts.load import load_contracts
from open_data_platform.contracts.records import parsed_csv_record
from open_data_platform.ingestion.kaggle_download import (
    KaggleCredentials,
    KaggleDownloadError,
    Transport,
    credentials_from_env,
    download_dataset_zip,
    extract_expected_members,
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

_ECOMMERCE_SOURCE = "olist_brazilian_ecommerce"
_FUNNEL_SOURCE = "olist_marketing_funnel"
_SELLER_JOIN = (
    (_FUNNEL_SOURCE, "closed_deals"),
    (_ECOMMERCE_SOURCE, "sellers"),
    (_ECOMMERCE_SOURCE, "order_items"),
)


class OlistIngestError(ValueError):
    """An Olist snapshot could not be fetched or landed."""


@dataclass(frozen=True, slots=True)
class OlistFile:
    """One published CSV and the contract dataset it feeds."""

    object_name: str
    dataset: str


@dataclass(frozen=True, slots=True)
class OlistSource:
    """One Kaggle dataset slug and the files extracted from it."""

    slug: str
    source_id: str
    files: tuple[OlistFile, ...]


@dataclass(frozen=True, slots=True)
class FetchResult:
    """Which slugs were downloaded and which staging trees were already complete."""

    downloaded: tuple[str, ...]
    skipped: tuple[str, ...]


ECOMMERCE = OlistSource(
    slug="olistbr/brazilian-ecommerce",
    source_id=_ECOMMERCE_SOURCE,
    files=(
        OlistFile("olist_customers_dataset.csv", "customers"),
        OlistFile("olist_geolocation_dataset.csv", "geolocation"),
        OlistFile("olist_order_items_dataset.csv", "order_items"),
        OlistFile("olist_order_payments_dataset.csv", "order_payments"),
        OlistFile("olist_order_reviews_dataset.csv", "order_reviews"),
        OlistFile("olist_orders_dataset.csv", "orders"),
        OlistFile("olist_products_dataset.csv", "products"),
        OlistFile("olist_sellers_dataset.csv", "sellers"),
        OlistFile(
            "product_category_name_translation.csv",
            "product_category_name_translation",
        ),
    ),
)
FUNNEL = OlistSource(
    slug="olistbr/marketing-funnel-olist",
    source_id=_FUNNEL_SOURCE,
    files=(
        OlistFile("olist_marketing_qualified_leads_dataset.csv", "qualified_leads"),
        OlistFile("olist_closed_deals_dataset.csv", "closed_deals"),
    ),
)
OLIST_SOURCES = (ECOMMERCE, FUNNEL)


def olist_staging_dirs(
    env: Mapping[str, str],
    *,
    repo_root: Path | None = None,
) -> tuple[Path, Path]:
    """Return ``(ecommerce_dir, funnel_dir)``.

    Set paths come from the environment as given. Unset paths are the
    documented ``data/raw/{source_id}`` directories under the repository root.
    """
    root = _repo_root() if repo_root is None else repo_root
    ecommerce = env.get("ODP_OLIST_ECOMMERCE_DIR", "").strip()
    funnel = env.get("ODP_OLIST_FUNNEL_DIR", "").strip()
    ecommerce_dir = (
        Path(ecommerce) if ecommerce else root / "data/raw/olist_brazilian_ecommerce"
    )
    funnel_dir = Path(funnel) if funnel else root / "data/raw/olist_marketing_funnel"
    return ecommerce_dir, funnel_dir


def require_seller_join(
    contracts: Mapping[tuple[str, str], DataContract],
) -> None:
    """Require the published ``seller_id`` columns on the three joined datasets."""
    for source_id, dataset in _SELLER_JOIN:
        contract = contracts.get((source_id, dataset))
        if contract is None:
            msg = f"missing contract for {source_id}.{dataset}"
            raise OlistIngestError(msg)
        field = next(
            (item for item in contract.fields if item.name == "seller_id"),
            None,
        )
        if field is None or field.field_type != "string" or field.nullable:
            msg = f"{source_id}.{dataset} seller_id must be a required string"
            raise OlistIngestError(msg)


def fetch_olist_snapshots(
    *,
    ecommerce_dir: Path,
    funnel_dir: Path,
    credentials: KaggleCredentials,
    force: bool = False,
    transport: Transport | None = None,
) -> FetchResult:
    """Download any Olist slug whose expected CSVs are not already staged.

    One slug is downloaded and extracted before the next starts. ``force``
    downloads again and replaces those CSVs. A failed archive does not
    publish a partial extract of that slug.
    """
    directories = {
        ECOMMERCE.source_id: ecommerce_dir,
        FUNNEL.source_id: funnel_dir,
    }
    downloaded: list[str] = []
    skipped: list[str] = []
    for source in OLIST_SOURCES:
        fetched = _fetch_one_source(
            source,
            directories[source.source_id],
            credentials=credentials,
            force=force,
            transport=transport,
        )
        if fetched:
            downloaded.append(source.slug)
        else:
            skipped.append(source.slug)
    return FetchResult(downloaded=tuple(downloaded), skipped=tuple(skipped))


def ingest_olist_historical(
    client: BaseClient,
    *,
    bucket: str,
    execution_id: str,
    ingestion_ts: datetime | None = None,
    ecommerce_dir: Path,
    funnel_dir: Path,
    contracts: tuple[DataContract, ...] | None = None,
    sources: tuple[OlistSource, ...] = OLIST_SOURCES,
) -> tuple[RawWriteResult, ...]:
    """Read, validate, and write one staged CSV at a time.

    The file body is released after its put. A structural failure stops the
    remaining files and does not write a quarantine sidecar for that file.
    Objects already written for this execution stay in place and are
    overwritten when the same ``execution_id`` and ``ingestion_ts`` run again.
    Invalid rows stay in the CSV and are copied to the quarantine key.
    """
    loaded = load_contracts() if contracts is None else contracts
    by_identity = {(item.source_id, item.dataset): item for item in loaded}
    require_seller_join(by_identity)
    directories = {
        ECOMMERCE.source_id: ecommerce_dir,
        FUNNEL.source_id: funnel_dir,
    }
    ts = datetime.now(UTC) if ingestion_ts is None else ingestion_ts
    results: list[RawWriteResult] = []
    for source in sources:
        for spec in source.files:
            contract = by_identity.get((source.source_id, spec.dataset))
            if contract is None:
                msg = f"missing contract for {source.source_id}.{spec.dataset}"
                raise OlistIngestError(msg)
            if contract.payload_format != "csv":
                msg = f"{source.source_id}.{spec.dataset} payload must be csv"
                raise OlistIngestError(msg)
            path = directories[source.source_id] / spec.object_name
            if not path.is_file():
                msg = f"missing staged file: {spec.object_name}"
                raise OlistIngestError(msg)
            body = path.read_bytes()
            records = _csv_records(body, contract, spec.object_name)
            try:
                sidecar = prepare_quarantine(contract, records)
            except InvalidRecordsError as exc:
                raise OlistIngestError(str(exc)) from exc
            result = write_raw_object(
                client,
                bucket=bucket,
                body=body,
                source_id=source.source_id,
                dataset=spec.dataset,
                execution_id=execution_id,
                object_name=spec.object_name,
                ingestion_ts=ts,
                content_type="text/csv",
            )
            write_quarantine_object(
                client,
                bucket=bucket,
                source_id=source.source_id,
                dataset=spec.dataset,
                execution_id=execution_id,
                object_name=spec.object_name,
                ingestion_ts=result.metadata.ingestion_ts,
                body=sidecar,
            )
            results.append(result)
    return tuple(results)


def run_olist_historical(
    client: BaseClient,
    *,
    bucket: str,
    execution_id: str,
    ingestion_ts: datetime | None = None,
    ecommerce_dir: Path | None = None,
    funnel_dir: Path | None = None,
    force: bool = False,
    env: Mapping[str, str] | None = None,
    transport: Transport | None = None,
) -> tuple[RawWriteResult, ...]:
    """Fetch one Olist slug, write its CSVs, then do the next slug.

    Ecommerce is finished before the marketing funnel download starts.
    """
    settings = os.environ if env is None else env
    default_ecommerce, default_funnel = olist_staging_dirs(settings)
    ecommerce = default_ecommerce if ecommerce_dir is None else ecommerce_dir
    funnel = default_funnel if funnel_dir is None else funnel_dir
    credentials = credentials_from_env(settings)
    ts = datetime.now(UTC) if ingestion_ts is None else ingestion_ts
    results: list[RawWriteResult] = []
    for source, directory in ((ECOMMERCE, ecommerce), (FUNNEL, funnel)):
        _fetch_one_source(
            source,
            directory,
            credentials=credentials,
            force=force,
            transport=transport,
        )
        results.extend(
            ingest_olist_historical(
                client,
                bucket=bucket,
                execution_id=execution_id,
                ingestion_ts=ts,
                ecommerce_dir=ecommerce,
                funnel_dir=funnel,
                sources=(source,),
            )
        )
    return tuple(results)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint. Credentials are resolved before any storage client."""
    parser = argparse.ArgumentParser(
        description="Fetch Olist snapshots and write them to raw storage.",
    )
    parser.add_argument("--execution-id", required=True)
    parser.add_argument(
        "--ingestion-ts",
        default=None,
        help="UTC ISO-8601 timestamp. The same value overwrites the same keys.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Download again even when the staged CSVs are already present.",
    )
    args = parser.parse_args(argv)
    try:
        _run_cli(
            execution_id=args.execution_id,
            ingestion_ts=args.ingestion_ts,
            force=args.force,
        )
    except (OlistIngestError, KaggleDownloadError, KeyError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _run_cli(*, execution_id: str, ingestion_ts: str | None, force: bool) -> None:
    env = os.environ
    credentials_from_env(env)
    bucket = env.get("ODP_RAW_BUCKET", "").strip()
    if not bucket:
        msg = "ODP_RAW_BUCKET is required"
        raise OlistIngestError(msg)
    parsed = _parse_ingestion_ts(ingestion_ts) if ingestion_ts else None
    client = client_from_env(env)
    run_olist_historical(
        client,
        bucket=bucket,
        execution_id=execution_id,
        ingestion_ts=parsed,
        force=force,
        env=env,
    )


def _parse_ingestion_ts(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = "ingestion-ts must be ISO-8601"
        raise OlistIngestError(msg) from exc
    if parsed.tzinfo is None:
        msg = "ingestion-ts must include a timezone"
        raise OlistIngestError(msg)
    return parsed


def _fetch_one_source(
    source: OlistSource,
    dest: Path,
    *,
    credentials: KaggleCredentials,
    force: bool,
    transport: Transport | None,
) -> bool:
    """Download and extract one slug. Return False when staging is reused."""
    names = frozenset(item.object_name for item in source.files)
    if not force and _staging_complete(dest, names):
        return False
    payload = download_dataset_zip(
        slug=source.slug,
        credentials=credentials,
        transport=transport,
    )
    extract_expected_members(payload, dest, names)
    return True


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file() and (
            parent / "config" / "contracts"
        ).is_dir():
            return parent
    msg = "repository root was not found"
    raise OlistIngestError(msg)


def _staging_complete(directory: Path, names: frozenset[str]) -> bool:
    return all((directory / name).is_file() for name in names)


def _csv_header(body: bytes, object_name: str) -> list[str]:
    try:
        # The published category file starts with a UTF-8 BOM.
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        msg = f"staged file is not UTF-8: {object_name}"
        raise OlistIngestError(msg) from exc
    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = next(reader)
    except StopIteration as exc:
        msg = f"staged file has no header: {object_name}"
        raise OlistIngestError(msg) from exc
    if not header or any(not name for name in header):
        msg = f"staged file has an invalid header: {object_name}"
        raise OlistIngestError(msg)
    return header


def _csv_records(
    body: bytes,
    contract: DataContract,
    object_name: str,
) -> tuple[dict[str, object], ...]:
    header = _csv_header(body, object_name)
    expected = {field.name for field in contract.fields}
    if len(header) != len(set(header)) or set(header) != expected:
        msg = f"CSV header does not match contract for {contract.dataset}"
        raise OlistIngestError(msg)
    text = body.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text, newline=""))
    next(reader)
    records: list[dict[str, object]] = []
    for index, row in enumerate(reader):
        if len(row) != len(header):
            msg = f"CSV row {index} does not match contract for {contract.dataset}"
            raise OlistIngestError(msg)
        cells = dict(zip(header, row, strict=True))
        records.append(parsed_csv_record(contract, cells))
    return tuple(records)
