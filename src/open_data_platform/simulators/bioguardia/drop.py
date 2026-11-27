"""Write one daily drop: gzip table files, the manifest, then ``_SUCCESS``.

A drop is complete only when ``_SUCCESS`` exists; it is written last, so a
crash leaves an incomplete drop that the next run regenerates. The manifest
always describes the complete files; a planted truncated or missing file is
what the platform's ingestion must detect against it.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import shutil
import tempfile
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO, Protocol

from open_data_platform.simulators.bioguardia import (
    GENERATOR_VERSION,
    OPS_SOURCE,
)
from open_data_platform.simulators.bioguardia.catalog import CATALOGS, SOURCE_SYSTEMS
from open_data_platform.simulators.bioguardia.clock import format_local, local_midnight
from open_data_platform.simulators.bioguardia.context import DayContext
from open_data_platform.simulators.bioguardia.rng import unit_of
from open_data_platform.simulators.bioguardia.spec import TableSpec, dec, parse_scaled
from open_data_platform.simulators.bioguardia.world import World

GZIP_LEVEL = 6
SUCCESS_MARKER = "_SUCCESS"
MANIFEST = "_manifest.json"
DEFAULT_MAX_DROP_BYTES = 2_000_000_000
RELEASE_HOURS = {OPS_SOURCE: 2, "bioguardia_finance": 6}
REDELIVERY_DELAY_DAYS = 3


class DropTooLargeError(RuntimeError):
    """A drop exceeded the byte cap; nothing was published."""


class Target(Protocol):
    def exists(self, key: str) -> bool: ...
    def put_file(self, key: str, path: Path, content_type: str) -> None: ...
    def put_bytes(self, key: str, body: bytes, content_type: str) -> None: ...


class DirectoryTarget:
    """A local directory that mirrors bucket keys (tests and dry runs)."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def exists(self, key: str) -> bool:
        return (self.root / key).exists()

    def put_file(self, key: str, path: Path, content_type: str) -> None:
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)

    def put_bytes(self, key: str, body: bytes, content_type: str) -> None:
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)


class BucketTarget:
    """An S3-compatible bucket (MinIO locally)."""

    def __init__(self, client: Any, bucket: str) -> None:
        from open_data_platform.object_storage import ensure_bucket

        self.client = client
        self.bucket = bucket
        ensure_bucket(client, bucket)

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 404 or exc.response.get("Error", {}).get("Code") in {
                "404",
                "NoSuchKey",
                "NotFound",
            }:
                return False
            raise
        return True

    def put_file(self, key: str, path: Path, content_type: str) -> None:
        self.client.upload_file(
            str(path), self.bucket, key, ExtraArgs={"ContentType": content_type}
        )

    def put_bytes(self, key: str, body: bytes, content_type: str) -> None:
        self.client.put_object(
            Bucket=self.bucket, Key=key, Body=body, ContentType=content_type
        )


class _HashingWriter:
    """File wrapper that counts bytes and hashes them as they are written."""

    def __init__(self, raw: BinaryIO) -> None:
        self.raw = raw
        self.digest = hashlib.sha256()
        self.size = 0

    def write(self, data: bytes) -> int:
        self.digest.update(data)
        self.size += len(data)
        return self.raw.write(data)

    def flush(self) -> None:
        self.raw.flush()


@dataclass
class TableFile:
    spec: TableSpec
    path: Path
    rows: int
    size: int
    uncompressed_size: int
    sha256: str
    schema_version: int
    columns: list[dict[str, str]]
    totals: dict[str, str]
    time_range: dict[str, str] | None
    seconds: float


@dataclass
class DropSummary:
    source: str
    day: date
    version: int
    tables: list[TableFile] = field(default_factory=list)
    file_defects: list[dict[str, Any]] = field(default_factory=list)
    seconds: float = 0.0

    @property
    def rows(self) -> int:
        return sum(t.rows for t in self.tables)

    @property
    def size(self) -> int:
        return sum(t.size for t in self.tables)


def drop_prefix(source: str, day: date, version: int) -> str:
    return f"{source}/export_date={day.isoformat()}/export_version={version}"


def release_at(source: str, day: date, version: int) -> int:
    """When a drop is published (epoch seconds)."""
    delay = 1 + (REDELIVERY_DELAY_DAYS if version == 2 else 0)
    return local_midnight(day + timedelta(days=delay)) + RELEASE_HOURS[source] * 3600


def versions_of(world: World, source: str, day: date) -> list[int]:
    """Version 1, plus version 2 on the dates scheduled for a re-delivery."""
    if (
        source == OPS_SOURCE
        and world.config.defects
        and unit_of(world.config.seed, "redeliver", source, day.isoformat()) < 1 / 20
    ):
        return [1, 2]
    return [1]


def _tap(
    spec: TableSpec, columns: list[str], rows: Iterable[Any], stats: dict[str, Any]
) -> Iterator[Any]:
    """Count rows and accumulate control totals while they stream to the file."""
    jsonl = spec.payload_format == "jsonl"
    totals = [
        (name, columns.index(name), spec.columns[spec.index(name)].scale or 0)
        for name in spec.totals
        if name in columns
    ]
    sums = [0] * len(totals)
    time_index = columns.index(spec.time_column) if spec.time_column in columns else -1
    low: str | None = None
    high: str | None = None
    count = 0
    for row in rows:
        count += 1
        for position, (name, index, scale) in enumerate(totals):
            value = row.get(name) if jsonl else row[index]
            if isinstance(value, int) and not isinstance(value, bool):
                sums[position] += value * 10**scale
            elif isinstance(value, str):
                parsed = parse_scaled(value, scale)
                if parsed is not None:
                    sums[position] += parsed
        if time_index >= 0:
            stamp = row.get(spec.time_column) if jsonl else row[time_index]
            # Only a new extreme is parsed, so the check stays cheap; a planted
            # malformed timestamp never becomes the range.
            if stamp and (low is None or stamp < low) and _is_timestamp(stamp):
                low = stamp
            if stamp and (high is None or stamp > high) and _is_timestamp(stamp):
                high = stamp
        yield row
    stats["rows"] = count
    stats["totals"] = {
        f"sum_{name}": dec(total, scale)
        for (name, _, scale), total in zip(totals, sums, strict=True)
    }
    stats["time_range"] = (
        {"column": spec.time_column, "min": low, "max": high}
        if low is not None
        else None
    )


def _is_timestamp(text: str) -> bool:
    try:
        datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


def write_table(spec: TableSpec, ctx: DayContext, directory: Path) -> TableFile:
    began = time.monotonic()
    columns = spec.columns_for(ctx)
    names = [c.name for c in columns]
    path = directory / spec.file_name
    stats: dict[str, Any] = {}
    with path.open("wb") as raw:
        hashing = _HashingWriter(raw)
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=hashing, compresslevel=GZIP_LEVEL, mtime=0
        ) as packed:
            text = io.TextIOWrapper(packed, encoding="utf-8", newline="")
            rows = _tap(spec, names, spec.rows(ctx), stats)
            if spec.payload_format == "jsonl":
                dumps = json.JSONEncoder(
                    ensure_ascii=False, separators=(",", ":")
                ).encode
                for row in rows:
                    text.write(dumps(row))
                    text.write("\n")
            else:
                writer = csv.writer(text, lineterminator="\n")
                writer.writerow(names)
                writer.writerows(rows)
            text.flush()
            text.detach()
            # GzipFile counts the uncompressed bytes it wrote (exact above 4 GiB,
            # unlike the trailer's ISIZE, which is the length modulo 2**32).
            counted = getattr(packed, "size", None)
    if counted is None:
        with path.open("rb") as written:
            written.seek(-4, 2)
            counted = int.from_bytes(written.read(4), "little")
    uncompressed = int(counted)
    return TableFile(
        spec=spec,
        path=path,
        rows=stats["rows"],
        size=hashing.size,
        uncompressed_size=uncompressed,
        sha256=hashing.digest.hexdigest(),
        schema_version=spec.schema_version(ctx),
        columns=[{"name": c.name, "type": c.type} for c in columns],
        totals=stats["totals"],
        time_range=stats["time_range"],
        seconds=round(time.monotonic() - began, 3),
    )


def _file_defects(
    world: World, source: str, day: date, version: int, tables: list[TableFile]
) -> list[dict[str, Any]]:
    """Truncated and missing files, planted in version 1 only."""
    if version != 1 or not world.config.defects or not tables:
        return []
    seed = world.config.seed
    out = []
    if unit_of(seed, "truncate", source, day.isoformat()) < 1 / 30:
        target = tables[
            int(unit_of(seed, "truncate-table", source, day.isoformat()) * len(tables))
        ]
        fraction = 0.6 + 0.3 * unit_of(seed, "truncate-cut", source, day.isoformat())
        out.append(
            {
                "defect": "truncated_file",
                "table": target.spec.name,
                "file": target.spec.file_name,
                "kept_bytes": int(target.size * fraction),
            }
        )
    if unit_of(seed, "missing", source, day.isoformat()) < 1 / 45:
        target = tables[
            int(unit_of(seed, "missing-table", source, day.isoformat()) * len(tables))
        ]
        if not any(d["table"] == target.spec.name for d in out):
            out.append(
                {
                    "defect": "missing_file",
                    "table": target.spec.name,
                    "file": target.spec.file_name,
                }
            )
    return out


def manifest_document(world: World, summary: DropSummary) -> dict[str, Any]:
    cfg = world.config
    return {
        "source_id": summary.source,
        "source_system": SOURCE_SYSTEMS[summary.source],
        "export_date": summary.day.isoformat(),
        "export_version": summary.version,
        "released_at": format_local(
            release_at(summary.source, summary.day, summary.version)
        ),
        "timezone": "-03:00",
        "generator_version": GENERATOR_VERSION,
        "seed": cfg.seed,
        "scale": cfg.scale,
        "table_count": len(summary.tables),
        "tables": [
            {
                "table": t.spec.name,
                "file": t.spec.file_name,
                "format": t.spec.payload_format,
                "compression": "gzip",
                "change_type": t.spec.change_type,
                "key": list(t.spec.key),
                "schema_version": t.schema_version,
                "rows": t.rows,
                "bytes": t.size,
                "uncompressed_bytes": t.uncompressed_size,
                "sha256": t.sha256,
                "columns": t.columns,
                "control_totals": t.totals,
                "time_range": t.time_range,
            }
            for t in summary.tables
        ],
    }


def generate_drop(
    world: World,
    source: str,
    day: date,
    version: int,
    target: Target,
    evaluation: Target | None,
    *,
    work_dir: Path | None = None,
    max_bytes: int = DEFAULT_MAX_DROP_BYTES,
) -> DropSummary:
    """Generate and publish one drop; ``_SUCCESS`` is the last object written."""
    began = time.monotonic()
    ctx = DayContext(world, day, version)
    summary = DropSummary(source=source, day=day, version=version)
    prefix = drop_prefix(source, day, version)
    with tempfile.TemporaryDirectory(prefix="bioguardia-", dir=work_dir) as tmp:
        directory = Path(tmp)
        for spec in CATALOGS[source]:
            if spec.present(ctx):
                summary.tables.append(write_table(spec, ctx, directory))
        if summary.size > max_bytes:
            msg = (
                f"{source} {day} v{version}: {summary.size} bytes "
                f"exceed the cap of {max_bytes}"
            )
            raise DropTooLargeError(msg)
        summary.file_defects = _file_defects(
            world, source, day, version, summary.tables
        )
        missing = {
            d["table"] for d in summary.file_defects if d["defect"] == "missing_file"
        }
        truncated = {
            d["table"]: d["kept_bytes"]
            for d in summary.file_defects
            if d["defect"] == "truncated_file"
        }
        for table in summary.tables:
            name = table.spec.name
            if name in missing:
                continue
            content_type = "application/gzip"
            if name in truncated:
                cut = directory / f"{table.spec.file_name}.cut"
                with table.path.open("rb") as source_file, cut.open("wb") as out:
                    out.write(source_file.read(truncated[name]))
                target.put_file(f"{prefix}/{table.spec.file_name}", cut, content_type)
            else:
                target.put_file(
                    f"{prefix}/{table.spec.file_name}", table.path, content_type
                )
        manifest = json.dumps(
            manifest_document(world, summary), indent=2, sort_keys=False
        ).encode("utf-8")
        target.put_bytes(f"{prefix}/{MANIFEST}", manifest, "application/json")
        if evaluation is not None:
            ctx.planted.file_defects.extend(summary.file_defects)
            planted = {
                "source_id": source,
                "export_date": day.isoformat(),
                "export_version": version,
                **ctx.planted.as_document(),
            }
            evaluation.put_bytes(
                f"{prefix}/planted.json",
                json.dumps(planted, separators=(",", ":")).encode("utf-8"),
                "application/json",
            )
        marker = {
            "source_id": source,
            "export_date": day.isoformat(),
            "export_version": version,
            "table_count": len(summary.tables),
            "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
        }
        target.put_bytes(
            f"{prefix}/{SUCCESS_MARKER}",
            json.dumps(marker).encode("utf-8"),
            "application/json",
        )
    summary.seconds = round(time.monotonic() - began, 3)
    return summary
