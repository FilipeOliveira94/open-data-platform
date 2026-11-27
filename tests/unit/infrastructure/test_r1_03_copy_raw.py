"""R1-03 raw copy: only the source id in metadata and markers changes."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


copy_raw = _load("r1_03_copy_raw", ROOT / "scripts" / "r1_03_copy_raw.py")
FakeS3 = _load("fake_s3", ROOT / "tests" / "unit" / "lakehouse" / "fake_s3.py").FakeS3

BUCKET = "raw-bucket"
RUN = "dt=2026-10-07/execution_id=run-1"
OLD = f"raw/{copy_raw.OLD}/orders/{RUN}"
NEW = f"raw/{copy_raw.NEW}/orders/{RUN}"
META = {"source_id": copy_raw.OLD, "execution_id": "run-1", "_ingestion_ts": "t"}


class CopyFakeS3(FakeS3):
    """FakeS3 plus the head, copy, and paginator calls the copy makes."""

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        item = self.objects[(Bucket, Key)]
        return {"Metadata": dict(item["Metadata"]), "ContentType": "application/json"}

    def copy_object(self, **kwargs: object) -> None:
        source = kwargs["CopySource"]
        assert isinstance(source, dict)
        assert kwargs["MetadataDirective"] == "REPLACE"
        body = self.objects[(source["Bucket"], source["Key"])]["Body"]
        self.put_object(
            Bucket=kwargs["Bucket"],
            Key=kwargs["Key"],
            Body=body,
            Metadata=kwargs["Metadata"],
        )

    def get_paginator(self, name: str) -> CopyFakeS3:
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, object]]:
        return [self.list_objects_v2(Bucket=Bucket, Prefix=Prefix)]


def _store(marker: object) -> CopyFakeS3:
    store = CopyFakeS3()
    store.put_object(
        Bucket=BUCKET, Key=f"{OLD}/orders_a_b.json", Body=b"[1]", Metadata=META
    )
    store.put_object(
        Bucket=BUCKET,
        Key=f"{OLD}/_COMPLETE",
        Body=json.dumps(marker).encode(),
        Metadata={"source_id": copy_raw.OLD},
    )
    return store


def test_copy_rewrites_only_the_source_id() -> None:
    marker = {"source_id": copy_raw.OLD, "dataset": "orders", "object_names": ["x"]}
    store = _store(marker)
    counts = copy_raw.copy_all(store, BUCKET, dry_run=False)
    assert counts == {"raw\torders\tobject": 1, "raw\torders\tmarker": 1}
    data = store.objects[(BUCKET, f"{NEW}/orders_a_b.json")]
    assert data["Body"] == b"[1]"
    assert data["Metadata"] == {**META, "source_id": copy_raw.NEW}
    copied = json.loads(store.objects[(BUCKET, f"{NEW}/_COMPLETE")]["Body"])
    assert copied == {**marker, "source_id": copy_raw.NEW}
    # The old objects stay until the delete step.
    assert (BUCKET, f"{OLD}/orders_a_b.json") in store.objects


def test_dry_run_writes_nothing() -> None:
    store = _store({"source_id": copy_raw.OLD})
    copy_raw.copy_all(store, BUCKET, dry_run=True)
    assert not any(key.startswith(f"raw/{copy_raw.NEW}/") for _, key in store.objects)


@pytest.mark.parametrize("marker", [["not", "an", "object"], {"source_id": "other"}])
def test_a_marker_that_does_not_name_the_old_source_stops_the_copy(marker) -> None:
    with pytest.raises(SystemExit, match="marker is not an object"):
        copy_raw.copy_all(_store(marker), BUCKET, dry_run=False)
