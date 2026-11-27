"""A BioGuardia drop written to MinIO reads back complete and matching its manifest.

Writes to ``ODP_TEST_LANDING_BUCKET`` (never the landing bucket the platform
reads) at a small scale.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date

import pytest

from open_data_platform.object_storage import client_from_env
from open_data_platform.simulators.bioguardia import OPS_SOURCE
from open_data_platform.simulators.bioguardia.drop import (
    MANIFEST,
    SUCCESS_MARKER,
    BucketTarget,
    drop_prefix,
    generate_drop,
)
from open_data_platform.simulators.bioguardia.world import WorldConfig, build_world

pytestmark = pytest.mark.integration

DAY = date(2026, 12, 3)


@pytest.fixture
def client():
    if not os.environ.get("MINIO_ENDPOINT_URL"):
        pytest.skip("MINIO_ENDPOINT_URL is not set")
    return client_from_env(os.environ)


def test_a_drop_lands_in_minio_with_a_matching_manifest(client) -> None:
    bucket = os.environ.get("ODP_TEST_LANDING_BUCKET", "odp-test-landing")
    world = build_world(WorldConfig(seed=17, scale=0.002))
    landing = BucketTarget(client, bucket)
    evaluation = BucketTarget(client, f"{bucket}-evaluation")
    summary = generate_drop(world, OPS_SOURCE, DAY, 1, landing, evaluation)
    prefix = drop_prefix(OPS_SOURCE, DAY, 1)
    manifest = json.loads(
        client.get_object(Bucket=bucket, Key=f"{prefix}/{MANIFEST}")["Body"].read()
    )
    marker = json.loads(
        client.get_object(Bucket=bucket, Key=f"{prefix}/{SUCCESS_MARKER}")[
            "Body"
        ].read()
    )
    assert marker["table_count"] == manifest["table_count"] == len(summary.tables)
    planted = {d["table"] for d in summary.file_defects}
    for entry in manifest["tables"]:
        if entry["table"] in planted:
            continue
        body = client.get_object(Bucket=bucket, Key=f"{prefix}/{entry['file']}")[
            "Body"
        ].read()
        assert len(body) == entry["bytes"]
        assert hashlib.sha256(body).hexdigest() == entry["sha256"]
    listed = client.list_objects_v2(Bucket=f"{bucket}-evaluation", Prefix=prefix)
    assert any(
        item["Key"].endswith("planted.json") for item in listed.get("Contents", [])
    )
