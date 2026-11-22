"""Isolation for integration tests that write to MinIO.

Tests write to a test bucket, never ``ODP_RAW_BUCKET``: a raw execution left
behind there is read by the next lakehouse refresh. ``purge_executions``
deletes every raw, marker, and quarantine object of the registered execution
ids at teardown, also when the test fails.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from botocore.exceptions import EndpointConnectionError

TEST_RAW_BUCKET_DEFAULT = "odp-test-raw"
_PREFIXES = ("raw/", "quarantine/")


@pytest.fixture
def test_raw_bucket() -> str:
    """The bucket integration tests write to; it must not be the raw bucket."""
    bucket = os.environ.get("ODP_TEST_RAW_BUCKET", "").strip() or (
        TEST_RAW_BUCKET_DEFAULT
    )
    if bucket == os.environ.get("ODP_RAW_BUCKET", "odp-raw"):
        pytest.fail("ODP_TEST_RAW_BUCKET must differ from ODP_RAW_BUCKET")
    return bucket


@pytest.fixture
def purge_executions() -> Iterator[Callable[[Any, str, str], None]]:
    """Register ``(client, bucket, execution_id)``; its objects go at teardown."""
    registered: list[tuple[Any, str, str]] = []

    def register(client: Any, bucket: str, execution_id: str) -> None:
        registered.append((client, bucket, execution_id))

    yield register
    for client, bucket, execution_id in registered:
        try:
            delete_execution(client, bucket, execution_id)
        except EndpointConnectionError:
            # MinIO was never reachable: the test skipped and wrote nothing.
            return


def delete_execution(client: Any, bucket: str, execution_id: str) -> int:
    """Delete every object under an ``execution_id=`` segment. Return the count."""
    segment = f"/execution_id={execution_id}/"
    deleted = 0
    paginator = client.get_paginator("list_objects_v2")
    for prefix in _PREFIXES:
        try:
            pages = list(paginator.paginate(Bucket=bucket, Prefix=prefix))
        except client.exceptions.NoSuchBucket:
            return deleted
        for page in pages:
            for item in page.get("Contents", []):
                if segment in item["Key"]:
                    client.delete_object(Bucket=bucket, Key=item["Key"])
                    deleted += 1
    return deleted
