"""Unit tests for object-storage helpers (no live MinIO required)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from open_data_platform.object_storage import ensure_bucket, put_and_get_object


class _FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


def _client_error() -> ClientError:
    return ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadBucket",
    )


def test_put_and_get_object_round_trip() -> None:
    client = MagicMock()
    client.head_bucket.side_effect = _client_error()
    stored: dict[str, bytes] = {}

    def put_object(*, Bucket: str, Key: str, Body: bytes, **_: Any) -> None:
        stored[f"{Bucket}/{Key}"] = Body

    def get_object(*, Bucket: str, Key: str, **_: Any) -> dict[str, Any]:
        return {"Body": _FakeBody(stored[f"{Bucket}/{Key}"])}

    client.put_object.side_effect = put_object
    client.get_object.side_effect = get_object

    payload = b"phase-0-smoke"
    result = put_and_get_object(
        client,
        bucket="odp-smoke",
        key="phase0/hello.txt",
        body=payload,
    )

    assert result == payload
    client.create_bucket.assert_called_once_with(Bucket="odp-smoke")
    client.put_object.assert_called_once()
    client.get_object.assert_called_once()


def test_ensure_bucket_skips_create_when_present() -> None:
    client = MagicMock()
    client.head_bucket.return_value = {}

    ensure_bucket(client, "existing")

    client.create_bucket.assert_not_called()


def test_ensure_bucket_creates_when_missing() -> None:
    client = MagicMock()
    client.head_bucket.side_effect = _client_error()

    ensure_bucket(client, "new-bucket")

    client.create_bucket.assert_called_once_with(Bucket="new-bucket")


@pytest.mark.parametrize(
    ("bucket", "key"),
    [
        ("odp-smoke", "a/b.txt"),
        ("other", "x"),
    ],
)
def test_put_and_get_uses_bucket_and_key(bucket: str, key: str) -> None:
    client = MagicMock()
    client.head_bucket.return_value = {}
    client.get_object.return_value = {"Body": _FakeBody(b"ok")}

    put_and_get_object(client, bucket=bucket, key=key, body=b"ok")

    client.put_object.assert_called_once_with(Bucket=bucket, Key=key, Body=b"ok")
    client.get_object.assert_called_once_with(Bucket=bucket, Key=key)
