"""In-memory S3 client for lakehouse unit tests (no MinIO)."""

from __future__ import annotations

import hashlib
import io
from pathlib import Path

from botocore.exceptions import ClientError


class FakeS3:
    """In-memory S3 client covering the calls Bronze and the raw writer make."""

    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[tuple[str, str], dict[str, object]] = {}
        self.reads: list[str] = []
        self.uploads: list[str] = []

    def head_bucket(self, *, Bucket: str) -> dict[str, object]:
        if Bucket not in self.buckets:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadBucket",
            )
        return {}

    def create_bucket(self, *, Bucket: str) -> None:
        self.buckets.add(Bucket)

    def put_object(self, **kwargs: object) -> None:
        bucket = str(kwargs["Bucket"])
        key = str(kwargs["Key"])
        body = kwargs["Body"]
        if not isinstance(body, bytes):
            msg = "body must be bytes"
            raise AssertionError(msg)
        self.buckets.add(bucket)
        metadata = kwargs.get("Metadata") or {}
        self.objects[(bucket, key)] = {
            "Body": body,
            "Metadata": dict(metadata),  # type: ignore[arg-type]
        }

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.objects.pop((Bucket, Key), None)

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:
        self.uploads.append(Key)
        self.put_object(Bucket=Bucket, Key=Key, Body=Path(Filename).read_bytes())

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        if (Bucket, Key) not in self.objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
                "GetObject",
            )
        item = self.objects[(Bucket, Key)]
        body = item["Body"]
        assert isinstance(body, bytes)
        self.reads.append(Key)
        return {
            "Body": io.BytesIO(body),
            "Metadata": item["Metadata"],
            "ETag": _etag(body),
        }

    def list_objects_v2(
        self,
        *,
        Bucket: str,
        Prefix: str,
        ContinuationToken: str | None = None,
    ) -> dict[str, object]:
        del ContinuationToken
        keys = sorted(
            key
            for bucket, key in self.objects
            if bucket == Bucket and key.startswith(Prefix)
        )
        contents = []
        for key in keys:
            body = self.objects[(Bucket, key)]["Body"]
            assert isinstance(body, bytes)
            contents.append({"Key": key, "ETag": _etag(body)})
        return {"Contents": contents, "IsTruncated": False}


def _etag(body: bytes) -> str:
    return '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"'
