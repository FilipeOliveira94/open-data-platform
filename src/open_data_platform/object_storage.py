"""Minimal S3-compatible object storage helpers (local MinIO / AWS S3)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from collections.abc import Mapping


def create_s3_client(
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region_name: str = "us-east-1",
) -> BaseClient:
    """Create a boto3 S3 client pointed at an S3-compatible endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name,
    )


def ensure_bucket(client: BaseClient, bucket: str) -> None:
    """Create ``bucket`` if it does not already exist."""
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)


def put_and_get_object(
    client: BaseClient,
    *,
    bucket: str,
    key: str,
    body: bytes,
) -> bytes:
    """Write ``body`` to ``bucket/key`` and return the bytes read back."""
    ensure_bucket(client, bucket)
    client.put_object(Bucket=bucket, Key=key, Body=body)
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def client_from_env(env: Mapping[str, str]) -> BaseClient:
    """Build an S3 client from environment-style settings."""
    endpoint = env["MINIO_ENDPOINT_URL"]
    access_key = env["MINIO_ROOT_USER"]
    secret_key = env["MINIO_ROOT_PASSWORD"]
    region = env.get("MINIO_REGION", "us-east-1")
    return create_s3_client(
        endpoint_url=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region_name=region,
    )
