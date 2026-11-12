"""Create the Airflow task-log bucket when it is missing.

The bucket name and MinIO credentials come from the environment. A failure
names the field and the S3 error code. It does not include the secret.
"""

from __future__ import annotations

import os
import sys

import boto3
from botocore.exceptions import ClientError

_MISSING = {"404", "NoSuchBucket", "404 Not Found"}


def main() -> None:
    bucket = os.environ["ODP_AIRFLOW_LOG_BUCKET"]
    client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name=os.environ.get("MINIO_REGION", "us-east-1"),
    )
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in _MISSING:
            print(
                f"ODP_AIRFLOW_LOG_BUCKET check failed ({code})",
                file=sys.stderr,
            )
            raise SystemExit(1) from None
        client.create_bucket(Bucket=bucket)


if __name__ == "__main__":
    main()
