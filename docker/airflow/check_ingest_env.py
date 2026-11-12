"""Check the runtime the ingestion DAGs use.

Each step prints names and counts. Failure text names the setting or the
S3 error code. It does not include secrets, row values, or the object body.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable, Mapping
from typing import Protocol

from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from open_data_platform.contracts.load import default_contracts_dir, load_contracts
from open_data_platform.contracts.model import ContractError
from open_data_platform.ingestion.postgres_oltp import (
    PostgresOltpError,
    connect_from_env,
)
from open_data_platform.object_storage import client_from_env, put_and_get_object

_COMPOSE_POSTGRES_HOST = "postgres"
_COMPOSE_POSTGRES_PORT = "5432"
_COMPOSE_MINIO_ENDPOINT = "http://minio:9000"
_SMOKE_KEY = "airflow-env/ready.txt"
_SMOKE_BODY = b"platform_env ready"
_DATABASE_SQL = "SELECT current_database()"
_SCHEMA_SQL = (
    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s"
)
_OLTP_SCHEMA = "oltp"


class EnvCheckError(Exception):
    """A named runtime check failed."""


class _Cursor(Protocol):
    def execute(
        self,
        statement: str,
        params: tuple[object, ...] | None = None,
    ) -> object: ...

    def fetchone(self) -> tuple[object, ...] | None: ...

    def __enter__(self) -> _Cursor: ...

    def __exit__(self, *args: object) -> None: ...


class _Connection(Protocol):
    def cursor(self) -> _Cursor: ...

    def close(self) -> None: ...


def check_imports() -> None:
    """Import the libraries the ingestion CLIs need."""
    import boto3
    import psycopg
    import yaml

    import open_data_platform

    names = ", ".join(
        module.__name__ for module in (boto3, psycopg, yaml, open_data_platform)
    )
    print(f"imports ok: {names}")


def check_contracts() -> None:
    """Load the repository contracts from the mounted tree."""
    try:
        root = default_contracts_dir()
        loaded = load_contracts(root)
    except ContractError as exc:
        raise EnvCheckError(str(exc)) from None
    print(f"contracts_dir={root}")
    print(f"contracts_yaml={len(loaded)}")


def check_minio_raw(
    env: Mapping[str, str] | None = None,
    *,
    client: BaseClient | None = None,
) -> None:
    """Head the raw bucket. Do not create it."""
    settings = os.environ if env is None else env
    _require_minio_endpoint(settings)
    bucket = _require_setting(settings, "ODP_RAW_BUCKET")
    s3 = _minio_client(settings, client)
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        raise _client_error("ODP_RAW_BUCKET check failed", exc) from None
    except BotoCoreError:
        raise EnvCheckError("ODP_RAW_BUCKET check failed") from None
    print(f"raw_bucket={bucket}")


def check_minio_write(
    env: Mapping[str, str] | None = None,
    *,
    client: BaseClient | None = None,
) -> None:
    """Put, read, and delete one object in the smoke bucket."""
    settings = os.environ if env is None else env
    _require_minio_endpoint(settings)
    bucket = _require_setting(settings, "MINIO_SMOKE_BUCKET")
    s3 = _minio_client(settings, client)
    try:
        body = put_and_get_object(
            s3,
            bucket=bucket,
            key=_SMOKE_KEY,
            body=_SMOKE_BODY,
        )
    except ClientError as exc:
        raise _client_error("MINIO_SMOKE_BUCKET write failed", exc) from None
    except BotoCoreError:
        raise EnvCheckError("MINIO_SMOKE_BUCKET write failed") from None
    if body != _SMOKE_BODY:
        msg = "MINIO_SMOKE_BUCKET object body mismatch"
        raise EnvCheckError(msg)
    try:
        s3.delete_object(Bucket=bucket, Key=_SMOKE_KEY)
    except ClientError as exc:
        raise _client_error("MINIO_SMOKE_BUCKET delete failed", exc) from None
    except BotoCoreError:
        raise EnvCheckError("MINIO_SMOKE_BUCKET delete failed") from None
    print(f"smoke_bucket={bucket}")
    print(f"smoke_key={_SMOKE_KEY}")


def check_postgres_oltp(
    env: Mapping[str, str] | None = None,
    *,
    connect: Callable[[Mapping[str, str]], _Connection] | None = None,
) -> None:
    """Open the OLTP database and require the ``oltp`` schema."""
    settings = os.environ if env is None else env
    host = settings.get("POSTGRES_HOST", "").strip()
    if host != _COMPOSE_POSTGRES_HOST:
        msg = "POSTGRES_HOST must be the Compose service postgres"
        raise EnvCheckError(msg)
    port = settings.get("POSTGRES_PORT", "").strip()
    if port != _COMPOSE_POSTGRES_PORT:
        msg = "POSTGRES_PORT must be 5432 inside Compose"
        raise EnvCheckError(msg)
    database = _require_setting(settings, "POSTGRES_DB")
    forbidden = _forbidden_databases(settings)
    if database in forbidden:
        msg = "POSTGRES_DB must be the OLTP database"
        raise EnvCheckError(msg)
    opener = connect_from_env if connect is None else connect
    try:
        connection = opener(settings)
    except PostgresOltpError as exc:
        raise EnvCheckError(str(exc)) from None
    try:
        current, schema_present = _read_database(connection)
    finally:
        connection.close()
    if current != database or current in forbidden:
        msg = "current_database does not match the OLTP database"
        raise EnvCheckError(msg)
    if not schema_present:
        msg = "oltp schema is absent"
        raise EnvCheckError(msg)
    print(f"postgres_host={host}")
    print(f"database={current}")
    print("oltp_schema=present")


def main(argv: list[str] | None = None) -> int:
    """Run one named check."""
    parser = argparse.ArgumentParser(
        description="Check the Airflow ingestion runtime.",
    )
    parser.add_argument(
        "step",
        choices=(
            "imports",
            "contracts",
            "minio_raw",
            "minio_write",
            "postgres_oltp",
        ),
    )
    args = parser.parse_args(argv)
    steps = {
        "imports": check_imports,
        "contracts": check_contracts,
        "minio_raw": check_minio_raw,
        "minio_write": check_minio_write,
        "postgres_oltp": check_postgres_oltp,
    }
    try:
        steps[args.step]()
    except EnvCheckError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _require_setting(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        msg = f"{name} is required"
        raise EnvCheckError(msg)
    return value


def _require_minio_endpoint(env: Mapping[str, str]) -> None:
    endpoint = env.get("MINIO_ENDPOINT_URL", "").strip()
    if endpoint != _COMPOSE_MINIO_ENDPOINT:
        msg = "MINIO_ENDPOINT_URL must be http://minio:9000"
        raise EnvCheckError(msg)


def _forbidden_databases(env: Mapping[str, str]) -> frozenset[str]:
    missing = [
        name
        for name in ("AIRFLOW_DB", "ICEBERG_CATALOG_DB")
        if not env.get(name, "").strip()
    ]
    if missing:
        names = ", ".join(missing)
        msg = f"PostgreSQL settings are required: {names}"
        raise EnvCheckError(msg)
    return frozenset(
        {
            env["AIRFLOW_DB"].strip(),
            env["ICEBERG_CATALOG_DB"].strip(),
        }
    )


def _minio_client(env: Mapping[str, str], client: BaseClient | None) -> BaseClient:
    if client is not None:
        return client
    try:
        return client_from_env(env)
    except KeyError as exc:
        name = exc.args[0] if exc.args and isinstance(exc.args[0], str) else "setting"
        msg = f"MinIO setting is required: {name}"
        raise EnvCheckError(msg) from None


def _client_error(prefix: str, exc: ClientError) -> EnvCheckError:
    code = str(exc.response.get("Error", {}).get("Code", ""))
    if code:
        return EnvCheckError(f"{prefix} ({code})")
    return EnvCheckError(prefix)


def _read_database(connection: _Connection) -> tuple[str, bool]:
    with connection.cursor() as cursor:
        cursor.execute(_DATABASE_SQL)
        row = cursor.fetchone()
        cursor.execute(_SCHEMA_SQL, (_OLTP_SCHEMA,))
        schema = cursor.fetchone()
    current = ""
    if row is not None and row[0] is not None:
        current = str(row[0])
    return current, schema is not None


if __name__ == "__main__":
    raise SystemExit(main())
