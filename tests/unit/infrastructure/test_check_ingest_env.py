"""Runtime checks for the Airflow ingestion environment. No Compose required."""

from __future__ import annotations

import importlib.util
from collections.abc import Mapping
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "docker" / "airflow" / "check_ingest_env.py"
PLANTED_SECRET = "planted-secret-value"


def _script():
    spec = importlib.util.spec_from_file_location("check_ingest_env", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _env(**overrides: str) -> dict[str, str]:
    settings = {
        "MINIO_ENDPOINT_URL": "http://minio:9000",
        "MINIO_ROOT_USER": "minioadmin",
        "MINIO_ROOT_PASSWORD": PLANTED_SECRET,
        "MINIO_REGION": "us-east-1",
        "ODP_RAW_BUCKET": "odp-raw",
        "MINIO_SMOKE_BUCKET": "odp-smoke",
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": "odp",
        "POSTGRES_PASSWORD": PLANTED_SECRET,
        "POSTGRES_DB": "odp",
        "AIRFLOW_DB": "airflow",
        "ICEBERG_CATALOG_DB": "iceberg",
    }
    settings.update(overrides)
    return settings


def _error(code: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": PLANTED_SECRET}},
        "HeadBucket",
    )


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class _S3:
    def __init__(
        self,
        *,
        buckets: set[str] | None = None,
        head_code: str | None = None,
        put_code: str | None = None,
        delete_code: str | None = None,
        read_body: bytes | None = None,
    ) -> None:
        self.buckets = set() if buckets is None else set(buckets)
        self.head_code = head_code
        self.put_code = put_code
        self.delete_code = delete_code
        self.read_body = read_body
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created: list[str] = []
        self.deleted: list[tuple[str, str]] = []

    def head_bucket(self, *, Bucket: str) -> None:
        if self.head_code is not None:
            raise _error(self.head_code)
        if Bucket not in self.buckets:
            raise _error("404")

    def create_bucket(self, *, Bucket: str) -> None:
        self.created.append(Bucket)
        self.buckets.add(Bucket)

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        if self.put_code is not None:
            raise _error(self.put_code)
        self.objects[(Bucket, Key)] = Body

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, _Body]:
        if self.read_body is not None:
            payload = self.read_body
        else:
            payload = self.objects[(Bucket, Key)]
        return {"Body": _Body(payload)}

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        if self.delete_code is not None:
            raise _error(self.delete_code)
        self.deleted.append((Bucket, Key))
        self.objects.pop((Bucket, Key), None)


class _Cursor:
    def __init__(self, database: str, schema_present: bool) -> None:
        self.database = database
        self.schema_present = schema_present
        self.statements: list[tuple[str, tuple[object, ...] | None]] = []
        self._row: tuple[object, ...] | None = None

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(
        self,
        statement: str,
        params: tuple[object, ...] | None = None,
    ) -> None:
        self.statements.append((statement, params))
        module = _script()
        if statement == module._DATABASE_SQL:
            self._row = (self.database,)
        elif statement == module._SCHEMA_SQL:
            self._row = ("oltp",) if self.schema_present else None
        else:
            raise AssertionError(statement)

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row


class _Connection:
    def __init__(self, database: str, schema_present: bool = True) -> None:
        self.cursor_obj = _Cursor(database, schema_present)
        self.closed = False

    def cursor(self) -> _Cursor:
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


def _assert_secret_hidden(text: str) -> None:
    assert PLANTED_SECRET not in text


def test_imports_and_contracts(capsys: pytest.CaptureFixture[str]) -> None:
    module = _script()
    assert module.main(["imports"]) == 0
    assert module.main(["contracts"]) == 0
    captured = capsys.readouterr()
    assert "imports ok: boto3, psycopg, yaml, open_data_platform" in captured.out
    assert "contracts_dir=" in captured.out
    assert "contracts_yaml=0" not in captured.out
    _assert_secret_hidden(captured.out)
    _assert_secret_hidden(captured.err)


def test_missing_step_is_rejected() -> None:
    module = _script()
    with pytest.raises(SystemExit) as raised:
        module.main([])
    assert raised.value.code == 2


def test_raw_bucket_head_does_not_create_it(
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _script()
    client = _S3(buckets={"odp-raw"})
    module.check_minio_raw(_env(), client=client)
    assert client.created == []
    captured = capsys.readouterr()
    assert "raw_bucket=odp-raw" in captured.out
    _assert_secret_hidden(captured.out)


def test_raw_bucket_error_hides_the_secret() -> None:
    module = _script()
    client = _S3(head_code="AccessDenied")
    with pytest.raises(module.EnvCheckError, match="AccessDenied") as raised:
        module.check_minio_raw(_env(), client=client)
    _assert_secret_hidden(str(raised.value))
    assert client.created == []


def test_wrong_minio_endpoint_is_rejected_before_the_client() -> None:
    module = _script()
    client = _S3(buckets={"odp-raw"})
    with pytest.raises(module.EnvCheckError, match="MINIO_ENDPOINT_URL"):
        module.check_minio_raw(
            _env(MINIO_ENDPOINT_URL="http://localhost:9000"),
            client=client,
        )
    assert client.created == []


def test_smoke_write_puts_and_deletes(capsys: pytest.CaptureFixture[str]) -> None:
    module = _script()
    client = _S3(buckets={"odp-smoke"})
    module.check_minio_write(_env(), client=client)
    assert client.deleted == [("odp-smoke", "airflow-env/ready.txt")]
    assert client.objects == {}
    captured = capsys.readouterr()
    assert "smoke_bucket=odp-smoke" in captured.out
    assert "smoke_key=airflow-env/ready.txt" in captured.out
    assert b"platform_env ready".decode() not in captured.out
    _assert_secret_hidden(captured.out)


def test_smoke_write_mismatch_hides_the_body() -> None:
    module = _script()
    client = _S3(buckets={"odp-smoke"}, read_body=PLANTED_SECRET.encode())
    with pytest.raises(module.EnvCheckError, match="body mismatch") as raised:
        module.check_minio_write(_env(), client=client)
    _assert_secret_hidden(str(raised.value))
    assert client.deleted == []


def test_smoke_delete_error_hides_the_secret() -> None:
    module = _script()
    client = _S3(buckets={"odp-smoke"}, delete_code="AccessDenied")
    with pytest.raises(module.EnvCheckError, match="delete failed") as raised:
        module.check_minio_write(_env(), client=client)
    _assert_secret_hidden(str(raised.value))


def test_postgres_oltp_reads_the_oltp_database(
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _script()
    connection = _Connection("odp")
    module.check_postgres_oltp(_env(), connect=lambda _env: connection)
    assert connection.closed is True
    statements = connection.cursor_obj.statements
    assert statements[0][0] == module._DATABASE_SQL
    assert statements[1] == (module._SCHEMA_SQL, ("oltp",))
    rendered = " ".join(statement for statement, _params in statements)
    _assert_secret_hidden(rendered)
    captured = capsys.readouterr()
    assert "postgres_host=postgres" in captured.out
    assert "database=odp" in captured.out
    assert "oltp_schema=present" in captured.out
    _assert_secret_hidden(captured.out)


def test_postgres_host_must_be_the_compose_service() -> None:
    module = _script()
    called = False

    def connect(_env: Mapping[str, str]) -> _Connection:
        nonlocal called
        called = True
        return _Connection("odp")

    with pytest.raises(module.EnvCheckError, match="POSTGRES_HOST"):
        module.check_postgres_oltp(_env(POSTGRES_HOST="localhost"), connect=connect)
    assert called is False


def test_published_port_is_rejected() -> None:
    module = _script()
    with pytest.raises(module.EnvCheckError, match="POSTGRES_PORT"):
        module.check_postgres_oltp(
            _env(POSTGRES_PORT="5433"),
            connect=lambda _env: _Connection("odp"),
        )


def test_metadata_and_catalog_databases_are_rejected() -> None:
    module = _script()
    for database in ("airflow", "iceberg"):
        called = False

        def connect(
            _env: Mapping[str, str],
            database: str = database,
        ) -> _Connection:
            nonlocal called
            called = True
            return _Connection(database)

        with pytest.raises(module.EnvCheckError, match="OLTP database"):
            module.check_postgres_oltp(_env(POSTGRES_DB=database), connect=connect)
        assert called is False


def test_current_database_must_match_postgres_db() -> None:
    module = _script()
    connection = _Connection("airflow")
    with pytest.raises(module.EnvCheckError, match="current_database") as raised:
        module.check_postgres_oltp(_env(), connect=lambda _env: connection)
    assert connection.closed is True
    _assert_secret_hidden(str(raised.value))
    assert "airflow" not in str(raised.value)


def test_missing_oltp_schema_fails_without_seeding() -> None:
    module = _script()
    connection = _Connection("odp", schema_present=False)
    with pytest.raises(module.EnvCheckError, match="oltp schema is absent"):
        module.check_postgres_oltp(_env(), connect=lambda _env: connection)
    assert connection.closed is True


def test_main_postgres_failure_hides_the_secret(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _script()
    for name, value in _env().items():
        monkeypatch.setenv(name, value)

    def connect(_env: Mapping[str, str]) -> _Connection:
        assert _env["POSTGRES_PASSWORD"] == PLANTED_SECRET
        raise module.PostgresOltpError("PostgreSQL connection failed")

    monkeypatch.setattr(module, "connect_from_env", connect)
    assert module.main(["postgres_oltp"]) == 1
    captured = capsys.readouterr()
    assert "PostgreSQL connection failed" in captured.err
    _assert_secret_hidden(captured.out)
    _assert_secret_hidden(captured.err)
