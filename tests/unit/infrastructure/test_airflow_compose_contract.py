"""Contract checks for local Airflow (P3-01). No Compose services required."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import yaml
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"
MAKEFILE = ROOT / "Makefile"
DAG = ROOT / "airflow" / "dags" / "platform_health.py"
BUCKET_SCRIPT = ROOT / "docker" / "airflow" / "ensure_log_bucket.py"

AIRFLOW_IMAGE = "apache/airflow:3.4.0-python3.12"
FERNET_PLACEHOLDER = "m2MF2P1kdm3qHPyXKJ7j0N5IGxBOswtdV9umW9-n6FA="
PLANTED_SECRET = "planted-secret-value"


def _compose_text() -> str:
    return COMPOSE.read_text(encoding="utf-8")


def _services() -> dict[str, object]:
    loaded = yaml.safe_load(_compose_text())
    services = loaded["services"]
    assert isinstance(services, dict)
    return services


def _bucket_script():
    spec = importlib.util.spec_from_file_location("ensure_log_bucket", BUCKET_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_default_compose_is_postgres_and_minio() -> None:
    services = _services()
    default = {name for name, spec in services.items() if "profiles" not in spec}
    assert default == {"postgres", "minio"}
    assert services["spark"]["profiles"] == ["lakehouse"]
    assert services["trino"]["profiles"] == ["lakehouse"]


def test_airflow_profile_pins_local_executor_and_minio_logs() -> None:
    text = _compose_text()
    services = _services()
    assert text.count(f"image: {AIRFLOW_IMAGE}") == 1
    for name in (
        "airflow-init",
        "airflow-apiserver",
        "airflow-scheduler",
        "airflow-dag-processor",
    ):
        assert services[name]["image"] == AIRFLOW_IMAGE
        assert services[name]["profiles"] == ["orchestration"]
        executor = services[name]["environment"]["AIRFLOW__CORE__EXECUTOR"]
        assert executor == "LocalExecutor"
    assert services["airflow-db-create"]["profiles"] == ["orchestration"]
    assert services["airflow-db-create"]["image"] == "postgres:16.15-bookworm"
    assert "CeleryExecutor" not in text
    assert "redis" not in services
    assert "flower" not in services
    commands = " ".join(str(spec.get("command", "")) for spec in services.values())
    assert "celery" not in commands
    assert "triggerer" not in commands
    scheduler = services["airflow-scheduler"]["environment"]
    assert scheduler["AIRFLOW__CORE__LOAD_EXAMPLES"] == "false"
    assert scheduler["AIRFLOW__CORE__DAG_RUN_CONF_OVERRIDES_PARAMS"] == "True"
    assert scheduler["PYTHONPATH"] == "/opt/odp/repo/src"
    assert scheduler["MINIO_ENDPOINT_URL"] == "http://minio:9000"
    assert scheduler["POSTGRES_HOST"] == "postgres"
    assert scheduler["POSTGRES_PORT"] == "5432"
    assert scheduler["POSTGRES_DB"] == "${POSTGRES_DB:-odp}"
    assert scheduler["ODP_RAW_BUCKET"] == "${ODP_RAW_BUCKET:-odp-raw}"
    assert scheduler["MINIO_SMOKE_BUCKET"] == "${MINIO_SMOKE_BUCKET:-odp-smoke}"
    assert scheduler["AIRFLOW_DB"] == "${AIRFLOW_DB:-airflow}"
    assert scheduler["ICEBERG_CATALOG_DB"] == "${ICEBERG_CATALOG_DB:-iceberg}"
    assert scheduler["KAGGLE_API_TOKEN"] == "${KAGGLE_API_TOKEN:-}"
    assert scheduler["KAGGLE_USERNAME"] == "${KAGGLE_USERNAME:-}"
    assert scheduler["KAGGLE_KEY"] == "${KAGGLE_KEY:-}"
    volumes = services["airflow-scheduler"]["volumes"]
    assert "./src:/opt/odp/repo/src:ro" in volumes
    assert "./config:/opt/odp/repo/config:ro" in volumes
    assert "./pyproject.toml:/opt/odp/repo/pyproject.toml:ro" in volumes
    assert "./tests/fixtures:/opt/odp/repo/tests/fixtures:ro" in volumes
    assert "./data/raw:/opt/odp/repo/data/raw:ro" in volumes
    assert (
        "./docker/airflow/check_ingest_env.py:/opt/odp/check_ingest_env.py:ro"
        in volumes
    )
    assert "./:/opt/odp/repo" not in " ".join(str(item) for item in volumes)
    assert scheduler["AIRFLOW__LOGGING__REMOTE_LOGGING"] == "True"
    assert (
        scheduler["AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER"]
        == "s3://${ODP_AIRFLOW_LOG_BUCKET:-odp-airflow-logs}/task-logs"
    )
    assert scheduler["AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID"] == "minio_logs"
    assert "${MINIO_ROOT_PASSWORD:-minioadmin}" in scheduler["AIRFLOW_CONN_MINIO_LOGS"]
    assert "http://minio:9000" in scheduler["AIRFLOW_CONN_MINIO_LOGS"]
    assert "addressing_style" in scheduler["AIRFLOW_CONN_MINIO_LOGS"]
    assert "${AIRFLOW_PORT:-8081}:8080" in text
    assert (
        "postgresql+psycopg://${POSTGRES_USER:-odp}:${POSTGRES_PASSWORD:-odp}" in text
    )
    assert FERNET_PLACEHOLDER in text
    assert "open_data_platform" not in text


def test_env_example_matches_the_local_placeholders() -> None:
    env = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert f"AIRFLOW_FERNET_KEY={FERNET_PLACEHOLDER}" in env
    assert "AIRFLOW_PORT=8081" in env
    assert "AIRFLOW_DB=airflow" in env
    assert "ODP_AIRFLOW_LOG_BUCKET=odp-airflow-logs" in env
    assert "AIRFLOW_ADMIN_USER=airflow" in env
    assert "AIRFLOW_ADMIN_PASSWORD=odp" in env
    assert "AIRFLOW_JWT_SECRET=local-dev-jwt" in env
    makefile = MAKEFILE.read_text(encoding="utf-8")
    assert "airflow-up:" in makefile
    assert "ODP_AIRFLOW_LOG_BUCKET" in makefile
    assert "--profile orchestration up -d --wait" in makefile


def test_health_dag_has_no_schedule_and_no_package_logic() -> None:
    text = DAG.read_text(encoding="utf-8")
    assert 'dag_id="platform_health"' in text
    assert "schedule=None" in text
    assert 'bash_command="echo platform_health ready"' in text
    assert "open_data_platform" not in text
    assert "SELECT" not in text.upper()
    assert "@daily" not in text
    assert "@weekly" not in text


def test_log_bucket_script_does_not_print_the_secret() -> None:
    text = BUCKET_SCRIPT.read_text(encoding="utf-8")
    assert "MINIO_ROOT_PASSWORD" in text
    assert "print(" in text
    assert "MINIO_ROOT_PASSWORD" not in text.split("print(", 1)[1]


class _Bucket:
    def __init__(self, missing: bool, code: str = "404") -> None:
        self.missing = missing
        self.code = code
        self.created: list[str] = []

    def head_bucket(self, *, Bucket: str) -> None:
        if self.missing:
            raise ClientError({"Error": {"Code": self.code}}, "HeadBucket")

    def create_bucket(self, *, Bucket: str) -> None:
        self.created.append(Bucket)


def _patch_bucket(monkeypatch: pytest.MonkeyPatch, bucket: _Bucket) -> object:
    module = _bucket_script()
    monkeypatch.setenv("ODP_AIRFLOW_LOG_BUCKET", "odp-airflow-logs")
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", PLANTED_SECRET)
    monkeypatch.setenv("MINIO_REGION", "us-east-1")
    monkeypatch.setattr(module.boto3, "client", lambda *_args, **_kwargs: bucket)
    return module


def test_missing_log_bucket_is_created(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    bucket = _Bucket(missing=True)
    module = _patch_bucket(monkeypatch, bucket)
    module.main()
    assert bucket.created == ["odp-airflow-logs"]
    captured = capsys.readouterr()
    assert PLANTED_SECRET not in captured.out
    assert PLANTED_SECRET not in captured.err


def test_existing_log_bucket_is_left_in_place(monkeypatch: pytest.MonkeyPatch) -> None:
    bucket = _Bucket(missing=False)
    module = _patch_bucket(monkeypatch, bucket)
    module.main()
    assert bucket.created == []


def test_unexpected_bucket_error_hides_the_secret(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    bucket = _Bucket(missing=True, code="AccessDenied")
    module = _patch_bucket(monkeypatch, bucket)
    with pytest.raises(SystemExit) as raised:
        module.main()
    assert raised.value.code == 1
    captured = capsys.readouterr()
    assert "ODP_AIRFLOW_LOG_BUCKET check failed (AccessDenied)" in captured.err
    assert PLANTED_SECRET not in captured.err
    assert bucket.created == []
