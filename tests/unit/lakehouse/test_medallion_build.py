"""Unit tests for the medallion medallion_build. No Compose services."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from open_data_platform.lakehouse import medallion_build
from open_data_platform.lakehouse.medallion_build import MedallionBuildError


class _Recorder:
    def __init__(self, statuses: list[int] | None = None) -> None:
        self.calls: list[tuple[list[str], dict[str, str]]] = []
        self.statuses = list(statuses or [])

    def __call__(self, command, env) -> int:
        self.calls.append((list(command), dict(env)))
        return self.statuses.pop(0) if self.statuses else 0


def test_run_dir_replaces_unsafe_characters(tmp_path: Path) -> None:
    path = medallion_build.run_dir(tmp_path, "manual__2026-11-13T10:00:00+00:00")
    assert path == tmp_path / "manual__2026-11-13T10_00_00_00_00"
    assert medallion_build.run_dir(tmp_path, "../etc") == tmp_path / "etc"
    with pytest.raises(MedallionBuildError):
        medallion_build.run_dir(tmp_path, "..")


def test_catalog_db_must_be_a_simple_identifier() -> None:
    assert medallion_build.catalog_db({}) == "iceberg"
    assert medallion_build.catalog_db({"ICEBERG_CATALOG_DB": "cat_2"}) == "cat_2"
    with pytest.raises(MedallionBuildError):
        medallion_build.catalog_db({"ICEBERG_CATALOG_DB": "x; DROP DATABASE odp"})


def test_spark_scripts_run_once_per_file_in_name_order(tmp_path: Path) -> None:
    for name in ("b.sql", "a.sql", "notes.txt"):
        (tmp_path / name).write_text("SELECT 1;", encoding="utf-8")
    recorder = _Recorder()
    env = {"ODP_SPARK_DIR": "/opt/odp/spark", "MINIO_ROOT_PASSWORD": "secret"}
    count = medallion_build.run_spark_scripts(tmp_path, "run_bronze.sh", env, recorder)
    assert count == 2
    assert [call[0] for call in recorder.calls] == [
        ["bash", "/opt/odp/spark/run_bronze.sh"],
        ["bash", "/opt/odp/spark/run_bronze.sh"],
    ]
    assert [call[1]["ODP_SPARK_SQL"] for call in recorder.calls] == [
        str(tmp_path / "a.sql"),
        str(tmp_path / "b.sql"),
    ]
    # Secrets stay in the environment, never in the argument vector.
    assert all("secret" not in " ".join(call[0]) for call in recorder.calls)


def test_spark_failure_stops_at_the_failing_file(tmp_path: Path) -> None:
    for name in ("a.sql", "b.sql", "c.sql"):
        (tmp_path / name).write_text("SELECT 1;", encoding="utf-8")
    recorder = _Recorder([0, 3])
    with pytest.raises(MedallionBuildError, match="b.sql with status 3"):
        medallion_build.run_spark_scripts(tmp_path, "run_silver.sh", {}, recorder)
    assert len(recorder.calls) == 2


def test_empty_sql_directory_fails(tmp_path: Path) -> None:
    with pytest.raises(MedallionBuildError, match="no Spark SQL files"):
        medallion_build.run_spark_scripts(tmp_path, "run_bronze.sh", {}, _Recorder())


def test_checks_pass_and_fail_on_the_first_mismatch(
    capsys: pytest.CaptureFixture[str],
) -> None:
    answers = {"SELECT 1": "1", "SELECT 2": "ok", "SELECT 3": "7"}
    assert (
        medallion_build.run_checks([("1", "SELECT 1"), ("ok", "SELECT 2")], answers.get)
        == 2
    )
    with pytest.raises(MedallionBuildError, match="live check failed"):
        medallion_build.run_checks([("1", "SELECT 1"), ("8", "SELECT 3")], answers.get)
    err = capsys.readouterr().err
    assert "expected 8" in err
    assert "got 7" in err
    with pytest.raises(MedallionBuildError, match="no live checks"):
        medallion_build.run_checks([], answers.get)


def test_dbt_build_excludes_reconciliation_and_reconcile_selects_it(
    tmp_path: Path,
) -> None:
    env = {"ODP_DBT_BIN": "/opt/dbt-venv/bin/dbt", "ODP_REPO": "/opt/odp/repo"}
    build = medallion_build.dbt_command("build", tmp_path, env)
    assert build[:2] == ["/opt/dbt-venv/bin/dbt", "build"]
    assert build[-2:] == ["--exclude", "tag:reconciliation"]
    assert "--project-dir" in build and "/opt/odp/repo/dbt" in build
    assert str(tmp_path / "dbt-target") in build
    reconcile = medallion_build.dbt_command("reconcile", tmp_path, env)
    assert reconcile[1] == "test"
    assert reconcile[-2:] == ["--select", "tag:reconciliation"]
    assert medallion_build.dbt_command("parse", tmp_path, env)[1] == "parse"
    with pytest.raises(MedallionBuildError):
        medallion_build.dbt_command("run", tmp_path, env)


def test_dbt_step_failure_is_reported(tmp_path: Path) -> None:
    recorder = _Recorder([2])
    with pytest.raises(MedallionBuildError, match="dbt-build exited with status 2"):
        medallion_build.run_step("dbt-build", tmp_path / "w", {}, recorder)
    assert recorder.calls[0][0][1] == "build"


def test_bronze_checks_need_the_count_file(tmp_path: Path) -> None:
    with pytest.raises(MedallionBuildError, match="missing count file"):
        medallion_build.run_step("bronze-checks", tmp_path, {}, _Recorder())


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def test_trino_scalar_follows_next_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = [
        {"nextUri": "http://trino:8080/v1/statement/q/1"},
        {"data": [["5571"]], "nextUri": "http://trino:8080/v1/statement/q/2"},
        {},
    ]
    seen: list[str] = []

    def fake_urlopen(request, timeout):
        seen.append(request.full_url)
        return _Response(json.dumps(pages.pop(0)).encode("utf-8"))

    monkeypatch.setattr(medallion_build.urllib.request, "urlopen", fake_urlopen)
    assert medallion_build.trino_scalar("SELECT COUNT(*) FROM t", {}) == "5571"
    assert seen[0] == "http://trino:8080/v1/statement"
    assert seen[1:] == [
        "http://trino:8080/v1/statement/q/1",
        "http://trino:8080/v1/statement/q/2",
    ]


def test_trino_error_names_the_error_not_the_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {"error": {"errorName": "TABLE_NOT_FOUND", "message": "row 'x'"}}
    monkeypatch.setattr(
        medallion_build.urllib.request,
        "urlopen",
        lambda request, timeout: _Response(json.dumps(payload).encode("utf-8")),
    )
    with pytest.raises(MedallionBuildError) as raised:
        medallion_build.trino_scalar("SELECT 1", {})
    assert str(raised.value) == "trino query failed: TABLE_NOT_FOUND"


def test_main_reports_a_failed_step(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    status = medallion_build.main(
        ["bronze-checks", "--work-root", str(tmp_path), "--run-label", "r1"]
    )
    assert status == 1
    assert "missing count file" in capsys.readouterr().err
