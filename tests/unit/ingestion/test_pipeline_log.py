"""Stage logs name counts and paths. A planted record value stays out."""

from __future__ import annotations

import logging

import pytest

from open_data_platform.ingestion.pipeline_log import (
    configure_cli_logging,
    finding_count,
    log_failure,
    log_stage,
)

PLANTED = "planted-secret-value"


def test_stage_log_reports_counts_and_hides_the_planted_value(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_cli_logging()
    log_stage(
        "wrote",
        dataset="orders",
        record_count=2,
        byte_count=40,
        object_key="raw/postgres_oltp/orders/dt=2026-10-17/execution_id=run/orders.json",
        finding_count=0,
    )
    captured = capsys.readouterr()
    assert PLANTED not in captured.out
    assert "INFO stage=wrote" in captured.out
    assert "dataset=orders" in captured.out
    assert "record_count=2" in captured.out
    assert "byte_count=40" in captured.out
    assert "finding_count=0" in captured.out
    assert captured.err == ""


def test_finding_count_counts_lines_without_returning_them() -> None:
    body = f'{{"record":"{PLANTED}"}}\n{{"record":"other"}}\n'.encode()
    assert finding_count(body) == 2
    assert finding_count(None) == 0


def test_stage_log_rejects_a_record(capsys: pytest.CaptureFixture[str]) -> None:
    configure_cli_logging()
    with pytest.raises(TypeError):
        log_stage("wrote", record={"city": PLANTED})  # type: ignore[arg-type]
    captured = capsys.readouterr()
    assert PLANTED not in captured.out
    assert PLANTED not in captured.err
    assert captured.out == ""
    assert captured.err == ""


def test_failure_log_drops_a_multiline_message(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_cli_logging()
    log_failure(RuntimeError(f"orders failed\n{PLANTED}"))
    captured = capsys.readouterr()
    assert "ERROR stage=failed" in captured.err
    assert "error_type=RuntimeError" in captured.err
    assert PLANTED not in captured.err
    assert "message=" in captured.err
    assert captured.out == ""


def test_configure_cli_logging_is_idempotent() -> None:
    configure_cli_logging()
    configure_cli_logging()
    logger = logging.getLogger("open_data_platform.ingestion")
    assert len(logger.handlers) == 2
