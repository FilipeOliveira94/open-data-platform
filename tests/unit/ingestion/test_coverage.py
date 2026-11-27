"""Coverage ledger and gap report (P3-06)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from open_data_platform.ingestion.coverage import (
    CADENCES,
    DDL,
    SELECT_SQL,
    UPSERT_SQL,
    Cadence,
    CoverageError,
    CoverageRecord,
    coverage_report,
    format_report,
    upsert_parameters,
    validate_frequency,
)

DATASETS = ("sellers", "customers")
DAY = timedelta(days=1)
START = datetime(2026, 11, 19, tzinfo=UTC)
DAILY = Cadence(START, DAY)


def _day(offset: int) -> datetime:
    return START + offset * DAY


def _records(
    start: datetime,
    end: datetime,
    *,
    execution_id: str = "run",
    frequency: str = "daily",
    datasets: tuple[str, ...] = DATASETS,
) -> list[CoverageRecord]:
    return [
        CoverageRecord(
            source_id="olist_synth_cdc",
            dataset=dataset,
            execution_id=execution_id,
            frequency=frequency,
            window_start=start,
            window_end=end,
            row_count=3,
        )
        for dataset in datasets
    ]


def _daily(*offsets: int) -> list[CoverageRecord]:
    rows: list[CoverageRecord] = []
    for offset in offsets:
        rows += _records(_day(offset), _day(offset + 1), execution_id=f"run-{offset}")
    return rows


def test_contiguous_windows_are_clean() -> None:
    report = coverage_report(
        "daily", _daily(0, 1, 2), datasets=DATASETS, as_of=START, cadence=DAILY
    )

    assert report.clean
    assert report.expected_windows == 3
    assert report.last_window == (_day(2), _day(3))


def test_planted_gap_is_reported() -> None:
    report = coverage_report(
        "daily", _daily(0, 2), datasets=DATASETS, as_of=START, cadence=DAILY
    )

    assert not report.clean
    assert [gap.window for gap in report.gaps] == [(_day(1), _day(2))]
    assert report.gaps[0].missing_datasets == DATASETS
    assert report.overlaps == ()


def test_window_missing_one_dataset_is_a_gap() -> None:
    records = _daily(0) + _records(_day(1), _day(2), datasets=("sellers",))

    report = coverage_report(
        "daily", records, datasets=DATASETS, as_of=START, cadence=DAILY
    )

    assert [(g.window, g.missing_datasets) for g in report.gaps] == [
        ((_day(1), _day(2)), ("customers",))
    ]


def test_planted_overlap_is_reported() -> None:
    weekly = Cadence(START, 7 * DAY)
    records = _records(_day(0), _day(7), frequency="weekly") + _records(
        _day(3), _day(10), frequency="weekly", execution_id="shifted"
    )

    report = coverage_report(
        "weekly", records, datasets=DATASETS, as_of=START, cadence=weekly
    )

    assert report.overlaps == (((_day(0), _day(7)), (_day(3), _day(10))),)
    assert not report.clean


def test_rerun_of_a_covered_window_is_not_an_overlap() -> None:
    records = _daily(0, 1) + _records(_day(1), _day(2), execution_id="backfill-1")

    report = coverage_report(
        "daily", records, datasets=DATASETS, as_of=START, cadence=DAILY
    )

    assert report.clean
    assert report.rerun_windows == ((_day(1), _day(2)),)


def test_backfill_before_start_is_listed_not_an_error() -> None:
    records = _daily(-30, 0)

    report = coverage_report(
        "daily", records, datasets=DATASETS, as_of=START, cadence=DAILY
    )

    assert report.clean
    assert report.before_start == ((_day(-30), _day(-29)),)


def test_other_frequencies_are_ignored() -> None:
    weekly = _records(_day(-1), _day(6), frequency="weekly")
    manual = _records(_day(0), _day(3), frequency="manual")

    report = coverage_report(
        "daily",
        _daily(0) + weekly + manual,
        datasets=DATASETS,
        as_of=START,
        cadence=DAILY,
    )

    assert report.clean
    assert report.expected_windows == 1


def test_empty_ledger_has_no_expected_window() -> None:
    report = coverage_report("daily", [], datasets=DATASETS, as_of=START, cadence=DAILY)

    assert report.clean
    assert report.last_window is None
    assert report.expected_windows == 0


def test_manual_frequency_has_no_cadence() -> None:
    with pytest.raises(CoverageError, match="no cadence"):
        coverage_report("manual", [], datasets=DATASETS, as_of=START)


def test_default_cadences_follow_the_dag_windows() -> None:
    assert CADENCES["daily"] == Cadence(
        datetime(2026, 11, 19, tzinfo=UTC), DAY, timedelta(minutes=15)
    )
    assert CADENCES["weekly"] == Cadence(
        datetime(2026, 11, 15, tzinfo=UTC), 7 * DAY, DAY
    )
    assert CADENCES["weekly"].first_start.weekday() == 6  # Sunday


@pytest.mark.parametrize(
    ("frequency", "days", "ok"),
    [
        ("daily", 1, True),
        ("daily", 7, False),
        ("weekly", 7, True),
        ("weekly", 1, False),
        ("manual", 3, True),
        ("hourly", 1, False),
    ],
)
def test_frequency_matches_window_length(frequency: str, days: int, ok: bool) -> None:
    if ok:
        assert validate_frequency(frequency, START, START + days * DAY) == frequency
    else:
        with pytest.raises(CoverageError):
            validate_frequency(frequency, START, START + days * DAY)


def test_ddl_checks_frequency_against_the_window() -> None:
    table = DDL[1]
    assert "PRIMARY KEY (source_id, dataset, execution_id)" in table
    assert "interval '1 day'" in table
    assert "interval '7 days'" in table


def test_planted_values_stay_out_of_the_sql() -> None:
    planted = "planted-run-7f3a"
    record = CoverageRecord(
        source_id="olist_synth_cdc",
        dataset="sellers",
        execution_id=planted,
        frequency="daily",
        window_start=START,
        window_end=START + DAY,
        row_count=42,
        dag_id="ingest_olist_synth_cdc_daily",
        airflow_run_id=planted,
    )

    rows = upsert_parameters([record], START)

    for statement in (UPSERT_SQL, SELECT_SQL, *DDL):
        assert planted not in statement
        assert "42" not in statement
    assert rows[0][2] == planted
    assert rows[0][7] == planted
    assert UPSERT_SQL.count("%s") == len(rows[0])


def test_upsert_rejects_negative_counts() -> None:
    record = _records(START, START + DAY)[0]
    bad = CoverageRecord(**{**_asdict(record), "row_count": -1})

    with pytest.raises(CoverageError, match="negative row count"):
        upsert_parameters([bad], START)


def test_format_report_names_windows_and_datasets_only() -> None:
    report = coverage_report(
        "daily", _daily(0, 2), datasets=DATASETS, as_of=START, cadence=DAILY
    )

    lines = format_report(report)

    assert lines[0].startswith("frequency=daily first_window=[2026-11-19,2026-11-20)")
    assert "gap [2026-11-20,2026-11-21) missing=sellers,customers" in lines
    assert lines[-1] == "not clean"


def _asdict(record: CoverageRecord) -> dict[str, object]:
    return {name: getattr(record, name) for name in record.__slots__}


def test_a_stopped_dag_reports_its_trailing_gaps() -> None:
    """Only the first window was recorded, and the DAG then stopped.

    Every window since then should have run by now, so each one is a gap
    (review R7).
    """
    report = coverage_report(
        "daily",
        _records(_day(0), _day(1)),
        datasets=DATASETS,
        as_of=_day(4),
        cadence=DAILY,
    )
    assert [gap.window for gap in report.gaps] == [
        (_day(1), _day(2)),
        (_day(2), _day(3)),
        (_day(3), _day(4)),
    ]


def test_a_window_is_not_due_before_its_run_delay() -> None:
    lagged = Cadence(START, DAY, timedelta(minutes=15))
    records = _records(_day(0), _day(1))

    early = coverage_report(
        "daily",
        records,
        datasets=DATASETS,
        as_of=_day(2) + timedelta(minutes=14),
        cadence=lagged,
    )
    due = coverage_report(
        "daily",
        records,
        datasets=DATASETS,
        as_of=_day(2) + timedelta(minutes=15),
        cadence=lagged,
    )

    assert early.clean
    assert [gap.window for gap in due.gaps] == [(_day(1), _day(2))]


def test_a_dag_that_never_ran_reports_every_due_window() -> None:
    report = coverage_report(
        "daily", [], datasets=DATASETS, as_of=_day(2), cadence=DAILY
    )

    assert report.last_window is None
    assert report.expected_windows == 2
    assert len(report.gaps) == 2


def test_as_of_must_be_timezone_aware() -> None:
    with pytest.raises(CoverageError, match="timezone-aware"):
        coverage_report(
            "daily", [], datasets=DATASETS, as_of=datetime(2026, 11, 20), cadence=DAILY
        )
