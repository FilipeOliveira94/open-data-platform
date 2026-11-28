"""P11-04 Spark-Kafka probe: jar pins, wrapper hygiene and the pure checks."""

from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SPARK = ROOT / "docker" / "spark"
WRAPPER = SPARK / "run_kafka_probe.sh"
_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"
_PACKAGE = re.compile(r'packages="(?:\$\{packages\},)?([^"]+)"')


def _load_probe():
    spec = importlib.util.spec_from_file_location(
        "kafka_probe", SPARK / "kafka_probe.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["kafka_probe"] = module
    spec.loader.exec_module(module)
    return module


probe = _load_probe()


def _packages(path: Path) -> set[str]:
    return set(_PACKAGE.findall(path.read_text(encoding="utf-8")))


def test_kafka_source_is_pinned_and_cached_with_the_lakehouse_jars() -> None:
    warm = _packages(SPARK / "warm_ivy.sh")
    assert _KAFKA_PACKAGE in warm
    assert _packages(WRAPPER) == warm


def test_batch_runners_do_not_load_the_kafka_source() -> None:
    for name in ("run_bronze.sh", "run_silver.sh"):
        assert "spark-sql-kafka" not in (SPARK / name).read_text(encoding="utf-8")


def test_wrapper_caps_resources_and_launches_the_probe() -> None:
    script = WRAPPER.read_text(encoding="utf-8")
    assert '--master "local[${ODP_SPARK_CORES}]"' in script
    assert '--driver-memory "${ODP_SPARK_DRIVER_MEMORY}"' in script
    assert "kafka_probe.py" in script
    assert '"$@"' in script
    # The driver JVM must not be PID 1, or --kill-in-flight cannot kill it.
    assert "exec /opt/spark/bin/spark-submit" not in script
    assert "/opt/spark/bin/spark-submit" in script


def test_wrapper_rejects_a_bad_core_count(tmp_path: Path) -> None:
    env = {
        "PATH": "/usr/bin:/bin",
        "MINIO_ROOT_USER": "placeholder",
        "MINIO_ROOT_PASSWORD": "placeholder",
        "ODP_WAREHOUSE_BUCKET": "odp-warehouse",
        "POSTGRES_USER": "placeholder",
        "POSTGRES_PASSWORD": "placeholder",
        "ICEBERG_CATALOG_DB": "iceberg",
        "ODP_SPARK_CORES": "0",
        "TMPDIR": str(tmp_path),
    }
    completed = subprocess.run(
        ["bash", str(WRAPPER), "check"], env=env, capture_output=True, text=True
    )
    assert completed.returncode == 1
    assert "ODP_SPARK_CORES must be a positive integer" in completed.stderr


def test_probe_imports_pyspark_only_inside_the_spark_modes() -> None:
    source = (SPARK / "kafka_probe.py").read_text(encoding="utf-8")
    assert not re.search(r"^(from pyspark|import pyspark)", source, re.MULTILINE)


def test_parse_offset_log_reads_the_source_offsets() -> None:
    text = (
        "v1\n"
        '{"batchWatermarkMs":0,"batchTimestampMs":1,"conf":{}}\n'
        '{"bioguardia.telemetry.health.v1":{"2":30,"1":20,"0":10}}\n'
    )
    assert probe.parse_offset_log(text, "bioguardia.telemetry.health.v1") == {
        0: 10,
        1: 20,
        2: 30,
    }


def test_parse_offset_log_rejects_another_topic() -> None:
    text = 'v1\n{}\n{"other":{"0":1}}\n'
    with pytest.raises(ValueError, match="topic"):
        probe.parse_offset_log(text, "bioguardia.telemetry.health.v1")


def test_contiguity_accepts_a_gapless_range() -> None:
    stats = [probe.PartitionStats(0, 10, 19, 10, 10)]
    assert probe.contiguity_problems(stats) == []


@pytest.mark.parametrize(
    ("stats", "fragment"),
    [
        (probe.PartitionStats(0, 10, 19, 11, 10), "duplicate"),
        (probe.PartitionStats(1, 10, 19, 9, 9), "gap"),
    ],
)
def test_contiguity_names_duplicates_and_gaps(
    stats: probe.PartitionStats, fragment: str
) -> None:
    problems = probe.contiguity_problems([stats])
    assert len(problems) == 1
    assert fragment in problems[0]
    assert f"partition {stats.partition}" in problems[0]


def test_run_continuity_checks_each_run_starts_after_the_previous() -> None:
    runs = [
        probe.RunStats("r1", 0, 0, 99),
        probe.RunStats("r2", 0, 100, 150),
        probe.RunStats("r1", 1, 5, 9),
        probe.RunStats("r2", 1, 11, 20),
    ]
    problems = probe.run_continuity_problems(runs, ["r1", "r2"])
    assert problems == ["partition 1: run r2 starts at 11, expected 10"]


def test_in_flight_batch_is_planned_but_not_committed() -> None:
    assert probe.in_flight_batch({0, 1, 2}, {0, 1}) == 2
    assert probe.in_flight_batch({0, 1}, {0, 1}) is None
    assert probe.in_flight_batch(set(), set()) is None


def test_batch_ids_ignore_checksum_and_temp_files(tmp_path: Path) -> None:
    for name in ("0", "1", ".1.crc", "2.tmp", "_metadata"):
        (tmp_path / name).write_text("", encoding="utf-8")
    assert probe.batch_ids(tmp_path) == {0, 1}
    assert probe.batch_ids(tmp_path / "missing") == set()


@pytest.mark.parametrize(
    "argv",
    [
        ["run"],
        ["run", "--run-label", "r 1"],
        ["run", "--run-label", "r1", "--max-seconds", "0"],
        ["nope"],
    ],
)
def test_cli_rejects_bad_arguments(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        probe.parse_args(argv)


def test_cli_defaults() -> None:
    args = probe.parse_args(["run", "--run-label", "r1"])
    assert args.topic == "bioguardia.telemetry.health.v1"
    assert args.bootstrap == "kafka:9092"
    assert args.trigger_seconds == 10
    assert args.kill_in_flight is False


def test_table_location_is_a_scratch_prefix_of_a_plain_bucket() -> None:
    assert (
        probe.table_location("odp-warehouse")
        == "s3a://odp-warehouse/scratch/p11_04/telemetry_probe"
    )
    for bad in ("", "Odp", "a/b", "odp'warehouse"):
        with pytest.raises(SystemExit):
            probe.table_location(bad)


def test_tail_matches_the_committed_end_offsets() -> None:
    stats = [
        probe.PartitionStats(0, 10, 19, 10, 10),
        probe.PartitionStats(1, 5, 9, 5, 5),
    ]
    assert probe.tail_problems(stats, {0: 20, 1: 10}) == []


def test_tail_names_lost_rows_and_missing_partitions() -> None:
    stats = [
        probe.PartitionStats(0, 10, 19, 10, 10),
        probe.PartitionStats(3, 1, 1, 1, 1),
    ]
    assert probe.tail_problems(stats, {0: 25, 2: 7}) == [
        "partition 0: table ends at 19, committed end offset is 25",
        "partition 2: no rows, committed end 7",
        "partition 3: rows but no committed end offset",
    ]
