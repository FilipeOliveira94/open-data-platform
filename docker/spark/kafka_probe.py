"""P11-04: Spark Structured Streaming on Kafka, compatibility probe.

Reads one telemetry topic and appends it to a scratch Iceberg table through
the JDBC catalog. This is not a pipeline step: ``drop`` removes the table,
its namespace and the checkpoint after the check (ADR-0028).

Modes, each one ``spark-submit`` through ``run_kafka_probe.sh``:

- ``run``: stream with a processing-time trigger, then stop between
  micro-batches after ``--max-seconds``. ``--kill-in-flight`` instead kills
  the driver JVM with SIGKILL as soon as a new batch is planned
  (``offsets/N`` written) and not yet committed (no ``commits/N``).
- ``drain``: ``Trigger.AvailableNow`` from the checkpoint, then exit.
- ``rewind-commit``: delete the newest commit-log entry, the state a crash
  leaves between the sink commit and the commit log. No Spark session.
- ``check``: counts, per-partition offset ranges, per-run ranges and the
  Iceberg snapshots per streaming epoch. Exits 1 on a duplicate or a gap.
- ``drop``: drop the table and namespace, delete the checkpoint.

Output is counts, offsets, batch ids and names only, never a payload
(AGENTS.md section 8). PySpark is imported inside the Spark modes, so the
checks below are unit-tested without it.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import sys
import threading
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

NAMESPACE = "local.scratch_p11_04"
TABLE = f"{NAMESPACE}.telemetry_probe"
DEFAULT_TOPIC = "bioguardia.telemetry.health.v1"
SCRATCH_PREFIX = "scratch/p11_04"
EPOCH_KEY = "spark.sql.streaming.epochId"
_LABEL = re.compile(r"^[a-z0-9_-]{1,32}$")
_BATCH_FILE = re.compile(r"^[0-9]+$")
_BUCKET = re.compile(r"^[a-z0-9.-]{3,63}$")


@dataclass(frozen=True, slots=True)
class PartitionStats:
    partition: int
    min_offset: int
    max_offset: int
    rows: int
    distinct_offsets: int


@dataclass(frozen=True, slots=True)
class RunStats:
    run_label: str
    partition: int
    min_offset: int
    max_offset: int


def parse_offset_log(text: str, topic: str) -> dict[int, int]:
    """Source offsets of one ``offsets/N`` file: partition -> next offset."""
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 3 or not lines[0].startswith("v"):
        raise ValueError("offset log entry has no source line")
    source = json.loads(lines[2])
    if set(source) != {topic}:
        raise ValueError("offset log entry names another topic")
    return {int(partition): int(offset) for partition, offset in source[topic].items()}


def contiguity_problems(stats: Iterable[PartitionStats]) -> list[str]:
    """A duplicate (partition, offset) or a gap in a partition's range."""
    problems: list[str] = []
    for item in sorted(stats, key=lambda s: s.partition):
        span = item.max_offset - item.min_offset + 1
        if item.rows != item.distinct_offsets:
            problems.append(
                f"partition {item.partition}: {item.rows - item.distinct_offsets}"
                " duplicate offsets"
            )
        elif item.distinct_offsets != span:
            problems.append(
                f"partition {item.partition}: gap, {item.distinct_offsets} offsets"
                f" in a span of {span}"
            )
    return problems


def run_continuity_problems(
    runs: Iterable[RunStats], order: Sequence[str]
) -> list[str]:
    """Each run starts at the previous run's last offset + 1, per partition."""
    by_partition: dict[int, dict[str, RunStats]] = {}
    for run in runs:
        by_partition.setdefault(run.partition, {})[run.run_label] = run
    problems: list[str] = []
    for partition in sorted(by_partition):
        seen = [
            by_partition[partition][label]
            for label in order
            if label in by_partition[partition]
        ]
        for previous, current in zip(seen, seen[1:], strict=False):
            expected = previous.max_offset + 1
            if current.min_offset != expected:
                problems.append(
                    f"partition {partition}: run {current.run_label} starts at"
                    f" {current.min_offset}, expected {expected}"
                )
    return problems


def tail_problems(
    stats: Iterable[PartitionStats], committed_ends: dict[int, int]
) -> list[str]:
    """The table's last offset + 1 equals the newest committed batch's end."""
    by_partition = {item.partition: item for item in stats}
    problems: list[str] = []
    for partition in sorted(set(by_partition) | set(committed_ends)):
        end = committed_ends.get(partition)
        item = by_partition.get(partition)
        if end is None:
            problems.append(f"partition {partition}: rows but no committed end offset")
        elif item is None:
            problems.append(f"partition {partition}: no rows, committed end {end}")
        elif item.max_offset + 1 != end:
            problems.append(
                f"partition {partition}: table ends at {item.max_offset},"
                f" committed end offset is {end}"
            )
    return problems


def batch_ids(directory: Path) -> set[int]:
    """Batch ids of a checkpoint log directory (``offsets`` or ``commits``)."""
    if not directory.is_dir():
        return set()
    return {
        int(path.name) for path in directory.iterdir() if _BATCH_FILE.match(path.name)
    }


def in_flight_batch(offsets: set[int], commits: set[int]) -> int | None:
    """The newest planned batch when it has no commit entry yet."""
    if not offsets:
        return None
    newest = max(offsets)
    return None if newest in commits else newest


def _label(value: str) -> str:
    if not _LABEL.match(value):
        raise argparse.ArgumentTypeError("run label must match [a-z0-9_-]{1,32}")
    return value


def _positive(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="kafka_probe")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--bootstrap", default=os.environ.get("ODP_KAFKA_BOOTSTRAP", "kafka:9092")
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=Path(os.environ.get("ODP_WORK_ROOT", "/opt/odp/work"))
        / "p11-04"
        / "checkpoint",
    )
    modes = parser.add_subparsers(dest="mode", required=True)
    for name in ("run", "drain"):
        mode = modes.add_parser(name)
        mode.add_argument("--run-label", type=_label, required=True)
        mode.add_argument("--max-offsets-per-trigger", type=_positive, default=20_000)
    run = modes.choices["run"]
    run.add_argument("--trigger-seconds", type=_positive, default=10)
    run.add_argument("--max-seconds", type=_positive, default=120)
    run.add_argument("--kill-in-flight", action="store_true")
    check = modes.add_parser("check")
    check.add_argument("--runs", default="", help="comma-separated run labels in order")
    modes.add_parser("rewind-commit")
    modes.add_parser("drop")
    return parser.parse_args(argv)


def _say(message: str) -> None:
    print(f"kafka_probe: {message}", flush=True)


def _session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName("odp-p11-04-kafka-probe").getOrCreate()


def table_location(bucket: str) -> str:
    """The probe table's own prefix; ``drop`` deletes all of it."""
    if not _BUCKET.match(bucket):
        raise SystemExit("ODP_WAREHOUSE_BUCKET must be a plain bucket name")
    return f"s3a://{bucket}/{SCRATCH_PREFIX}/telemetry_probe"


def _ensure_table(spark, location: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {TABLE} ("
        "topic STRING, `partition` INT, `offset` BIGINT,"
        " kafka_timestamp TIMESTAMP, value STRING, probe_run STRING)"
        f" USING iceberg LOCATION '{location}'"
        " TBLPROPERTIES ('format-version'='2')"
    )


def _stream(args: argparse.Namespace) -> int:
    from pyspark.sql import functions as F

    spark = _session()
    _ensure_table(spark, table_location(os.environ["ODP_WAREHOUSE_BUCKET"]))
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))
        .load()
    )
    rows = source.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("value"),
        F.lit(args.run_label).alias("probe_run"),
    )
    writer = (
        rows.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", str(args.checkpoint))
    )
    if args.mode == "drain":
        query = writer.trigger(availableNow=True).toTable(TABLE)
        query.awaitTermination()
        _say(f"drain {args.run_label} finished")
        return 0
    newest_at_start = max(batch_ids(args.checkpoint / "offsets"), default=-1)
    query = writer.trigger(processingTime=f"{args.trigger_seconds} seconds").toTable(
        TABLE
    )
    if args.kill_in_flight:
        _kill_when_in_flight(args.checkpoint, newest_at_start)
    query.awaitTermination(args.max_seconds)
    if args.kill_in_flight:
        query.stop()
        _say(f"run {args.run_label}: no batch was seen in flight; nothing killed")
        return 1
    # Structured Streaming has no graceful stop: wait for an idle trigger,
    # stop, and report it when a batch started in between.
    while query.status["isTriggerActive"]:
        time.sleep(0.05)
    query.stop()
    offsets = batch_ids(args.checkpoint / "offsets")
    batch = in_flight_batch(offsets, batch_ids(args.checkpoint / "commits"))
    if batch is not None:
        _say(f"run {args.run_label} stopped while batch {batch} was in flight")
        return 1
    _say(f"run {args.run_label} stopped between micro-batches")
    return 0


def _kill_when_in_flight(checkpoint: Path, newest_at_start: int) -> None:
    """SIGKILL the driver JVM (this process's parent) mid-batch.

    ``newest_at_start`` is read before the query starts, so a batch the
    restart replays from the checkpoint is not the one killed.
    """
    parent = os.getppid()
    try:
        comm = Path(f"/proc/{parent}/comm").read_text(encoding="utf-8").strip()
    except OSError:
        comm = ""
    if comm != "java":
        _say("the parent process is not the driver JVM; the kill is disabled")
        return

    def watch() -> None:
        while True:
            offsets = batch_ids(checkpoint / "offsets")
            batch = in_flight_batch(offsets, batch_ids(checkpoint / "commits"))
            if batch is not None and batch > newest_at_start:
                _say(f"killing the driver JVM while batch {batch} is in flight")
                os.kill(parent, signal.SIGKILL)
                os._exit(137)
            time.sleep(0.02)

    threading.Thread(target=watch, daemon=True).start()


def _rewind_commit(checkpoint: Path) -> int:
    commits = checkpoint / "commits"
    ids = batch_ids(commits)
    if not ids:
        _say("no commit-log entry to rewind")
        return 1
    newest = max(ids)
    if in_flight_batch(batch_ids(checkpoint / "offsets"), ids) is not None:
        _say("the newest planned batch is already uncommitted; nothing rewound")
        return 1
    for name in (str(newest), f".{newest}.crc"):
        (commits / name).unlink(missing_ok=True)
    _say(f"removed commit-log entry {newest}; the next start replays batch {newest}")
    return 0


def _checkpoint_report(args: argparse.Namespace) -> dict[int, int] | None:
    """Print the checkpoint state; return the newest committed end offsets."""
    offsets = batch_ids(args.checkpoint / "offsets")
    commits = batch_ids(args.checkpoint / "commits")
    _say(
        f"checkpoint: newest planned batch {max(offsets, default=None)},"
        f" newest committed batch {max(commits, default=None)},"
        f" in flight {in_flight_batch(offsets, commits)}"
    )
    if offsets:
        newest = max(offsets)
        text = (args.checkpoint / "offsets" / str(newest)).read_text(encoding="utf-8")
        ends = parse_offset_log(text, args.topic)
        ends_text = json.dumps(ends, sort_keys=True)
        _say(f"checkpoint end offsets of batch {newest}: {ends_text}")
    if not commits:
        return None
    committed = max(commits)
    text = (args.checkpoint / "offsets" / str(committed)).read_text(encoding="utf-8")
    return parse_offset_log(text, args.topic)


def _check(args: argparse.Namespace) -> int:
    from pyspark.sql import functions as F

    committed_ends = _checkpoint_report(args)
    spark = _session()
    table = spark.table(TABLE)
    total = table.count()
    distinct = table.select("partition", "offset").distinct().count()
    _say(f"rows {total}, distinct (partition, offset) {distinct}")
    partitions = [
        PartitionStats(r["partition"], r["lo"], r["hi"], r["n"], r["d"])
        for r in table.groupBy("partition")
        .agg(
            F.min("offset").alias("lo"),
            F.max("offset").alias("hi"),
            F.count(F.lit(1)).alias("n"),
            F.countDistinct("offset").alias("d"),
        )
        .collect()
    ]
    for item in sorted(partitions, key=lambda s: s.partition):
        _say(
            f"partition {item.partition}: offsets {item.min_offset}..{item.max_offset},"
            f" rows {item.rows}"
        )
    runs = [
        RunStats(r["probe_run"], r["partition"], r["lo"], r["hi"])
        for r in table.groupBy("probe_run", "partition")
        .agg(
            F.min("offset").alias("lo"),
            F.max("offset").alias("hi"),
            F.count(F.lit(1)).alias("n"),
        )
        .collect()
    ]
    for run in sorted(runs, key=lambda r: (r.run_label, r.partition)):
        _say(
            f"run {run.run_label} partition {run.partition}:"
            f" offsets {run.min_offset}..{run.max_offset}"
        )
    epochs = [
        r["epoch"]
        for r in spark.sql(
            f"SELECT summary['{EPOCH_KEY}'] AS epoch FROM {TABLE}.snapshots"
        ).collect()
    ]
    repeated = sorted({e for e in epochs if e is not None and epochs.count(e) > 1})
    epoch_ids = sorted({int(e) for e in epochs if e})
    _say(
        f"snapshots {len(epochs)}, streaming epochs {epoch_ids},"
        f" epochs with more than one snapshot {repeated}"
    )
    order = [label for label in args.runs.split(",") if label]
    problems = contiguity_problems(partitions) + run_continuity_problems(runs, order)
    if total == 0:
        problems.append("the table has no rows")
    if committed_ends is None:
        problems.append("the checkpoint has no committed batch")
    else:
        problems += tail_problems(partitions, committed_ends)
    if repeated:
        problems.append(f"epochs committed twice: {repeated}")
    for problem in problems:
        _say(f"FAIL {problem}")
    _say("PASS" if not problems else "FAIL")
    return 0 if not problems else 1


def _drop(args: argparse.Namespace) -> int:
    spark = _session()
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS {NAMESPACE}")
    # PURGE deletes only files the table references. A batch replayed after
    # a crash writes its data files before Iceberg skips the commit of an
    # epoch already committed, so the prefix is emptied with the S3 API
    # (S3A would leave a directory marker behind).
    from open_data_platform.object_storage import client_from_env

    bucket = os.environ["ODP_WAREHOUSE_BUCKET"]
    table_location(bucket)
    client = client_from_env(os.environ)
    removed = 0
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{SCRATCH_PREFIX}/"
    ):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            reply = client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            removed += len(keys) - len(reply.get("Errors", []))
    left = client.list_objects_v2(Bucket=bucket, Prefix=f"{SCRATCH_PREFIX}/")
    shutil.rmtree(args.checkpoint, ignore_errors=True)
    _say(f"dropped {TABLE}, {NAMESPACE}, the checkpoint and {removed} scratch objects")
    if left.get("KeyCount", 0) or args.checkpoint.exists():
        _say(f"FAIL {left.get('KeyCount', 0)} scratch objects or the checkpoint remain")
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.mode in ("run", "drain"):
        return _stream(args)
    if args.mode == "rewind-commit":
        return _rewind_commit(args.checkpoint)
    if args.mode == "check":
        return _check(args)
    return _drop(args)


if __name__ == "__main__":
    raise SystemExit(main())
