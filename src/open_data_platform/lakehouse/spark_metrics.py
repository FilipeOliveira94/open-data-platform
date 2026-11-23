"""Measured read and write counts of one Spark SQL script (P3-08).

Each medallion Spark script runs with two transient files in its work dir:

- the Spark event log. The Iceberg scan of every ``BatchScan`` node reports
  its result and skipped data files and the bytes of the files it reads as
  driver metrics; its task metrics count the rows it returns.
- the Iceberg ``CommitReport`` lines of ``LoggingMetricsReporter``
  (``docker/spark/log4j2-metrics.properties``): added and removed data
  files and records, and the table total after each commit.

Iceberg 1.12 does not send Spark scans to ``LoggingMetricsReporter``: a scan
with its own reporter replaces the logging one. The event log is therefore
the scan source and the commit reports are the write source.

Both files can carry statement text and scan filters. They are mode 600,
read for counts and table names only, and deleted after parsing. Nothing
from them is printed.
"""

from __future__ import annotations

import json
import re
import shutil
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path

CATALOG = "local"
EVENT_DIR = "spark-events"
COMMIT_FILE = "iceberg-commits.log"

_SCAN_PREFIX = "BatchScan "
# Driver metrics of an Iceberg Spark scan, by display name.
_SCAN_DRIVER_METRICS = {
    "number of result data files": "scanned_files",
    "number of skipped data files": "skipped_files",
    "total data file size (bytes)": "scanned_bytes",
}
_SCAN_ROWS = "number of output rows"
_COMMIT = re.compile(r"CommitReport\{tableName=(?P<table>[^,]+),")
_COMMIT_COUNTER = re.compile(
    r"(?P<name>[A-Za-z]+)=CounterResult\{unit=[A-Z]+, value=(?P<value>\d+)\}"
)
_COMMIT_FIELDS = {
    "addedDataFiles": "added_files",
    "removedDataFiles": "removed_files",
    "addedRecords": "added_rows",
    "removedRecords": "removed_rows",
}
_TABLE = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


class SparkMetricsError(RuntimeError):
    """The metrics files of a Spark script could not be read."""


@dataclass(frozen=True, slots=True)
class TableIO:
    """What one Spark script read from and wrote to one Iceberg table.

    Scan counts add up over every scan of the table in the script.
    ``scans`` is the number of SQL executions that scanned it. ``total_rows``
    is the table total after the last commit. ``None`` means the script did
    not scan, or did not commit, that table.
    """

    table: str
    scans: int = 0
    scanned_files: int | None = None
    skipped_files: int | None = None
    scanned_bytes: int | None = None
    rows_scanned: int | None = None
    commits: int = 0
    added_files: int | None = None
    removed_files: int | None = None
    added_rows: int | None = None
    removed_rows: int | None = None
    total_rows: int | None = None


@dataclass(frozen=True, slots=True)
class SparkMetrics:
    """The tables one Spark script touched, by ``namespace.table``."""

    tables: Mapping[str, TableIO] = field(default_factory=dict)

    def get(self, table: str) -> TableIO | None:
        return self.tables.get(table)


@dataclass(frozen=True, slots=True)
class MetricsCapture:
    """Paths one Spark script writes its metrics to, under ``directory``."""

    directory: Path

    @property
    def event_dir(self) -> Path:
        return self.directory / EVENT_DIR

    @property
    def commit_file(self) -> Path:
        return self.directory / COMMIT_FILE

    def prepare(self) -> dict[str, str]:
        """Create an empty capture dir and return the Spark script env."""
        if self.directory.exists():
            shutil.rmtree(self.directory)
        self.event_dir.mkdir(parents=True, mode=0o700)
        self.directory.chmod(0o700)
        return {
            "ODP_SPARK_EVENT_DIR": str(self.event_dir),
            "ODP_SPARK_METRICS_FILE": str(self.commit_file),
        }

    def collect(self) -> SparkMetrics:
        """Parse both files, then delete the capture dir.

        A script that failed before Spark started leaves no file and gives
        empty metrics.
        """
        try:
            scans: dict[str, dict[str, int]] = {}
            for path in sorted(self.event_dir.glob("*")):
                if path.is_file():
                    with path.open(encoding="utf-8") as lines:
                        _add_scans(scans, parse_event_log(lines))
            commits: dict[str, dict[str, int]] = {}
            if self.commit_file.is_file():
                with self.commit_file.open(encoding="utf-8") as lines:
                    commits = parse_commit_reports(lines)
        except (OSError, ValueError) as exc:
            msg = f"spark metrics unreadable: {type(exc).__name__}"
            raise SparkMetricsError(msg) from exc
        finally:
            shutil.rmtree(self.directory, ignore_errors=True)
        return combine(scans, commits)


def table_name(name: str) -> str | None:
    """``namespace.table`` of an Iceberg table name, or ``None``.

    Spark names a scanned table ``local.ns.table``; a create-as-select commit
    reports ``ns.table``. Metadata tables and path tables are not data
    tables and give ``None``.
    """
    parts = name.strip().split(".")
    if len(parts) == 3 and parts[0] == CATALOG:
        parts = parts[1:]
    candidate = ".".join(parts)
    return candidate if _TABLE.fullmatch(candidate) else None


def parse_event_log(lines: Iterable[str]) -> dict[str, dict[str, int]]:
    """Scan counts per table from one Spark event log.

    Plan nodes come from ``SQLExecutionStart`` and adaptive plan updates;
    their accumulator ids map driver metric updates and successful task
    updates to the table of the ``BatchScan`` node.
    """
    metric_of: dict[int, tuple[str, str]] = {}
    scan_nodes: set[tuple[int, str]] = set()
    totals: dict[str, dict[str, int]] = {}
    for line in lines:
        if not line.strip():
            continue
        event = json.loads(line)
        kind = str(event.get("Event", ""))
        if kind.endswith("SQLExecutionStart") or kind.endswith(
            "SQLAdaptiveExecutionUpdate"
        ):
            execution = int(event["executionId"])
            for node_name, metrics in _plan_nodes(event.get("sparkPlanInfo") or {}):
                if not node_name.startswith(_SCAN_PREFIX):
                    continue
                table = table_name(node_name.removeprefix(_SCAN_PREFIX))
                if table is None:
                    continue
                scan_nodes.add((execution, table))
                for metric in metrics:
                    name = str(metric.get("name", ""))
                    if name in _SCAN_DRIVER_METRICS or name == _SCAN_ROWS:
                        metric_of[int(metric["accumulatorId"])] = (table, name)
        elif kind.endswith("DriverAccumUpdates"):
            for accumulator, value in event.get("accumUpdates") or ():
                _add(totals, metric_of.get(int(accumulator)), int(value), driver=True)
        elif kind == "SparkListenerTaskEnd":
            reason = (event.get("Task End Reason") or {}).get("Reason")
            if reason != "Success":
                continue
            info = event.get("Task Info") or {}
            for item in info.get("Accumulables") or ():
                found = metric_of.get(int(item.get("ID", -1)))
                if found is not None and found[1] == _SCAN_ROWS:
                    _add(totals, found, int(item.get("Update") or 0), driver=False)
    for _execution, table in scan_nodes:
        entry = totals.setdefault(table, {})
        entry["scans"] = entry.get("scans", 0) + 1
    return totals


def parse_commit_reports(lines: Iterable[str]) -> dict[str, dict[str, int]]:
    """Commit counts per table from ``LoggingMetricsReporter`` lines."""
    totals: dict[str, dict[str, int]] = {}
    for line in lines:
        found = _COMMIT.search(line)
        if found is None:
            continue
        table = table_name(found.group("table"))
        if table is None:
            continue
        entry = totals.setdefault(table, {})
        entry["commits"] = entry.get("commits", 0) + 1
        for counter in _COMMIT_COUNTER.finditer(line):
            name = counter.group("name")
            value = int(counter.group("value"))
            if name in _COMMIT_FIELDS:
                key = _COMMIT_FIELDS[name]
                entry[key] = entry.get(key, 0) + value
            elif name == "totalRecords":
                entry["total_rows"] = value
        for key in _COMMIT_FIELDS.values():
            entry.setdefault(key, 0)
    return totals


def combine(
    scans: Mapping[str, Mapping[str, int]],
    commits: Mapping[str, Mapping[str, int]],
) -> SparkMetrics:
    """One ``TableIO`` per table that was scanned or committed."""
    tables: dict[str, TableIO] = {}
    for table in sorted(set(scans) | set(commits)):
        item = TableIO(table=table)
        scan = scans.get(table)
        if scan:
            item = replace(
                item,
                scans=scan.get("scans", 0),
                scanned_files=scan.get("scanned_files", 0),
                skipped_files=scan.get("skipped_files", 0),
                scanned_bytes=scan.get("scanned_bytes", 0),
                rows_scanned=scan.get("rows_scanned", 0),
            )
        commit = commits.get(table)
        if commit:
            item = replace(
                item,
                commits=commit.get("commits", 0),
                added_files=commit.get("added_files"),
                removed_files=commit.get("removed_files"),
                added_rows=commit.get("added_rows"),
                removed_rows=commit.get("removed_rows"),
                total_rows=commit.get("total_rows"),
            )
        tables[table] = item
    return SparkMetrics(tables=tables)


def _plan_nodes(node: Mapping[str, object]) -> Iterable[tuple[str, list[dict]]]:
    stack = [node]
    while stack:
        current = stack.pop()
        metrics = current.get("metrics")
        yield str(current.get("nodeName", "")), list(metrics or ())  # type: ignore[call-overload]
        children = current.get("children")
        stack.extend(children or ())  # type: ignore[arg-type]


def _add(
    totals: dict[str, dict[str, int]],
    found: tuple[str, str] | None,
    value: int,
    *,
    driver: bool,
) -> None:
    if found is None:
        return
    table, name = found
    if name == _SCAN_ROWS:
        if driver:
            return
        key = "rows_scanned"
    else:
        key = _SCAN_DRIVER_METRICS[name]
    entry = totals.setdefault(table, {})
    entry[key] = entry.get(key, 0) + value


def _add_scans(
    into: dict[str, dict[str, int]], more: Mapping[str, Mapping[str, int]]
) -> None:
    for table, counts in more.items():
        entry = into.setdefault(table, {})
        for key, value in counts.items():
            entry[key] = entry.get(key, 0) + value
