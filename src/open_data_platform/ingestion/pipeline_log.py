"""Stage logs for ingestion pipelines.

Lines name the stage, the dataset, the object path, and counts. They do not
include a record, a field value, or a secret. The CLI configures the logger.
Library calls stay quiet until that configuration is in place.
"""

from __future__ import annotations

import logging
import sys

_LOGGER_NAME = "open_data_platform.ingestion"


class _CurrentStream:
    """Write to the named stream as it is now.

    A handler that keeps the stream object from configuration time misses
    later redirections, including a test capture.
    """

    def __init__(self, name: str) -> None:
        self._name = name

    def write(self, text: str) -> int:
        stream = sys.stdout if self._name == "stdout" else sys.stderr
        return stream.write(text)

    def flush(self) -> None:
        stream = sys.stdout if self._name == "stdout" else sys.stderr
        stream.flush()


class _BelowError(logging.Filter):
    """Keep INFO and WARNING off stderr."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.ERROR


def configure_cli_logging() -> None:
    """Send stage lines to stdout and failures to stderr.

    Airflow records stdout as info and stderr as error. ``logging.info``
    still has to use stdout, or the task log paints the line red.
    """
    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return
    formatter = logging.Formatter("%(levelname)s %(message)s")
    info = logging.StreamHandler(_CurrentStream("stdout"))
    info.setLevel(logging.INFO)
    info.addFilter(_BelowError())
    info.setFormatter(formatter)
    failure = logging.StreamHandler(_CurrentStream("stderr"))
    failure.setLevel(logging.ERROR)
    failure.setFormatter(formatter)
    logger.addHandler(info)
    logger.addHandler(failure)
    logger.propagate = False


def finding_count(body: bytes | None) -> int:
    """Return how many JSONL findings are in a quarantine body."""
    if not body:
        return 0
    return sum(1 for line in body.splitlines() if line)


def log_stage(stage: str, **fields: int | str) -> None:
    """Log one stage completion. ``fields`` are counts, names, or paths."""
    parts = [f"stage={_token(stage)}"]
    for key in sorted(fields):
        parts.append(f"{_token(key)}={_field(key, fields[key])}")
    logging.getLogger(_LOGGER_NAME).info(" ".join(parts))


def log_failure(exc: BaseException) -> None:
    """Log a failure by type. The message is only kept when it is one line."""
    message = str(exc)
    if "\n" in message or "\r" in message:
        message = ""
    logging.getLogger(_LOGGER_NAME).error(
        "stage=failed error_type=%s message=%s",
        type(exc).__name__,
        message,
    )


def _token(value: str) -> str:
    if not value or any(character.isspace() for character in value):
        msg = "log name must be a single token"
        raise ValueError(msg)
    return value


def _field(key: str, value: object) -> int | str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        msg = f"log field {key} must be a count or a name"
        raise TypeError(msg)
    if isinstance(value, str) and any(character in value for character in "\n\r"):
        msg = f"log field {key} must be a single line"
        raise ValueError(msg)
    return value
