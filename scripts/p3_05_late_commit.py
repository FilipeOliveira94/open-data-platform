"""P3-05 live check helper: plant a late commit and count olist-synth windows.

Run from the repository root with the PostgreSQL settings in the
environment (``.env`` or ``.env.example``):

    uv run python scripts/p3_05_late_commit.py plant <updated_at>
    uv run python scripts/p3_05_late_commit.py count <window_start> <window_end>

``plant`` commits one synthetic seller whose ``updated_at`` is the given
instant, now, after the daily window that holds it was extracted: a late
commit. The seller id is a new UUID on every call. Values go to PostgreSQL
as bound parameters.

``count`` prints ``<dataset>\\t<rows>`` for each olist-synth table, counting rows
whose ``updated_at`` falls in ``[window_start, window_end)``.

Prints counts only, never record values.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime

from psycopg import sql

from open_data_platform.ingestion.olist_synth_cdc import connect_from_env

DATASETS = ("sellers", "customers", "orders", "order_items")


def _instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        msg = "instant must include a timezone"
        raise ValueError(msg)
    return parsed


def plant(updated_at: datetime) -> None:
    with connect_from_env(os.environ) as connection, connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO olist_synth.sellers (seller_id, seller_zip_code_prefix, "
            "seller_city, seller_state, updated_at) VALUES (%s, %s, %s, %s, %s)",
            (f"p3-05-late-{uuid.uuid4()}", 1000, "sao paulo", "SP", updated_at),
        )
        connection.commit()
    print("planted\t1")


def count(start: datetime, end: datetime) -> None:
    with connect_from_env(os.environ) as connection, connection.cursor() as cursor:
        for dataset in DATASETS:
            query = sql.SQL(
                "SELECT count(*) FROM olist_synth.{} "
                "WHERE updated_at >= %s AND updated_at < %s"
            ).format(sql.Identifier(dataset))
            cursor.execute(query, (start, end))
            row = cursor.fetchone()
            print(f"{dataset}\t{row[0] if row else 0}")


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[0] == "plant":
        plant(_instant(argv[1]))
        return 0
    if len(argv) == 3 and argv[0] == "count":
        count(_instant(argv[1]), _instant(argv[2]))
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
