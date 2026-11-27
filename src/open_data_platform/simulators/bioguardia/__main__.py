"""Command line: ``python -m open_data_platform.simulators.bioguardia``.

Subcommands:

- ``export generate``: publish the drops of a date range.
- ``export serve``: the daily release loop (the Compose service).
- ``columns``: print the column tables of the source notes.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

from open_data_platform.simulators.bioguardia.clock import local_date_of
from open_data_platform.simulators.bioguardia.world import WorldConfig


def _date(text: str) -> date:
    return date.fromisoformat(text)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bioguardia", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    export = commands.add_parser("export", help="daily ops and finance drops")
    export_commands = export.add_subparsers(dest="action", required=True)
    generate = export_commands.add_parser("generate", help="publish a date range")
    generate.add_argument(
        "--start", type=_date, required=True, help="first export date"
    )
    generate.add_argument("--end", type=_date, required=True, help="last export date")
    generate.add_argument("--source", choices=("ops", "finance", "all"), default="all")
    generate.add_argument(
        "--allow-future",
        action="store_true",
        help="write drops before their release time",
    )
    generate.add_argument(
        "--overwrite",
        action="store_true",
        help="regenerate drops that already have _SUCCESS",
    )
    generate.add_argument("--workers", type=int, default=1)
    generate.add_argument(
        "--target-dir",
        type=Path,
        help="write to a local directory instead of the buckets",
    )
    serve = export_commands.add_parser("serve", help="release loop (Compose service)")
    serve.add_argument(
        "--interval", type=int, default=300, help="seconds between checks"
    )
    serve.add_argument("--once", action="store_true", help="check once and exit")
    serve.add_argument("--target-dir", type=Path)
    serve.add_argument(
        "--heartbeat", type=Path, default=Path("/tmp/bioguardia-export.heartbeat")
    )

    commands.add_parser("columns", help="print the source-note column tables")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    env = dict(os.environ)
    if args.command == "columns":
        from open_data_platform.simulators.bioguardia.notes import column_tables

        print(column_tables())
        return 0
    config = WorldConfig.from_env(env)
    if args.command == "export":
        from open_data_platform.simulators.bioguardia import export

        options = export.settings(env)
        destination = export.Destination(
            directory=args.target_dir,
            landing_bucket=options["landing_bucket"],
            evaluation_bucket=options["evaluation_bucket"],
        )
        if args.action == "generate":
            if args.end < args.start:
                print("--end is before --start", file=sys.stderr)
                return 2
            published = export.generate_range(
                config,
                export.SOURCES[args.source],
                args.start,
                args.end,
                destination,
                env,
                allow_future=args.allow_future,
                overwrite=args.overwrite,
                workers=max(1, args.workers),
                max_bytes=options["max_bytes"],
                work_dir=options["work_dir"],
            )
            return 0 if published >= 0 else 2
        import time

        history = options["history_start"] or (
            local_date_of(int(time.time())) - timedelta(days=30)
        )
        export.serve(
            config,
            destination,
            env,
            history_start=history,
            interval=args.interval,
            max_bytes=options["max_bytes"],
            work_dir=options["work_dir"],
            heartbeat=args.heartbeat,
            once=args.once,
        )
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
