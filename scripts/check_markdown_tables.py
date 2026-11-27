"""Fail if a tracked Markdown file has a table with an empty header row.

GFM always renders the header row, so `| | |` shows up as a blank row on top
of the table. Give every table a header that names its columns. Fenced code
blocks are skipped.

Usage: python3 scripts/check_markdown_tables.py
"""

import re
import subprocess
import sys
from pathlib import Path

EMPTY_ROW = re.compile(r"^\s*\|(\s*\|)+\s*$")
SEPARATOR = re.compile(r"^\s*\|(\s*:?-{3,}:?\s*\|)+\s*$")
FENCE = re.compile(r"^\s*(```|~~~)")


def main() -> int:
    files = subprocess.run(
        ["git", "ls-files", "*.md"], capture_output=True, text=True, check=True
    ).stdout.split()
    failures = []
    for path in files:
        lines = Path(path).read_text(encoding="utf-8").split("\n")
        fenced = False
        for i, line in enumerate(lines):
            if FENCE.match(line):
                fenced = not fenced
            nxt = lines[i + 1] if i + 1 < len(lines) else ""
            if not fenced and EMPTY_ROW.match(line) and SEPARATOR.match(nxt):
                failures.append(f"{path}:{i + 1}")
    print(
        f"markdown tables: {len(files)} files, {len(failures)} with an empty header row"
    )
    for failure in failures:
        print(f"  {failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
