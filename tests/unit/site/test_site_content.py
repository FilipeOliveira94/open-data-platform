"""Consistency checks between the report site pages and the repository docs (P9-02).

The site states status, versions and decisions that the repository records
elsewhere. These tests fail when a page drifts from its source, so a wrong
"verified" or a stale pin cannot reach the site silently.
"""

from __future__ import annotations

import hashlib
import importlib.util
import re
import struct
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
DOCS_PAGES = ROOT / "site" / "src" / "content" / "docs"
HOME = DOCS_PAGES / "index.mdx"
ARCHITECTURE = DOCS_PAGES / "architecture.mdx"
DIAGRAMS = ROOT / "docs" / "assets" / "diagrams"
REPO_URL = "https://github.com/FilipeOliveira94/open-data-platform/"


def _table_rows(text: str, header_start: str) -> list[list[str]]:
    """Return the body rows of the first table whose header starts with header_start."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith(header_start):
            rows = []
            for row in lines[i + 2 :]:
                if not row.startswith("|"):
                    break
                rows.append(
                    [cell.strip() for cell in row.strip().strip("|").split("|")]
                )
            return rows
    raise AssertionError(f"no table with header starting {header_start!r}")


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    nxt = text.find("\n## ", start + len(heading))
    return text[start : nxt if nxt != -1 else len(text)]


def _plain(cell: str) -> str:
    """Drop Markdown emphasis and links so a status cell compares as text."""
    cell = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", cell)
    return cell.replace("**", "").strip()


def test_home_status_matches_phase_overview() -> None:
    roadmap = (ROOT / "ROADMAP.md").read_text(encoding="utf-8")
    overview = _table_rows(_section(roadmap, "## Phase overview"), "| Phase |")
    expected = {row[0]: _plain(row[-1]) for row in overview}

    home = _table_rows(HOME.read_text(encoding="utf-8"), "| Phase |")
    shown = {row[0]: _plain(row[-1]) for row in home}

    assert shown == expected


def test_architecture_adr_status_matches_decisions() -> None:
    decisions = (ROOT / "DECISIONS.md").read_text(encoding="utf-8")
    status = {}
    for line in decisions.splitlines():
        m = re.match(r"\| (D-\d{3}) \|.*\| `(\w+)` \| \[ADR-", line)
        if m:
            status[m.group(1)] = m.group(2)
    assert status["D-008"] == "proposed"
    assert status["D-014"] == "proposed"

    checked = 0
    for page in (HOME, ARCHITECTURE):
        text = page.read_text(encoding="utf-8")
        for adr, stated in re.findall(
            r"\b(D-\d{3})\b[^\n|`]*`(approved|proposed|superseded)`", text
        ):
            assert status[adr] == stated, (
                f"{page.name}: {adr} is {status[adr]!r}, page says {stated!r}"
            )
            checked += 1
    assert checked >= 10


def test_architecture_versions_match_recorded_pins() -> None:
    lakehouse = (ROOT / "docs" / "architecture" / "phase-2-lakehouse.md").read_text(
        "utf-8"
    )
    decisions = (ROOT / "DECISIONS.md").read_text(encoding="utf-8")
    recorded = _section(lakehouse, "## Versions") + decisions

    # What the stack is built and run from today.
    build = (ROOT / "docker-compose.yml").read_text(encoding="utf-8") + "".join(
        f.read_text(encoding="utf-8", errors="ignore")
        for f in sorted((ROOT / "docker").rglob("*"))
        if f.is_file()
    )

    stack = _table_rows(ARCHITECTURE.read_text(encoding="utf-8"), "| Component |")
    assert stack, "Architecture stack table is empty"
    for row in stack:
        version = row[1]
        for pin in re.findall(r"`([^`]+)`", version):
            assert pin in recorded, f"{row[0]}: pin {pin!r} is not recorded"
            assert pin in build, f"{row[0]}: pin {pin!r} is not in compose or docker/"


def test_site_repo_links_point_to_existing_paths() -> None:
    pattern = re.compile(re.escape(REPO_URL) + r"(?:blob|tree)/main/([^)#\s\"']+)")
    seen = 0
    for page in sorted(DOCS_PAGES.glob("*.md*")):
        for rel in pattern.findall(page.read_text(encoding="utf-8")):
            assert (ROOT / rel).exists(), f"{page.name} links to missing {rel}"
            seen += 1
    assert seen > 0


def test_diagram_figure_sources_exist() -> None:
    page = ARCHITECTURE.read_text(encoding="utf-8")
    assert f"export const repo = '{REPO_URL}blob/main/';" in page
    sources = re.findall(r"source=\{`\$\{repo\}([^`]+)`\}", page)
    assert len(sources) == page.count("<DiagramFigure")
    for rel in sources:
        assert (ROOT / rel).is_file(), f"diagram source {rel} is missing"


def _load_builder():
    spec = importlib.util.spec_from_file_location(
        "build_diagrams", DIAGRAMS / "build_diagrams.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolve the defining module through sys.modules.
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[spec.name]
    return module


@pytest.mark.parametrize("name", ["lakehouse", "orchestration"])
def test_diagram_sources_match_generator(name: str, tmp_path: Path) -> None:
    builder = _load_builder()
    builder.HERE = tmp_path
    builder.main()
    for suffix in ("", "-dark"):
        committed = (DIAGRAMS / f"{name}{suffix}.html").read_text(encoding="utf-8")
        generated = (tmp_path / f"{name}{suffix}.html").read_text(encoding="utf-8")
        assert committed == generated, (
            f"{name}{suffix}.html is stale: rerun build_diagrams.py"
        )
        png = (DIAGRAMS / f"{name}{suffix}.png").read_bytes()
        width, height = struct.unpack(">II", png[16:24])
        view_w, view_h = re.search(r'viewBox="0 0 (\d+) (\d+)"', committed).groups()
        assert (width, height) == (2 * int(view_w), 2 * int(view_h))

    recorded = {}
    for line in (DIAGRAMS / "exports.sha256").read_text(encoding="utf-8").splitlines():
        digest, png_name = line.split()
        recorded[png_name] = digest
    for suffix in ("", "-dark"):
        html = (DIAGRAMS / f"{name}{suffix}.html").read_bytes()
        assert recorded[f"{name}{suffix}.png"] == hashlib.sha256(html).hexdigest(), (
            f"{name}{suffix}.png was exported from another HTML: re-export, then "
            "run build_diagrams.py --record-exports"
        )


def test_architecture_diagrams_come_from_docs() -> None:
    page = ARCHITECTURE.read_text(encoding="utf-8")
    imports = re.findall(r"from '([^']+\.png)'", page)
    assert imports, "Architecture imports no diagram"
    for rel in imports:
        target = (ARCHITECTURE.parent / rel).resolve()
        assert target.parent == DIAGRAMS, f"{rel} is not under docs/assets/diagrams"
        assert target.is_file()

    owners = {
        "lakehouse": ROOT / "docs" / "architecture" / "phase-2-lakehouse.md",
        "orchestration": ROOT / "docs" / "architecture" / "phase-3-orchestration.md",
    }
    for name, doc in owners.items():
        assert f"assets/diagrams/{name}.png" in doc.read_text(encoding="utf-8")
        assert any(f"/{name}.png" in rel for rel in imports)
