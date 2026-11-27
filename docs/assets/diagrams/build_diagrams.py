"""Build the architecture diagrams used by docs and the report site.

Writes one self-contained HTML file per diagram and skin into this folder:
``lakehouse.html``, ``lakehouse-dark.html``, ``orchestration.html`` and
``orchestration-dark.html``. The PNGs next to them are exports of these files
(``export_png.py`` from the diagram-design skill, scale 2).

Skin: Filipe Oliveira personal brand (``filipeoliveira-hub-assets/brand``),
Urbanist for names and Geist Mono for technical labels. Every fact drawn here
comes from ``docs/architecture/phase-2-lakehouse.md``, the layer notes, and
``docs/architecture/phase-3-orchestration.md``; change those first.

Run: ``python3 docs/assets/diagrams/build_diagrams.py``. After exporting the PNGs,
run it again with ``--record-exports`` so ``exports.sha256`` names the HTML each
PNG was exported from; the site tests fail when the two drift.
"""

# ruff: noqa: E501  (SVG and CSS template strings read better unwrapped)
from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass
from html import escape
from pathlib import Path

HERE = Path(__file__).resolve().parent
SANS = "'Urbanist', system-ui, sans-serif"
MONO = "'Geist Mono', ui-monospace, monospace"
FONTS = (
    "https://fonts.googleapis.com/css2?family=Urbanist:wght@500;700"
    "&amp;family=Geist+Mono:wght@400;500&amp;display=swap"
)


@dataclass(frozen=True)
class Skin:
    slug: str
    paper: str
    paper2: str
    card: str
    ink: str
    muted: str
    soft: str
    accent: str
    accent_tint: str
    accent_band: str
    ink_band: str
    muted_band: str
    card_stroke: str
    outer_stroke: str
    rule: str
    zone_fill: str
    zone_stroke: str


LIGHT = Skin(
    slug="",
    paper="#F3F0F9",
    paper2="#E2D8F6",
    card="#FFFFFF",
    ink="#241A3D",
    muted="#5E5470",
    soft="#6A6080",
    accent="#CC1F63",
    accent_tint="rgba(240,55,127,0.07)",
    accent_band="rgba(204,31,99,0.14)",
    ink_band="rgba(36,26,61,0.06)",
    muted_band="rgba(94,84,112,0.10)",
    card_stroke="#241A3D",
    outer_stroke="#5E5470",
    rule="rgba(36,26,61,0.12)",
    zone_fill="rgba(201,182,240,0.22)",
    zone_stroke="rgba(93,45,190,0.36)",
)

DARK = Skin(
    slug="-dark",
    paper="#120B20",
    paper2="#1A1229",
    card="rgba(241,236,250,0.04)",
    ink="#F1ECFA",
    muted="#B3A8C9",
    soft="#A397BB",
    accent="#FF6FA6",
    accent_tint="rgba(255,111,166,0.10)",
    accent_band="rgba(255,111,166,0.20)",
    ink_band="rgba(241,236,250,0.08)",
    muted_band="rgba(163,151,187,0.16)",
    card_stroke="rgba(241,236,250,0.30)",
    outer_stroke="#B3A8C9",
    rule="rgba(241,236,250,0.12)",
    zone_fill="rgba(132,88,219,0.14)",
    zone_stroke="rgba(201,173,255,0.40)",
)


def text(
    x: float,
    y: float,
    body: str,
    *,
    size: float,
    fill: str,
    family: str = MONO,
    weight: int | None = None,
    anchor: str = "start",
    spacing: str | None = None,
    italic: bool = False,
) -> str:
    attrs = [
        f'x="{x:g}"',
        f'y="{y:g}"',
        f'fill="{fill}"',
        f'font-size="{size:g}"',
        f'font-family="{family}"',
    ]
    if weight:
        attrs.append(f'font-weight="{weight}"')
    if anchor != "start":
        attrs.append(f'text-anchor="{anchor}"')
    if spacing:
        attrs.append(f'letter-spacing="{spacing}"')
    if italic:
        attrs.append('font-style="italic"')
    return f"<text {' '.join(attrs)}>{escape(body)}</text>"


def markers(s: Skin, prefix: str) -> str:
    return (
        "<defs>"
        f'<marker id="{prefix}-arrow" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">'
        f'<polygon points="0 0, 8 3, 0 6" fill="{s.muted}"/></marker>'
        f'<marker id="{prefix}-arrow-accent" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">'
        f'<polygon points="0 0, 8 3, 0 6" fill="{s.accent}"/></marker>'
        "</defs>"
    )


def page(title: str, eyebrow: str, svg: str, s: Skin, width: int) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{escape(title)}</title>
  <link href="{FONTS}" rel="stylesheet">
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: {SANS}; background: {s.paper}; color: {s.ink}; padding: 2rem 1rem; }}
    .frame {{ max-width: {width}px; margin: 0 auto; min-width: 0; }}
    .eyebrow {{ font-family: {MONO}; font-size: 11px; letter-spacing: 0.18em; color: {s.muted}; text-transform: uppercase; }}
    h1 {{ font-size: 28px; font-weight: 700; letter-spacing: -0.02em; margin: 0.25rem 0 1.25rem; }}
    .diagram-container {{ width: 100%; overflow-x: auto; }}
    svg {{ width: 100%; min-width: {width}px; display: block; }}
    @media print {{
      .diagram-container {{ overflow-x: visible; }}
      svg {{ min-width: 0; }}
    }}
  </style>
</head>
<body>
  <div class="frame">
    <p class="eyebrow">{escape(eyebrow)}</p>
    <h1>{escape(title)}</h1>
    <!-- Generated by build_diagrams.py; edit the script, not this file. -->
    <div class="diagram-container">
{svg}
    </div>
  </div>
</body>
</html>
"""


# --------------------------------------------------------------------------- #
# Lakehouse (medallion type, four tiers, Gold focal)
# --------------------------------------------------------------------------- #

TIER_W, TIER_H, TIER_GAP, PAD, TIER_Y = 176, 360, 16, 20, 80
FIELD_W = TIER_W - 32

TIERS = [
    {
        "name": "Raw",
        "bucket": "MinIO · raw/",
        "style": "outer",
        "fields": [
            ("Writer", "Python ingestion package"),
            ("Format", "CSV · JSON · JSONL objects"),
            ("Keeps", "each execution as fetched"),
        ],
        "rules": ["keyed by execution_id", "findings → quarantine/"],
    },
    {
        "name": "Bronze",
        "bucket": "iceberg.bronze",
        "style": "default",
        "fields": [
            ("Writer", "Spark 3.5.8 · MERGE append"),
            ("Format", "Iceberg v2 · STRING columns"),
            ("Keeps", "raw history, duplicates and lineage"),
        ],
        "rules": ["new complete objects", "partitioned by dt"],
    },
    {
        "name": "Silver",
        "bucket": "iceberg.silver",
        "style": "default",
        "fields": [
            ("Writer", "Spark 3.5.8 · atomic replace"),
            ("Format", "Iceberg v2 · typed columns"),
            ("Keeps", "one row per key"),
        ],
        "rules": ["invalid rows excluded", "or flagged by contract"],
    },
    {
        "name": "Gold",
        "bucket": "Gold schemas",
        "style": "focal",
        "fields": [
            ("Writer", "dbt-trino 1.11.0 on Trino 484"),
            ("Format", "Iceberg v2 · facts, dims, marts"),
            ("Keeps", "full refresh of every model"),
        ],
        "rules": ["business marts", "reconciled weekly"],
    },
]
PROMOTIONS = ["MERGE APPEND", "TYPE · DEDUP", "DBT BUILD"]
PATHS = [
    ("GATE", "bronze_checks · silver_checks", "a failed check blocks the next layer"),
    (
        "READ",
        "Trino 484 · Iceberg JDBC catalog",
        "reads every layer · catalog on PostgreSQL",
    ),
]


def lakehouse_svg(s: Skin) -> tuple[str, int]:
    n = len(TIERS)
    width = PAD * 2 + n * TIER_W + (n - 1) * TIER_GAP
    path_y = TIER_Y + TIER_H + 20
    path_h = 60
    height = path_y + path_h + 20
    slug = f"lakehouse{s.slug}"
    out: list[str] = [
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" role="img" '
        f'aria-labelledby="{slug}-title {slug}-desc">',
        f'<title id="{slug}-title">Medallion lakehouse: Raw, Bronze, Silver, Gold</title>',
        f'<desc id="{slug}-desc">Python ingestion writes raw objects to MinIO; Spark appends them '
        "to string-typed Bronze Iceberg tables and rebuilds typed, deduplicated Silver tables; "
        "dbt-trino builds the Gold marts; a failed Bronze or Silver check blocks the next "
        "layer, and Trino reads every layer through the Iceberg JDBC catalog.</desc>",
        markers(s, slug),
        f'<rect width="{width}" height="{height}" fill="{s.paper}"/>',
    ]

    def cx(i: int) -> int:
        return PAD + i * (TIER_W + TIER_GAP) + TIER_W // 2

    # Promotion arcs first, so the cards sit on top of their ends.
    for i, label in enumerate(PROMOTIONS):
        focal = TIERS[i + 1]["style"] == "focal"
        stroke = s.accent if focal else s.muted
        marker = f"{slug}-arrow-accent" if focal else f"{slug}-arrow"
        a, b = cx(i), cx(i + 1)
        out.append(
            f'<path d="M {a},{TIER_Y} C {a},0 {b},0 {b},{TIER_Y}" fill="none" stroke="{stroke}" '
            f'stroke-width="{1.6 if focal else 1.4}" marker-end="url(#{marker})"/>'
        )
        out.append(
            text(
                (a + b) / 2,
                54,
                label,
                size=10,
                fill=stroke,
                anchor="middle",
                spacing="0.08em",
            )
        )

    for i, tier in enumerate(TIERS):
        x = PAD + i * (TIER_W + TIER_GAP)
        c = cx(i)
        style = tier["style"]
        if style == "focal":
            fill, stroke, band, sw, bucket_fill = (
                s.accent_tint,
                s.accent,
                s.accent_band,
                1.6,
                s.accent,
            )
        elif style == "outer":
            fill, stroke, band, sw, bucket_fill = (
                s.card,
                s.outer_stroke,
                s.muted_band,
                1.0,
                s.muted,
            )
        else:
            fill, stroke, band, sw, bucket_fill = (
                s.card,
                s.card_stroke,
                s.ink_band,
                1.0,
                s.muted,
            )
        out.append(
            f'<rect x="{x}" y="{TIER_Y}" width="{TIER_W}" height="{TIER_H}" rx="6" fill="{s.paper}"/>'
        )
        out.append(
            f'<rect x="{x}" y="{TIER_Y}" width="{TIER_W}" height="{TIER_H}" rx="6" fill="{fill}" '
            f'stroke="{stroke}" stroke-width="{sw}"/>'
        )
        out.append(
            f'<path d="M {x + 0.5},{TIER_Y + 6} Q {x + 0.5},{TIER_Y + 0.5} {x + 6},{TIER_Y + 0.5} '
            f"H {x + TIER_W - 6} Q {x + TIER_W - 0.5},{TIER_Y + 0.5} {x + TIER_W - 0.5},{TIER_Y + 6} "
            f'V {TIER_Y + 50} H {x + 0.5} Z" fill="{band}"/>'
        )
        out.append(
            text(
                c,
                TIER_Y + 30,
                tier["name"],
                size=16,
                fill=s.ink,
                family=SANS,
                weight=700,
                anchor="middle",
            )
        )
        out.append(
            text(
                c,
                TIER_Y + 68,
                tier["bucket"],
                size=10.5,
                fill=bucket_fill,
                anchor="middle",
            )
        )
        fy = TIER_Y + 98
        for label, value in tier["fields"]:
            out.append(
                text(x + 16, fy, label, size=12, fill=s.ink, family=SANS, weight=700)
            )
            out.append(
                f'<foreignObject x="{x + 16}" y="{fy + 6}" width="{FIELD_W}" height="32">'
                f'<div xmlns="http://www.w3.org/1999/xhtml" style="font-family: {MONO}; '
                f'font-size: 10.5px; color: {s.muted}; line-height: 1.3;">{escape(value)}</div>'
                "</foreignObject>"
            )
            fy += 56
        out.append(
            f'<line x1="{x + 16}" y1="{TIER_Y + 276}" x2="{x + TIER_W - 16}" '
            f'y2="{TIER_Y + 276}" stroke="{s.rule}" stroke-width="1"/>'
        )
        out.append(
            text(
                x + 16,
                TIER_Y + 298,
                "RULES",
                size=9,
                fill=s.muted,
                weight=500,
                spacing="0.18em",
            )
        )
        rule_fill = s.accent if style == "focal" else s.muted
        for k, line in enumerate(tier["rules"]):
            out.append(
                text(x + 16, TIER_Y + 318 + k * 18, line, size=10, fill=rule_fill)
            )

    path_w = (width - 2 * PAD - 16) // 2
    for k, (tag, title, sub) in enumerate(PATHS):
        px = PAD + k * (path_w + 16)
        out.append(
            f'<rect x="{px}" y="{path_y}" width="{path_w}" height="{path_h}" rx="6" '
            f'fill="{s.card}" stroke="{s.rule}" stroke-width="1"/>'
        )
        out.append(
            f'<rect x="{px + 12}" y="{path_y + 12}" width="44" height="16" rx="2" '
            f'fill="none" stroke="{s.muted}" stroke-opacity="0.6" stroke-width="0.8"/>'
        )
        out.append(
            text(
                px + 34,
                path_y + 24,
                tag,
                size=9,
                fill=s.ink,
                weight=500,
                anchor="middle",
                spacing="0.12em",
            )
        )
        out.append(
            text(
                px + 72,
                path_y + 26,
                title,
                size=13,
                fill=s.ink,
                family=SANS,
                weight=700,
            )
        )
        out.append(text(px + 72, path_y + 44, sub, size=10.5, fill=s.muted))

    out.append("</svg>")
    return "\n".join(out), width


# --------------------------------------------------------------------------- #
# Orchestration (architecture type, nine nodes)
# --------------------------------------------------------------------------- #


def node(
    out: list[str],
    s: Skin,
    x: int,
    y: int,
    w: int,
    h: int,
    tag: str,
    name: str,
    subs: list[str],
    *,
    focal: bool = False,
) -> None:
    fill = s.accent_tint if focal else s.card
    stroke = s.accent if focal else s.card_stroke
    out.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="{s.paper}"/>'
    )
    out.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="{1.6 if focal else 1}"/>'
    )
    tag_w = 12 + len(tag) * 6
    out.append(
        f'<rect x="{x + 8}" y="{y + 8}" width="{tag_w}" height="14" rx="2" fill="none" '
        f'stroke="{stroke}" stroke-opacity="0.5" stroke-width="0.8"/>'
    )
    out.append(
        text(
            x + 8 + tag_w / 2,
            y + 18,
            tag,
            size=8,
            fill=s.accent if focal else s.muted,
            weight=500,
            anchor="middle",
            spacing="0.1em",
        )
    )
    cxn = x + w / 2
    top = y + 42
    out.append(
        text(
            cxn,
            top,
            name,
            size=14,
            fill=s.ink,
            family=SANS,
            weight=700,
            anchor="middle",
        )
    )
    for k, sub in enumerate(subs):
        out.append(
            text(cxn, top + 16 + k * 15, sub, size=10.5, fill=s.muted, anchor="middle")
        )


def label(
    out: list[str],
    s: Skin,
    x: float,
    y: float,
    body: str,
    *,
    accent: bool = False,
    anchor: str = "middle",
) -> None:
    w = len(body) * 6.4 + 10
    left = x - w / 2 if anchor == "middle" else x
    out.append(
        f'<rect x="{left:g}" y="{y - 10:g}" width="{w:g}" height="14" rx="2" fill="{s.paper}"/>'
    )
    tx = x if anchor == "middle" else x + 5
    out.append(
        text(
            tx,
            y + 1,
            body,
            size=10,
            fill=s.accent if accent else s.soft,
            anchor=anchor,
            spacing="0.06em",
        )
    )


DAGS = [
    ("DAG", "Daily extract", ["olist-synth-cdc", "00:00 UTC · [D-1, D)"]),
    ("DAG", "Weekly re-extract", ["olist-synth-cdc", "Mon · [W-8, W-1)"]),
    ("DAG", "API refresh", ["DummyJSON weekly", "IBGE monthly"]),
    ("DAG", "Manual loads", ["Olist · storefront", "fixed release, demo"]),
]


def orchestration_svg(s: Skin) -> tuple[str, int]:
    width, height = 792, 612
    slug = f"orchestration{s.slug}"
    arrow, arrow_accent = f"url(#{slug}-arrow)", f"url(#{slug}-arrow-accent)"
    out: list[str] = [
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" role="img" '
        f'aria-labelledby="{slug}-title {slug}-desc">',
        f'<title id="{slug}-title">Airflow orchestration of the batch path</title>',
        f'<desc id="{slug}-desc">Four groups of ingestion DAGs emit a raw Asset only after a '
        "successful extract; the Asset triggers transform_lakehouse, which promotes Bronze, "
        "Silver and Gold in order; a failed Bronze or Silver check blocks the next layer, and a weekly DAG reconciles Gold.</desc>",
        markers(s, slug),
        f'<rect width="{width}" height="{height}" fill="{s.paper}"/>',
        text(
            20,
            44,
            "INGESTION DAGS · AIRFLOW 3.4.0",
            size=9,
            fill=s.muted,
            weight=500,
            spacing="0.18em",
        ),
    ]
    xs = [20, 212, 404, 596]
    # Zone for the transform DAG, drawn first.
    out.append(
        f'<rect x="20" y="320" width="752" height="128" rx="8" fill="{s.zone_fill}" '
        f'stroke="{s.zone_stroke}" stroke-width="1" stroke-dasharray="5,4"/>'
    )
    out.append(
        text(
            756,
            340,
            "TRANSFORM_LAKEHOUSE · MAX_ACTIVE_RUNS=1 · EVENTS COALESCED",
            size=9,
            fill=s.muted,
            weight=500,
            anchor="end",
            spacing="0.14em",
        )
    )

    # Connectors before nodes. DAG bottoms at y=152 (h=88 from y=64).
    stroke = f'fill="none" stroke="{s.muted}" stroke-width="1.3" marker-end="{arrow}"'
    out.append(
        f'<path d="M 108,152 V 172 Q 108,180 116,180 H 328 Q 336,180 336,188 V 200" {stroke}/>'
    )
    out.append(
        f'<path d="M 300,152 V 160 Q 300,168 308,168 H 368 Q 376,168 376,176 V 200" {stroke}/>'
    )
    out.append(
        f'<path d="M 492,152 V 160 Q 492,168 484,168 H 424 Q 416,168 416,176 V 200" {stroke}/>'
    )
    out.append(
        f'<path d="M 684,152 V 172 Q 684,180 676,180 H 464 Q 456,180 456,188 V 200" {stroke}/>'
    )
    # Raw Asset -> Bronze (headline path).
    out.append(
        f'<path d="M 396,268 V 280 Q 396,288 388,288 H 152 Q 144,288 144,296 V 356" '
        f'fill="none" stroke="{s.accent}" stroke-width="1.6" marker-end="{arrow_accent}"/>'
    )
    label(out, s, 270, 306, "TRIGGERS", accent=True)
    # Gate arrows inside the zone.
    for a, b in ((244, 296), (496, 548)):
        out.append(f'<path d="M {a},390 H {b}" {stroke}/>')
        label(out, s, (a + b) / 2, 374, "PASS")
    # Weekly reconciliation, dashed.
    out.append(
        f'<path d="M 648,480 V 424" fill="none" stroke="{s.muted}" stroke-width="1.3" '
        f'stroke-dasharray="5,4" marker-end="{arrow}"/>'
    )
    label(out, s, 656, 466, "CHECKS", anchor="start")

    for x, (tag, name, subs) in zip(xs, DAGS, strict=True):
        node(out, s, x, 64, 176, 88, tag, name, subs)
    node(
        out,
        s,
        296,
        200,
        200,
        68,
        "ASSET",
        "Raw Asset",
        ["emitted on success only"],
        focal=True,
    )
    node(out, s, 44, 356, 200, 68, "SPARK", "Bronze", ["promote · bronze_checks"])
    node(out, s, 296, 356, 200, 68, "SPARK", "Silver", ["rebuild · silver_checks"])
    node(out, s, 548, 356, 200, 68, "DBT", "Gold", ["stage_quarantine · dbt_build"])
    node(
        out,
        s,
        548,
        480,
        200,
        68,
        "DAG",
        "reconcile_gold",
        ["weekly · lakehouse_gold pool"],
    )

    out.append(
        text(
            44,
            496,
            "A failed check blocks the next layer,",
            size=14,
            fill=s.ink,
            family=SANS,
            weight=500,
            italic=True,
        )
    )
    out.append(
        text(
            44,
            516,
            "also on a later refresh with no new raw objects.",
            size=14,
            fill=s.ink,
            family=SANS,
            weight=500,
            italic=True,
        )
    )

    # Legend strip.
    out.append(
        f'<line x1="20" y1="572" x2="772" y2="572" stroke="{s.rule}" stroke-width="0.8"/>'
    )
    out.append(
        text(20, 596, "LEGEND", size=9, fill=s.muted, weight=500, spacing="0.18em")
    )
    out.append(
        f'<path d="M 100,592 H 136" fill="none" stroke="{s.muted}" stroke-width="1.3" '
        f'marker-end="{arrow}"/>'
    )
    out.append(text(144, 596, "runs data", size=10.5, fill=s.muted))
    out.append(
        f'<path d="M 244,592 H 280" fill="none" stroke="{s.muted}" stroke-width="1.3" '
        f'stroke-dasharray="5,4" marker-end="{arrow}"/>'
    )
    out.append(text(288, 596, "weekly check", size=10.5, fill=s.muted))
    out.append(text(408, 596, "PASS", size=10, fill=s.soft, spacing="0.06em"))
    out.append(
        text(444, 596, "layer check passed for the dataset", size=10.5, fill=s.muted)
    )
    out.append("</svg>")
    return "\n".join(out), width


def main() -> None:
    for skin in (LIGHT, DARK):
        svg, width = lakehouse_svg(skin)
        (HERE / f"lakehouse{skin.slug}.html").write_text(
            page(
                "Medallion lakehouse",
                "Open Data Platform · verified through Phase 3",
                svg,
                skin,
                width,
            ),
            encoding="utf-8",
        )
        svg, width = orchestration_svg(skin)
        (HERE / f"orchestration{skin.slug}.html").write_text(
            page(
                "Airflow orchestration",
                "Open Data Platform · verified through Phase 3",
                svg,
                skin,
                width,
            ),
            encoding="utf-8",
        )


NAMES = ("lakehouse", "lakehouse-dark", "orchestration", "orchestration-dark")


def record_exports() -> None:
    """Write the SHA-256 of each HTML source next to the name of its PNG export."""
    lines = []
    for name in NAMES:
        digest = hashlib.sha256((HERE / f"{name}.html").read_bytes()).hexdigest()
        lines.append(f"{digest}  {name}.png\n")
    (HERE / "exports.sha256").write_text("".join(lines), encoding="utf-8")


if __name__ == "__main__":
    if "--record-exports" in sys.argv[1:]:
        record_exports()
    else:
        main()
