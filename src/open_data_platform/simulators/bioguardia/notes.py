"""Column tables for the source notes, generated from the table specs."""

from __future__ import annotations

from open_data_platform.simulators.bioguardia.catalog import CATALOGS
from open_data_platform.simulators.bioguardia.spec import TableSpec


def table_section(spec: TableSpec) -> str:
    lines = [
        f"### `{spec.name}`",
        "",
        f"{spec.description} Change type: {spec.change_type}. Key: "
        + ", ".join(f"`{k}`" for k in spec.key)
        + f". File: `{spec.file_name}`.",
        "",
        f"Grain: {spec.grain}",
        "",
        "| Column | Type | Nullable | Classification | Description |",
        "| --- | --- | --- | --- | --- |",
    ]
    for column in spec.columns:
        nullable = "yes" if column.nullable else "no"
        lines.append(
            f"| `{column.name}` | {column.type} | {nullable} | "
            f"{column.classification} | {column.description} |"
        )
    return "\n".join(lines)


def column_tables(source: str | None = None) -> str:
    sections = []
    for name, catalog in CATALOGS.items():
        if source is not None and name != source:
            continue
        sections.extend(table_section(spec) for spec in catalog)
    return "\n\n".join(sections)
