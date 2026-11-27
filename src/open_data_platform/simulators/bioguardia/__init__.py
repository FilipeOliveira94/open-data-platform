"""BioGuardia Atlantic synthetic sources (ADR-0027, P11-02 and P11-03).

One deterministic world model drives a daily operations export, a daily
finance export, and a telemetry stream. Domain model:
``docs/architecture/bioguardia-domain.md``.
"""

GENERATOR_VERSION = "1.0.0"
OPS_SOURCE = "bioguardia_ops"
FINANCE_SOURCE = "bioguardia_finance"
TELEMETRY_SOURCE = "bioguardia_telemetry"
