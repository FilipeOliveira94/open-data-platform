"""Synthetic external systems that feed the platform.

These modules play source systems (an agency's export service, a telemetry
gateway). They write to a landing bucket or a message broker. They never write
platform raw objects, Iceberg tables, or ingestion state; the platform's
ingestion reads what they publish.
"""
