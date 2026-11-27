# BioGuardia source simulators (P11-02, P11-03; ADR-0027).
#
# One small image runs the agency's export service and the telemetry gateway
# producer. These play external systems: they write to the landing bucket and
# to Kafka, never to platform raw objects or tables. The package source is
# mounted read-only at /opt/odp/repo/src by Compose, like the runtime image.
#
# Dependencies come from uv.lock (project dependencies plus the `sources`
# group) with hashes: docker/sources/requirements.txt is exported by
# `make sources-requirements`.
FROM python:3.12.15-slim-bookworm@sha256:6a0e06f5ba0b373d051aabd3fffb9b873f27f9a2a3a14bcbe37dee6f0e3bbc70

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/odp/repo/src

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt \
    && useradd --uid 50001 --create-home --shell /usr/sbin/nologin bioguardia \
    && mkdir -p /var/lib/bioguardia \
    && chown bioguardia /var/lib/bioguardia

USER bioguardia
WORKDIR /opt/odp/repo
ENTRYPOINT ["python", "-m", "open_data_platform.simulators.bioguardia"]
