# Single platform runtime (P3-03, ADR-0005 addendum).
#
# One image runs Airflow, Spark, and dbt. The Airflow services and the
# lakehouse `spark` service use it. There is no other Spark or dbt install.
#
# - Airflow: the pinned upstream image, unchanged.
# - Spark 3.5.8 and its Temurin 17 JRE: copied from the official Spark image
#   that Phase 2 validated. That image is only a build stage. No service
#   runs it.
# - dbt-core and dbt-trino: a separate venv so their pins do not touch the
#   Airflow environment. Hashes come from uv.lock (group `dbt`).
FROM spark:3.5.8-java17@sha256:5363cd7d22758958565206f67a764356ebebb9fa8f5721e9c55aa86a78efafda AS spark

FROM apache/airflow:3.4.0-python3.12

USER root

COPY --from=spark /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark /opt/spark

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    ODP_DBT_BIN=/opt/dbt-venv/bin/dbt \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
ENV PATH="${PATH}:/opt/java/openjdk/bin:/opt/spark/bin"

COPY dbt-requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir --require-hashes \
        -r /tmp/dbt-requirements.txt \
    && rm /tmp/dbt-requirements.txt \
    && mkdir -p /opt/odp/work \
    && chown -R airflow:0 /opt/odp/work \
    && chmod -R g+rwX /opt/odp/work

USER airflow
