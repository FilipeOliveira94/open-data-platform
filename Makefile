# Open Data Platform — common developer commands
# Requires: uv, Docker Compose v2, and optionally GNU make.

UV ?= uv
COMPOSE ?= docker compose
PYTEST ?= $(UV) run pytest
RUFF ?= $(UV) run ruff

.PHONY: help setup sync lint format test test-unit test-minio spark-ivy-warm live-bronze live-silver live-refresh live-rebuild coverage-report stage-quarantine dbt-parse dbt-build dbt-test-reconciliation dbt-unit live-gold runtime-build up airflow-up down ps logs clean

# One run label per make invocation (ADR-0024). The work directory and the
# staging keys carry it, so a make run and an Airflow run never share staging.
RUN_LABEL := make-$(shell date -u +%Y%m%dT%H%M%SZ)
# Empty: incremental refresh. live-rebuild passes --full-rebuild.
REFRESH_FLAGS ?=

# One medallion step in the shared runtime image (Spark + dbt, P3-03).
# Make and the Airflow transform DAGs call the same medallion_build module.
define medallion_step
$(COMPOSE) --profile lakehouse run --rm -T spark python -m open_data_platform.lakehouse.medallion_build $(1) --run-label $(RUN_LABEL) $(REFRESH_FLAGS)
endef

# Host path mounted at /tmp/.ivy2 in the Spark service (P2-08).
SPARK_IVY_DIR ?= .local/spark-ivy

# The runtime image runs as uid 50000 (earlier Spark image: uid 185); the bind
# mount must be writable by that user.
# One line: a newline in this macro starts a new Make shell and drops the
# environment sourced in the same recipe. chmod may fail on jars created by
# uid 185; those files stay readable and must not abort `set -e`.
define ensure_spark_ivy
	mkdir -p "$(SPARK_IVY_DIR)/cache" "$(SPARK_IVY_DIR)/jars" "$(SPARK_IVY_DIR)/local"; chmod -R a+rwX "$(SPARK_IVY_DIR)" || true
endef

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-22s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

setup: sync ## Install Python 3.12 toolchain and package (via uv)
	@echo "Setup complete. Copy .env.example to .env if you need local overrides."

sync: ## Sync dependencies from uv.lock
	$(UV) sync

lint: ## Run Ruff linter and check Markdown tables for an empty header
	$(RUFF) check src tests airflow docker/airflow
	python3 scripts/check_markdown_tables.py

format: ## Format with Ruff
	$(RUFF) format src tests airflow docker/airflow
	$(RUFF) check --fix src tests airflow docker/airflow

test: ## Run unit tests (no Compose services required)
	$(PYTEST) tests/unit

test-unit: test ## Alias for test

test-minio: ## MinIO write/read smoke (Compose MinIO must be healthy)
	@set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(PYTEST) tests/integration/infrastructure/test_minio_smoke.py

spark-ivy-warm: ## Download pinned Spark lakehouse jars into .local/spark-ivy (P2-08)
	@set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(ensure_spark_ivy); \
	$(COMPOSE) up -d --wait postgres minio; \
	$(COMPOSE) --profile lakehouse run --rm -T spark /opt/odp/spark/warm_ivy.sh

live-bronze: ## Promote new raw objects into Bronze, then Trino count checks (runtime image)
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(ensure_spark_ivy); \
	$(COMPOSE) up -d --wait postgres minio; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(call medallion_step,bronze); \
	$(call medallion_step,bronze-checks)

live-silver: ## Rebuild the Silver tables pending after Bronze, then Trino checks (runtime image)
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(ensure_spark_ivy); \
	$(COMPOSE) up -d --wait postgres minio; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(call medallion_step,silver); \
	$(call medallion_step,silver-checks)

stage-quarantine: ## Stage MinIO quarantine JSONL into iceberg.gold_staging.quarantine_findings
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(ensure_spark_ivy); \
	$(COMPOSE) up -d --wait postgres minio; \
	$(call medallion_step,stage-quarantine)

dbt-parse: ## Parse the Gold dbt project in the runtime image
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(call medallion_step,dbt-parse)

dbt-build: ## Build Gold models and schema tests (excludes reconciliation)
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(call medallion_step,dbt-build)

dbt-test-reconciliation: ## Run reconciliation tests (separate from live-gold; weekly reconcile_gold DAG)
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(call medallion_step,dbt-reconcile)

dbt-unit: ## Run dbt unit tests on fixtures (needs Trino and built parents; outside dbt-build)
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(call medallion_step,dbt-unit)

live-gold: stage-quarantine dbt-build ## Stage quarantine sidecars then build Gold models (while Gold is pending)

live-refresh: ## Incremental refresh, the same steps as transform_lakehouse (ADR-0024)
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(ensure_spark_ivy); \
	$(COMPOSE) up -d --wait postgres minio; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(call medallion_step,bronze); \
	$(call medallion_step,bronze-checks); \
	$(call medallion_step,silver); \
	$(call medallion_step,silver-checks); \
	$(call medallion_step,stage-quarantine); \
	$(call medallion_step,dbt-build)

live-rebuild: ## Rebuild every table from all complete raw objects (contract change or recovery; never scheduled)
	$(MAKE) live-refresh REFRESH_FLAGS=--full-rebuild

# daily or weekly. Exit status 1 when the report finds a gap or an overlap.
FREQUENCY ?= daily
coverage-report: ## Gap and overlap report of the PostgreSQL extract windows (FREQUENCY=daily|weekly, P3-06)
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(COMPOSE) up -d --wait postgres; \
	$(COMPOSE) --profile lakehouse run --rm -T --no-deps spark python -m open_data_platform.ingestion.coverage report --frequency $(FREQUENCY)

runtime-build: ## Build the shared runtime image (Airflow + Spark + dbt)
	$(COMPOSE) --profile lakehouse build spark

up: ## Start PostgreSQL and MinIO
	$(COMPOSE) up -d
	$(COMPOSE) ps

airflow-up: ## Create the Airflow log bucket and start the orchestration profile
	@set -euo pipefail; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(COMPOSE) up -d --wait postgres minio; \
	$(UV) run python -c 'import os; from open_data_platform.object_storage import client_from_env, ensure_bucket; ensure_bucket(client_from_env(os.environ), os.environ["ODP_AIRFLOW_LOG_BUCKET"])'; \
	$(COMPOSE) --profile orchestration up -d --build --wait airflow-apiserver airflow-scheduler airflow-dag-processor

down: ## Stop Compose services (keeps volumes)
	$(COMPOSE) down

ps: ## Show Compose service status
	$(COMPOSE) ps

logs: ## Tail Compose logs
	$(COMPOSE) logs -f --tail=200

clean: ## Remove Python caches (does not delete Docker volumes)
	rm -rf .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} + 2>/dev/null || true
