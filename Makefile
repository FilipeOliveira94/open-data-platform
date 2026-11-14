# Open Data Platform — common developer commands
# Requires: uv, Docker Compose v2, and optionally GNU make.

UV ?= uv
COMPOSE ?= docker compose
PYTEST ?= $(UV) run pytest
RUFF ?= $(UV) run ruff

.PHONY: help setup sync lint format test test-unit test-minio test-iceberg-poc test-catalog-probe spark-ivy-warm live-bronze live-silver stage-quarantine dbt-parse dbt-build dbt-test-reconciliation live-gold runtime-build up airflow-up down ps logs clean

# One medallion step in the shared runtime image (Spark + dbt, P3-03).
# Make and the Airflow transform DAGs call the same medallion_build module.
define medallion_step
$(COMPOSE) --profile lakehouse run --rm -T spark python -m open_data_platform.lakehouse.medallion_build $(1) --run-label make
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

lint: ## Run Ruff linter
	$(RUFF) check src tests airflow docker/airflow

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

test-iceberg-poc: ## Create the warehouse bucket and run the Iceberg probe
	@set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(ensure_spark_ivy); \
	$(UV) run python -c 'import os; from open_data_platform.object_storage import client_from_env, ensure_bucket; ensure_bucket(client_from_env(os.environ), os.environ["ODP_WAREHOUSE_BUCKET"])'; \
	$(COMPOSE) --profile lakehouse run --rm -T spark /opt/odp/spark/run_poc.sh

test-catalog-probe: ## Spark JDBC catalog write and Trino read (profile lakehouse)
	@set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(ensure_spark_ivy); \
	db="$${ICEBERG_CATALOG_DB:-iceberg}"; \
	case "$$db" in \
		*[!a-z0-9_]*|"") echo "ICEBERG_CATALOG_DB must be a simple identifier" >&2; exit 1;; \
	esac; \
	$(UV) run python -c 'import os; from open_data_platform.object_storage import client_from_env, ensure_bucket; ensure_bucket(client_from_env(os.environ), os.environ["ODP_WAREHOUSE_BUCKET"])'; \
	$(COMPOSE) up -d --wait postgres minio; \
	exists=$$($(COMPOSE) exec -T postgres psql -U "$$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$$db'"); \
	if [ "$$exists" != "1" ]; then \
		$(COMPOSE) exec -T postgres psql -U "$$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $$db"; \
	fi; \
	$(COMPOSE) --profile lakehouse run --rm -T spark /opt/odp/spark/run_catalog_probe.sh; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(COMPOSE) --profile lakehouse exec -T trino trino --execute "SELECT COUNT(*) AS row_count FROM iceberg.catalog_probe.probe"

spark-ivy-warm: ## Download pinned Spark lakehouse jars into .local/spark-ivy (P2-08)
	@set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(ensure_spark_ivy); \
	$(COMPOSE) up -d --wait postgres minio; \
	$(COMPOSE) --profile lakehouse run --rm -T spark /opt/odp/spark/warm_ivy.sh

live-bronze: ## Full-data Bronze from ODP_RAW_BUCKET, then Trino count checks (runtime image)
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

live-silver: ## Full-data Silver from existing Bronze tables, then Trino checks (runtime image)
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

live-gold: stage-quarantine dbt-build ## Stage quarantine sidecars then build Gold models

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
