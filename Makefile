# Open Data Platform — common developer commands
# Requires: uv, Docker Compose v2, and optionally GNU make.

UV ?= uv
COMPOSE ?= docker compose
PYTEST ?= $(UV) run pytest
RUFF ?= $(UV) run ruff

.PHONY: help setup sync lint format test test-unit test-minio test-iceberg-poc test-catalog-probe spark-ivy-warm live-bronze live-silver stage-quarantine dbt-parse dbt-build dbt-test-reconciliation live-gold up airflow-up down ps logs clean
DBT ?= $(UV) run dbt
DBT_PROJECT_DIR ?= dbt
DBT_PROFILES_DIR ?= dbt

# Host path mounted at /tmp/.ivy2 in the Spark service (P2-08).
SPARK_IVY_DIR ?= .local/spark-ivy

# Spark image runs as uid 185; the bind mount must be writable by that user.
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
	$(COMPOSE) --profile lakehouse run --rm -T spark /opt/odp/run_poc.sh

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
	$(COMPOSE) --profile lakehouse run --rm -T spark /opt/odp/run_catalog_probe.sh; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(COMPOSE) --profile lakehouse exec -T trino trino --execute "SELECT COUNT(*) AS row_count FROM iceberg.catalog_probe.probe"

spark-ivy-warm: ## Download pinned Spark lakehouse jars into .local/spark-ivy (P2-08)
	@set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(ensure_spark_ivy); \
	$(COMPOSE) up -d --wait postgres minio; \
	$(COMPOSE) --profile lakehouse run --rm -T spark /opt/odp/warm_ivy.sh

live-bronze: ## Full-data Bronze live run from ODP_RAW_BUCKET (default odp-raw)
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	mkdir -p .local; \
	$(ensure_spark_ivy); \
	db="$${ICEBERG_CATALOG_DB:-iceberg}"; \
	case "$$db" in \
		*[!a-z0-9_]*|"") echo "ICEBERG_CATALOG_DB must be a simple identifier" >&2; exit 1;; \
	esac; \
	$(COMPOSE) up -d --wait postgres minio; \
	exists=$$($(COMPOSE) exec -T postgres psql -U "$$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$$db'"); \
	if [ "$$exists" != "1" ]; then \
		$(COMPOSE) exec -T postgres psql -U "$$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $$db"; \
	fi; \
	rm -rf .local/bronze-sql; \
	$(UV) run python -m open_data_platform.lakehouse.bronze --all --sql-out .local/bronze-sql --counts-out .local/bronze-source-counts.tsv; \
	for sql in .local/bronze-sql/*.sql; do \
		echo "Spark Bronze: $$sql"; \
		$(COMPOSE) --profile lakehouse run --rm -T \
			-v "$(CURDIR)/$$sql:/tmp/bronze.sql:ro" \
			-e ODP_SPARK_SQL=/tmp/bronze.sql \
			spark /opt/odp/run_bronze.sh; \
	done; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(UV) run python -m open_data_platform.lakehouse.bronze --print-live-checks --counts-file .local/bronze-source-counts.tsv > .local/bronze-checks.tsv; \
	while IFS=$$(printf '\t') read -r expected sql; do \
		out=$$($(COMPOSE) --profile lakehouse exec -T trino trino --execute "$$sql" </dev/null); \
		got=$$(printf '%s\n' "$$out" | awk 'NF { line = $$0 } END { gsub(/[" \r]/, "", line); print line }'); \
		if [ "$$got" != "$$expected" ]; then \
			echo "Bronze live check failed" >&2; \
			echo "$$sql" >&2; \
			echo "expected $$expected" >&2; \
			printf '%s\n' "$$out" >&2; \
			exit 1; \
		fi; \
		echo "$$sql -> $$got"; \
	done < .local/bronze-checks.tsv

live-silver: ## Full-data Silver live run from existing Bronze tables
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	mkdir -p .local; \
	$(ensure_spark_ivy); \
	db="$${ICEBERG_CATALOG_DB:-iceberg}"; \
	case "$$db" in \
		*[!a-z0-9_]*|"") echo "ICEBERG_CATALOG_DB must be a simple identifier" >&2; exit 1;; \
	esac; \
	$(COMPOSE) up -d --wait postgres minio; \
	exists=$$($(COMPOSE) exec -T postgres psql -U "$$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$$db'"); \
	if [ "$$exists" != "1" ]; then \
		$(COMPOSE) exec -T postgres psql -U "$$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $$db"; \
	fi; \
	rm -rf .local/silver-sql; \
	$(UV) run python -m open_data_platform.lakehouse.silver --all --sql-out .local/silver-sql; \
	for sql in .local/silver-sql/*.sql; do \
		echo "Spark Silver: $$sql"; \
		$(COMPOSE) --profile lakehouse run --rm -T \
			-v "$(CURDIR)/$$sql:/tmp/silver.sql:ro" \
			-e ODP_SPARK_SQL=/tmp/silver.sql \
			spark /opt/odp/run_silver.sh; \
	done; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(UV) run python -m open_data_platform.lakehouse.silver --print-live-checks > .local/silver-checks.tsv; \
	while IFS=$$(printf '\t') read -r expected sql; do \
		out=$$($(COMPOSE) --profile lakehouse exec -T trino trino --execute "$$sql" </dev/null); \
		got=$$(printf '%s\n' "$$out" | awk 'NF { line = $$0 } END { gsub(/[" \r]/, "", line); print line }'); \
		if [ "$$got" != "$$expected" ]; then \
			echo "Silver live check failed" >&2; \
			echo "$$sql" >&2; \
			echo "expected $$expected" >&2; \
			printf '%s\n' "$$out" >&2; \
			exit 1; \
		fi; \
		echo "$$sql -> $$got"; \
	done < .local/silver-checks.tsv

stage-quarantine: ## Stage MinIO quarantine JSONL into iceberg.gold_staging.quarantine_findings
	@set -e; \
	set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	mkdir -p .local; \
	$(ensure_spark_ivy); \
	db="$${ICEBERG_CATALOG_DB:-iceberg}"; \
	case "$$db" in \
		*[!a-z0-9_]*|"") echo "ICEBERG_CATALOG_DB must be a simple identifier" >&2; exit 1;; \
	esac; \
	$(COMPOSE) up -d --wait postgres minio; \
	exists=$$($(COMPOSE) exec -T postgres psql -U "$$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$$db'"); \
	if [ "$$exists" != "1" ]; then \
		$(COMPOSE) exec -T postgres psql -U "$$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $$db"; \
	fi; \
	rm -rf .local/quarantine-sql; \
	$(UV) run python -m open_data_platform.lakehouse.quarantine_stage --sql-out .local/quarantine-sql; \
	echo "Spark quarantine staging"; \
	$(COMPOSE) --profile lakehouse run --rm -T \
		-v "$(CURDIR)/.local/quarantine-sql/quarantine_findings.sql:/tmp/quarantine.sql:ro" \
		-e ODP_SPARK_SQL=/tmp/quarantine.sql \
		spark /opt/odp/run_bronze.sh

dbt-parse: ## Parse the Gold dbt project (Trino profile required)
	@set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(DBT) parse --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-build: ## Build Gold models and schema tests (excludes reconciliation)
	@set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(DBT) build --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR) --exclude tag:reconciliation

dbt-test-reconciliation: ## Run reconciliation tests (separate from live-gold; daily cadence)
	@set -a; \
	[ -f .env.example ] && . ./.env.example; \
	[ -f .env ] && . ./.env; \
	set +a; \
	$(COMPOSE) --profile lakehouse up -d --wait trino; \
	$(DBT) test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR) --select tag:reconciliation

live-gold: stage-quarantine dbt-build ## Stage quarantine sidecars then build Gold models

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
	$(COMPOSE) --profile orchestration up -d --wait airflow-apiserver airflow-scheduler airflow-dag-processor

down: ## Stop Compose services (keeps volumes)
	$(COMPOSE) down

ps: ## Show Compose service status
	$(COMPOSE) ps

logs: ## Tail Compose logs
	$(COMPOSE) logs -f --tail=200

clean: ## Remove Python caches (does not delete Docker volumes)
	rm -rf .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} + 2>/dev/null || true
