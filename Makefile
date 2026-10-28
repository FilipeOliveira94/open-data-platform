# Open Data Platform — common developer commands
# Requires: uv, Docker Compose v2, and optionally GNU make.

UV ?= uv
COMPOSE ?= docker compose
PYTEST ?= $(UV) run pytest
RUFF ?= $(UV) run ruff

.PHONY: help setup sync lint format test test-unit test-minio test-iceberg-poc up down ps logs clean

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-18s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

setup: sync ## Install Python 3.12 toolchain and package (via uv)
	@echo "Setup complete. Copy .env.example to .env if you need local overrides."

sync: ## Sync dependencies from uv.lock
	$(UV) sync

lint: ## Run Ruff linter
	$(RUFF) check src tests

format: ## Format with Ruff
	$(RUFF) format src tests
	$(RUFF) check --fix src tests

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
	$(UV) run python -c 'import os; from open_data_platform.object_storage import client_from_env, ensure_bucket; ensure_bucket(client_from_env(os.environ), os.environ["ODP_WAREHOUSE_BUCKET"])'; \
	$(COMPOSE) --profile lakehouse run --rm -T spark

up: ## Start PostgreSQL and MinIO
	$(COMPOSE) up -d
	$(COMPOSE) ps

down: ## Stop Compose services (keeps volumes)
	$(COMPOSE) down

ps: ## Show Compose service status
	$(COMPOSE) ps

logs: ## Tail Compose logs
	$(COMPOSE) logs -f --tail=200

clean: ## Remove Python caches (does not delete Docker volumes)
	rm -rf .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} + 2>/dev/null || true
