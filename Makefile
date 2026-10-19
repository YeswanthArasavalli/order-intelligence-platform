# =============================================================================
# Order Intelligence Platform — Makefile
# =============================================================================
# Usage:
#   make help               Show all available targets
#   make setup              First-time setup (copy .env, generate cluster ID)
#   make up                 Start all services
#   make down               Stop all services
#   make restart            Restart all services
#   make ps                 Show service health status
#   make logs               Tail logs from all services
#   make clean              Remove all containers and named volumes (DESTRUCTIVE)
# =============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Load .env if it exists (for use in targets that need env vars)
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# Colour codes
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
CYAN   := \033[0;36m
RESET  := \033[0m

# =============================================================================
# HELP
# =============================================================================

.PHONY: help
help: ## Show this help message
	@echo ""
	@echo "$(CYAN)Order Intelligence Platform$(RESET)"
	@echo "$(CYAN)============================$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-25s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# =============================================================================
# SETUP — First-time initialisation
# =============================================================================

.PHONY: setup
setup: ## First-time setup: copy .env.example, generate Kafka cluster ID
	@echo "$(CYAN)Running first-time setup...$(RESET)"
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "$(GREEN)✓ Created .env from .env.example$(RESET)"; \
	else \
		echo "$(YELLOW)⚠ .env already exists — skipping copy$(RESET)"; \
	fi
	@$(MAKE) generate-cluster-id
	@echo ""
	@echo "$(GREEN)Setup complete. Next steps:$(RESET)"
	@echo "  1. Review and update .env with your passwords"
	@echo "  2. Run: make up"
	@echo "  3. Run: make verify"

.PHONY: generate-cluster-id
generate-cluster-id: ## Generate a new Kafka KRaft cluster ID and write to .env
	@./scripts/generate_cluster_id.sh

# =============================================================================
# SERVICE LIFECYCLE
# =============================================================================

.PHONY: up
up: ## Start all services in detached mode
	@echo "$(CYAN)Starting Order Intelligence Platform...$(RESET)"
	@if [ -z "$(KAFKA_CLUSTER_ID)" ]; then \
		echo "$(RED)ERROR: KAFKA_CLUSTER_ID is not set in .env$(RESET)"; \
		echo "       Run: make generate-cluster-id"; \
		exit 1; \
	fi
	@if [ -z "$(MINIO_ROOT_PASSWORD)" ]; then \
		echo "$(RED)ERROR: MINIO_ROOT_PASSWORD is not set in .env$(RESET)"; \
		exit 1; \
	fi
	@if [ -z "$(POSTGRES_PASSWORD)" ]; then \
		echo "$(RED)ERROR: POSTGRES_PASSWORD is not set in .env$(RESET)"; \
		exit 1; \
	fi
	@if [ -z "$(GRAFANA_ADMIN_PASSWORD)" ]; then \
		echo "$(RED)ERROR: GRAFANA_ADMIN_PASSWORD is not set in .env$(RESET)"; \
		exit 1; \
	fi
	docker compose up -d
	@echo ""
	@echo "$(GREEN)Services starting. Run 'make wait' to wait for all healthchecks.$(RESET)"
	@echo "Run 'make ps' to check status."

.PHONY: down
down: ## Stop all services (preserves volumes)
	@echo "$(CYAN)Stopping services...$(RESET)"
	docker compose down
	@echo "$(GREEN)✓ All services stopped. Data volumes preserved.$(RESET)"

.PHONY: restart
restart: down up ## Restart all services

.PHONY: rebuild
rebuild: ## Rebuild and restart (use after Dockerfile changes)
	docker compose down
	docker compose build --no-cache
	docker compose up -d

# =============================================================================
# MONITORING
# =============================================================================

.PHONY: ps
ps: ## Show service status with health information
	@echo ""
	@echo "$(CYAN)Service Status$(RESET)"
	@echo "$(CYAN)--------------$(RESET)"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
	@echo ""

.PHONY: health
health: ## Show detailed health status for all services
	@echo ""
	@echo "$(CYAN)Health Check Status$(RESET)"
	@echo "$(CYAN)-------------------$(RESET)"
	@for svc in oip-kafka oip-schema-registry oip-kafka-ui oip-minio oip-postgres oip-redis oip-prometheus oip-grafana; do \
		status=$$(docker inspect --format='{{.State.Health.Status}}' $$svc 2>/dev/null || echo "not found"); \
		if [ "$$status" = "healthy" ]; then \
			printf "  $(GREEN)✓$(RESET) %-30s $(GREEN)%s$(RESET)\n" "$$svc" "$$status"; \
		elif [ "$$status" = "starting" ]; then \
			printf "  $(YELLOW)⏳$(RESET) %-30s $(YELLOW)%s$(RESET)\n" "$$svc" "$$status"; \
		else \
			printf "  $(RED)✗$(RESET) %-30s $(RED)%s$(RESET)\n" "$$svc" "$$status"; \
		fi; \
	done
	@echo ""

.PHONY: wait
wait: ## Wait for all services to become healthy (timeout 120s)
	@./scripts/wait_for_services.sh

.PHONY: logs
logs: ## Tail logs from all services (Ctrl+C to stop)
	docker compose logs -f --tail=50

.PHONY: logs-kafka
logs-kafka: ## Tail Kafka logs only
	docker compose logs -f --tail=50 kafka

# NOTE: there is no `spark` Compose service. The Bronze/Silver/Gold Spark jobs
# run on the host (`python main.py bronze|silver|gold`); inspect their stdout.

# =============================================================================
# VERIFICATION
# =============================================================================

.PHONY: verify
verify: ## Run Phase 1 acceptance criteria verification
	@echo ""
	@echo "$(CYAN)Phase 1 Verification$(RESET)"
	@echo "$(CYAN)====================$(RESET)"
	@echo ""

	@# Kafka
	@printf "  Kafka broker API...        "; \
	if docker exec oip-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then \
		echo "$(GREEN)✓ PASS$(RESET)"; \
	else \
		echo "$(RED)✗ FAIL$(RESET)"; \
	fi

	@# Schema Registry
	@printf "  Schema Registry API...     "; \
	result=$$(curl -sf http://localhost:${HOST_SCHEMA_REGISTRY_PORT:-8081}/subjects 2>/dev/null); \
	if [ "$$result" = "[]" ]; then \
		echo "$(GREEN)✓ PASS (empty registry)$(RESET)"; \
	else \
		echo "$(RED)✗ FAIL (got: $$result)$(RESET)"; \
	fi

	@# Kafka UI
	@printf "  Kafka UI health...         "; \
	if curl -sf http://localhost:${HOST_KAFKA_UI_PORT:-8080}/actuator/health > /dev/null 2>&1; then \
		echo "$(GREEN)✓ PASS$(RESET)"; \
	else \
		echo "$(RED)✗ FAIL$(RESET)"; \
	fi

	@# MinIO
	@printf "  MinIO health...            "; \
	if curl -sf http://localhost:${HOST_MINIO_API_PORT:-9000}/minio/health/live > /dev/null 2>&1; then \
		echo "$(GREEN)✓ PASS$(RESET)"; \
	else \
		echo "$(RED)✗ FAIL$(RESET)"; \
	fi

	@# MinIO buckets
	@printf "  MinIO buckets...           "; \
	buckets=$$(docker exec oip-minio mc alias set local http://localhost:9000 ${MINIO_ROOT_USER:-minioadmin} ${MINIO_ROOT_PASSWORD} > /dev/null 2>&1 && \
		docker run --rm --network oip-network minio/mc ls local/ 2>/dev/null | awk '{print $$5}' | tr '\n' ' '); \
	if curl -sf -u "${MINIO_ROOT_USER:-minioadmin}:${MINIO_ROOT_PASSWORD}" http://localhost:${HOST_MINIO_API_PORT:-9000}/minio/health/live > /dev/null 2>&1; then \
		echo "$(GREEN)✓ PASS$(RESET)"; \
	else \
		echo "$(RED)✗ FAIL$(RESET)"; \
	fi

	@# PostgreSQL
	@printf "  PostgreSQL connection...   "; \
	if docker exec oip-postgres pg_isready -U ${POSTGRES_USER:-oip_user} -d ${POSTGRES_DB:-order_intelligence} > /dev/null 2>&1; then \
		echo "$(GREEN)✓ PASS$(RESET)"; \
	else \
		echo "$(RED)✗ FAIL$(RESET)"; \
	fi

	@# PostgreSQL schemas
	@printf "  PostgreSQL schemas...      "; \
	schema_count=$$(docker exec oip-postgres psql -U ${POSTGRES_USER:-oip_user} -d ${POSTGRES_DB:-order_intelligence} -tAc "SELECT count(*) FROM information_schema.schemata WHERE schema_name IN ('bronze','silver','gold','alerts');" 2>/dev/null); \
	if [ "$$schema_count" = "4" ]; then \
		echo "$(GREEN)✓ PASS (4 schemas found)$(RESET)"; \
	else \
		echo "$(RED)✗ FAIL (found $$schema_count/4 schemas)$(RESET)"; \
	fi

	@# Redis
	@printf "  Redis ping...              "; \
	if docker exec oip-redis redis-cli ping 2>/dev/null | grep -q "PONG"; then \
		echo "$(GREEN)✓ PASS$(RESET)"; \
	else \
		echo "$(RED)✗ FAIL$(RESET)"; \
	fi

	@# Prometheus
	@printf "  Prometheus ready...        "; \
	if curl -sf http://localhost:${HOST_PROMETHEUS_PORT:-9090}/-/ready > /dev/null 2>&1; then \
		echo "$(GREEN)✓ PASS$(RESET)"; \
	else \
		echo "$(RED)✗ FAIL$(RESET)"; \
	fi

	@# Grafana
	@printf "  Grafana health...          "; \
	if curl -sf http://localhost:${HOST_GRAFANA_PORT:-3000}/api/health > /dev/null 2>&1; then \
		echo "$(GREEN)✓ PASS$(RESET)"; \
	else \
		echo "$(RED)✗ FAIL$(RESET)"; \
	fi

	@echo ""
	@echo "$(CYAN)Open in browser:$(RESET)"
	@echo "  Kafka UI      http://localhost:${HOST_KAFKA_UI_PORT:-8080}"
	@echo "  MinIO Console http://localhost:${HOST_MINIO_CONSOLE_PORT:-9001}"
	@echo "  Prometheus    http://localhost:${HOST_PROMETHEUS_PORT:-9090}"
	@echo "  Grafana       http://localhost:${HOST_GRAFANA_PORT:-3000}"
	@echo ""

# =============================================================================
# KAFKA UTILITIES
# =============================================================================

.PHONY: kafka-topics
kafka-topics: ## List all Kafka topics
	docker exec oip-kafka kafka-topics.sh --list --bootstrap-server localhost:9092

.PHONY: kafka-groups
kafka-groups: ## List Kafka consumer groups
	docker exec oip-kafka kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

.PHONY: kafka-shell
kafka-shell: ## Open a shell inside the Kafka container
	docker exec -it oip-kafka /bin/bash

# =============================================================================
# DATABASE UTILITIES
# =============================================================================

.PHONY: db-shell
db-shell: ## Open a psql shell
	docker exec -it oip-postgres psql -U ${POSTGRES_USER:-oip_user} -d ${POSTGRES_DB:-order_intelligence}

.PHONY: db-schemas
db-schemas: ## List all PostgreSQL schemas
	docker exec oip-postgres psql -U ${POSTGRES_USER:-oip_user} -d ${POSTGRES_DB:-order_intelligence} -c "\dn"

.PHONY: db-reset
db-reset: ## Drop and recreate the PostgreSQL volume (DESTRUCTIVE — dev only)
	@echo "$(RED)WARNING: This will destroy all PostgreSQL data.$(RESET)"
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ]
	docker compose stop postgres
	docker volume rm oip-postgres-data || true
	docker compose up -d postgres
	@echo "$(GREEN)✓ PostgreSQL volume reset. Init script will re-run on startup.$(RESET)"

# =============================================================================
# REDIS UTILITIES
# =============================================================================

.PHONY: redis-shell
redis-shell: ## Open a redis-cli shell
	docker exec -it oip-redis redis-cli

.PHONY: redis-flush
redis-flush: ## Flush all Redis keys (dev only)
	docker exec oip-redis redis-cli FLUSHALL

# =============================================================================
# CLEANUP
# =============================================================================

.PHONY: clean
clean: ## Remove all containers, networks, and named volumes (DESTRUCTIVE)
	@echo "$(RED)WARNING: This will destroy ALL data including Kafka, MinIO, and PostgreSQL volumes.$(RESET)"
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ]
	docker compose down -v --remove-orphans
	@echo "$(GREEN)✓ All containers and volumes removed.$(RESET)"
	@echo "$(YELLOW)If you re-run make up, you must re-run make generate-cluster-id first.$(RESET)"

.PHONY: clean-force
clean-force: ## Force remove everything without confirmation prompt (CI use only)
	docker compose down -v --remove-orphans

# =============================================================================
# PYTHON ENVIRONMENT
# =============================================================================

.PHONY: install
install: ## Install Python dependencies
	pip install -r requirements.txt

.PHONY: lint
lint: ## Run linting (ruff + black check)
	ruff check .
	black --check .

.PHONY: format
format: ## Auto-format Python code
	black .
	ruff check --fix .

.PHONY: test
test: ## Run all tests
	pytest tests/ -v --tb=short

.PHONY: test-unit
test-unit: ## Run unit tests only
	pytest tests/unit/ -v --tb=short

.PHONY: test-integration
test-integration: ## Run the four service-dependent test files under tests/unit/ (requires `docker compose up -d postgres redis` + filled .env)
	pytest tests/unit/test_alerts.py tests/unit/test_notifications.py tests/unit/test_postgres_serving.py tests/unit/test_redis_serving.py -v --tb=short
