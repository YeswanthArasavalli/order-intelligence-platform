#!/usr/bin/env bash
# =============================================================================
# wait_for_services.sh
# Waits for all Phase 1 Docker services to reach healthy status.
#
# Usage:
#   ./scripts/wait_for_services.sh
#   make wait
#
# Exits 0 if all services become healthy within the timeout.
# Exits 1 if any service fails or the timeout is reached.
# =============================================================================

set -euo pipefail

# Configuration
TIMEOUT_SECONDS="${WAIT_TIMEOUT:-120}"
POLL_INTERVAL=5

# Colour codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

# Services that must be healthy (container names)
SERVICES=(
    "oip-kafka"
    "oip-schema-registry"
    "oip-kafka-ui"
    "oip-minio"
    "oip-postgres"
    "oip-redis"
    "oip-prometheus"
    "oip-grafana"
)

# ---------------------------------------------------------------------------
# Helper: get container health status
# ---------------------------------------------------------------------------
get_health() {
    local container="$1"
    docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "not_found"
}

# ---------------------------------------------------------------------------
# Helper: check if a container exists
# ---------------------------------------------------------------------------
container_exists() {
    docker inspect "$1" &>/dev/null 2>&1
    return $?
}

# ---------------------------------------------------------------------------
# Main wait loop
# ---------------------------------------------------------------------------
echo ""
echo -e "${CYAN}Waiting for all services to become healthy...${RESET}"
echo -e "${CYAN}Timeout: ${TIMEOUT_SECONDS}s | Poll: ${POLL_INTERVAL}s${RESET}"
echo ""

START_TIME=$(date +%s)
ALL_HEALTHY=false

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    if [ "$ELAPSED" -ge "$TIMEOUT_SECONDS" ]; then
        echo ""
        echo -e "${RED}TIMEOUT reached after ${TIMEOUT_SECONDS}s.${RESET}"
        echo ""
        break
    fi

    HEALTHY_COUNT=0
    TOTAL_COUNT=${#SERVICES[@]}

    # Print status line for each service
    printf "\r  "
    ALL_DONE=true

    for svc in "${SERVICES[@]}"; do
        status=$(get_health "$svc")
        if [ "$status" = "healthy" ]; then
            HEALTHY_COUNT=$((HEALTHY_COUNT + 1))
        elif [ "$status" = "unhealthy" ] || [ "$status" = "not_found" ]; then
            ALL_DONE=false
        else
            # starting
            ALL_DONE=false
        fi
    done

    # Clear and reprint status
    echo -ne "\r\033[K"
    echo -ne "  [${ELAPSED}s / ${TIMEOUT_SECONDS}s]  ${HEALTHY_COUNT}/${TOTAL_COUNT} healthy  "

    if [ "$HEALTHY_COUNT" -eq "$TOTAL_COUNT" ]; then
        ALL_HEALTHY=true
        break
    fi

    sleep "$POLL_INTERVAL"
done

echo ""
echo ""

# ---------------------------------------------------------------------------
# Final status report
# ---------------------------------------------------------------------------
echo -e "${CYAN}Service Health Report${RESET}"
echo -e "${CYAN}---------------------${RESET}"

FINAL_HEALTHY=0
FINAL_FAILED=0

for svc in "${SERVICES[@]}"; do
    status=$(get_health "$svc")
    case "$status" in
        healthy)
            printf "  ${GREEN}✓${RESET} %-35s ${GREEN}healthy${RESET}\n" "$svc"
            FINAL_HEALTHY=$((FINAL_HEALTHY + 1))
            ;;
        unhealthy)
            printf "  ${RED}✗${RESET} %-35s ${RED}unhealthy${RESET}\n" "$svc"
            FINAL_FAILED=$((FINAL_FAILED + 1))
            ;;
        starting)
            printf "  ${YELLOW}⏳${RESET} %-35s ${YELLOW}still starting${RESET}\n" "$svc"
            FINAL_FAILED=$((FINAL_FAILED + 1))
            ;;
        not_found)
            printf "  ${RED}✗${RESET} %-35s ${RED}not found (did you run make up?)${RESET}\n" "$svc"
            FINAL_FAILED=$((FINAL_FAILED + 1))
            ;;
        *)
            printf "  ${YELLOW}?${RESET} %-35s ${YELLOW}%s${RESET}\n" "$svc" "$status"
            FINAL_FAILED=$((FINAL_FAILED + 1))
            ;;
    esac
done

echo ""
echo -e "  Total:   ${#SERVICES[@]}"
echo -e "  ${GREEN}Healthy: ${FINAL_HEALTHY}${RESET}"

if [ "$FINAL_FAILED" -gt 0 ]; then
    echo -e "  ${RED}Failed:  ${FINAL_FAILED}${RESET}"
fi

echo ""

# ---------------------------------------------------------------------------
# Exit and troubleshooting hints
# ---------------------------------------------------------------------------
if [ "$ALL_HEALTHY" = true ]; then
    echo -e "${GREEN}✓ All services are healthy. Platform is ready.${RESET}"
    echo ""
    echo "  Next step: run 'make verify' to run full acceptance criteria."
    echo ""
    exit 0
else
    echo -e "${RED}✗ Not all services are healthy.${RESET}"
    echo ""
    echo -e "${YELLOW}Troubleshooting:${RESET}"
    echo ""
    echo "  1. Check logs for failing services:"
    echo "     docker compose logs <service-name>"
    echo ""
    echo "  2. Common issues:"
    echo "     - oip-kafka:           KAFKA_CLUSTER_ID mismatch (run: make clean && make generate-cluster-id && make up)"
    echo "     - oip-schema-registry: Kafka not ready yet — try waiting another 30s"
    echo "     - oip-minio:           Port 9000 conflict — change HOST_MINIO_API_PORT in .env"
    echo "     - oip-postgres:        Port 5432 conflict — change HOST_POSTGRES_PORT in .env"
    echo "     - oip-redis:           Port 6379 conflict — change HOST_REDIS_PORT in .env"
    echo ""
    echo "  3. Full reset (destroys all data):"
    echo "     make clean && make generate-cluster-id && make up"
    echo ""
    exit 1
fi
