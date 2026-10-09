#!/usr/bin/env bash
# =============================================================================
# generate_cluster_id.sh
# Generates a Kafka KRaft cluster ID and writes it to .env
#
# Usage:
#   ./scripts/generate_cluster_id.sh
#   make generate-cluster-id
#
# WARNING:
#   Only run this ONCE for the lifetime of the kafka-data volume.
#   If you run it again after Kafka has started, the new ID will mismatch
#   the ID stored in the volume and Kafka will refuse to start.
#   To reset: make clean  (destroys the volume), then re-run this script.
# =============================================================================

set -euo pipefail

ENV_FILE=".env"
ENV_EXAMPLE=".env.example"

# Colour codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

echo ""
echo -e "${CYAN}Kafka KRaft Cluster ID Generator${RESET}"
echo -e "${CYAN}==================================${RESET}"
echo ""

# -----------------------------------------------------------------------
# Ensure .env exists
# -----------------------------------------------------------------------
if [ ! -f "$ENV_FILE" ]; then
    if [ -f "$ENV_EXAMPLE" ]; then
        cp "$ENV_EXAMPLE" "$ENV_FILE"
        echo -e "${GREEN}✓ Created .env from .env.example${RESET}"
    else
        echo -e "${RED}ERROR: Neither .env nor .env.example found.${RESET}"
        echo "  Run this script from the project root directory."
        exit 1
    fi
fi

# -----------------------------------------------------------------------
# Check if KAFKA_CLUSTER_ID already set
# -----------------------------------------------------------------------
EXISTING_ID=$(grep "^KAFKA_CLUSTER_ID=" "$ENV_FILE" | cut -d'=' -f2 | tr -d '[:space:]')

if [ -n "$EXISTING_ID" ]; then
    echo -e "${YELLOW}KAFKA_CLUSTER_ID is already set in .env:${RESET}"
    echo "  $EXISTING_ID"
    echo ""
    echo -e "${YELLOW}Skipping generation to protect existing volume data.${RESET}"
    echo ""
    echo "  To generate a NEW cluster ID (requires 'make clean' first):"
    echo "  1. Run: make clean"
    echo "  2. Remove the KAFKA_CLUSTER_ID line from .env"
    echo "  3. Run: make generate-cluster-id"
    echo ""
    exit 0
fi

# -----------------------------------------------------------------------
# Generate cluster ID
# Strategy 1: Use kafka-storage.sh from a temporary Kafka container
# Strategy 2: Fallback to Python uuid (base64 encoded, Kafka format)
# -----------------------------------------------------------------------
echo "Generating Kafka KRaft cluster ID..."

CLUSTER_ID=""

# Try using Kafka's own tool via Docker
if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
    echo "  Using kafka-storage.sh via Docker (preferred method)..."
    CLUSTER_ID=$(docker run --rm \
        confluentinc/cp-kafka:7.7.1 \
        kafka-storage random-uuid 2>/dev/null || echo "")

    # Trim whitespace/newlines
    CLUSTER_ID=$(echo "$CLUSTER_ID" | tr -d '[:space:]')
fi

# Fallback: Python uuid
if [ -z "$CLUSTER_ID" ]; then
    echo "  Docker unavailable — falling back to Python uuid generator..."
    if command -v python3 &>/dev/null; then
        CLUSTER_ID=$(python3 -c "
import uuid, base64
uid = uuid.uuid4()
b64 = base64.urlsafe_b64encode(uid.bytes).decode('utf-8').rstrip('=')
print(b64)
" 2>/dev/null || echo "")
    fi
fi

# Last resort: openssl
if [ -z "$CLUSTER_ID" ]; then
    echo "  Python unavailable — falling back to openssl..."
    if command -v openssl &>/dev/null; then
        CLUSTER_ID=$(openssl rand -base64 16 | tr '+/' '-_' | tr -d '=\n' | head -c 22)
    fi
fi

if [ -z "$CLUSTER_ID" ]; then
    echo -e "${RED}ERROR: Could not generate a cluster ID.${RESET}"
    echo "  Install Docker, Python 3, or openssl and retry."
    exit 1
fi

# -----------------------------------------------------------------------
# Write to .env
# -----------------------------------------------------------------------
# Replace the empty KAFKA_CLUSTER_ID= line with the generated value
if grep -q "^KAFKA_CLUSTER_ID=" "$ENV_FILE"; then
    # Use sed to update the line in place (portable for macOS and Linux)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s|^KAFKA_CLUSTER_ID=.*|KAFKA_CLUSTER_ID=${CLUSTER_ID}|" "$ENV_FILE"
    else
        sed -i "s|^KAFKA_CLUSTER_ID=.*|KAFKA_CLUSTER_ID=${CLUSTER_ID}|" "$ENV_FILE"
    fi
else
    # Append if line doesn't exist
    echo "KAFKA_CLUSTER_ID=${CLUSTER_ID}" >> "$ENV_FILE"
fi

echo ""
echo -e "${GREEN}✓ Cluster ID generated and written to .env${RESET}"
echo ""
echo "  KAFKA_CLUSTER_ID=${CLUSTER_ID}"
echo ""
echo -e "${YELLOW}IMPORTANT:${RESET}"
echo "  This ID is now bound to your kafka-data Docker volume."
echo "  Do not regenerate it unless you run 'make clean' first."
echo ""
