#!/bin/bash
# End-to-end test script for schema splitting
#
# Prerequisites:
#   1. docker compose up -d (from this directory)
#   2. srctl binary built: cd ../.. && make build
#   3. Wait for services to be ready
#
# This script demonstrates:
#   - Analyzing monolithic schemas (Avro, Protobuf, JSON)
#   - Extracting sub-schemas to files
#   - Registering split schemas to Schema Registry
#   - Verifying with srctl get --with-refs
#   - Producing and consuming with split schemas

set -e

SR_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SRCTL="../../srctl"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

step() { echo -e "\n${BLUE}=== $1 ===${NC}\n"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

# Wait for Schema Registry
step "Waiting for Schema Registry"
for i in $(seq 1 30); do
  if curl -s "$SR_URL/" > /dev/null 2>&1; then
    success "Schema Registry is ready"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "Schema Registry not ready after 30 seconds"
    exit 1
  fi
  sleep 1
done

# ========================
# AVRO SCHEMA SPLITTING
# ========================
step "1. Analyzing Avro Schema"
$SRCTL split analyze --file "$SCRIPT_DIR/schemas/order-monolithic.avsc" --url "$SR_URL"

step "2. Extracting Avro Sub-Schemas"
rm -rf "$SCRIPT_DIR/output/avro"
$SRCTL split extract \
  --file "$SCRIPT_DIR/schemas/order-monolithic.avsc" \
  --output-dir "$SCRIPT_DIR/output/avro" \
  --url "$SR_URL"

success "Avro sub-schemas written to output/avro/"
echo "Files:"
ls -la "$SCRIPT_DIR/output/avro/"

step "3. Registering Split Avro Schema"
$SRCTL split register \
  --file "$SCRIPT_DIR/schemas/order-monolithic.avsc" \
  --subject orders-avro-value \
  --url "$SR_URL"

step "4. Verifying Avro Registration"
$SRCTL get orders-avro-value --with-refs --url "$SR_URL"
$SRCTL list --url "$SR_URL"

# ========================
# PROTOBUF SCHEMA SPLITTING
# ========================
step "5. Analyzing Protobuf Schema"
$SRCTL split analyze --file "$SCRIPT_DIR/schemas/order-monolithic.proto" --type PROTOBUF --url "$SR_URL"

step "6. Registering Split Protobuf Schema"
$SRCTL split register \
  --file "$SCRIPT_DIR/schemas/order-monolithic.proto" \
  --type PROTOBUF \
  --subject orders-proto-value \
  --url "$SR_URL"

step "7. Verifying Protobuf Registration"
$SRCTL get orders-proto-value --with-refs --url "$SR_URL"

# ========================
# JSON SCHEMA SPLITTING
# ========================
step "8. Analyzing JSON Schema"
$SRCTL split analyze --file "$SCRIPT_DIR/schemas/order-monolithic.json" --type JSON --url "$SR_URL"

step "9. Registering Split JSON Schema"
$SRCTL split register \
  --file "$SCRIPT_DIR/schemas/order-monolithic.json" \
  --type JSON \
  --subject orders-json-value \
  --url "$SR_URL"

step "10. Verifying JSON Registration"
$SRCTL get orders-json-value --with-refs --url "$SR_URL"

# ========================
# FINAL SUMMARY
# ========================
step "Final Registry State"
$SRCTL list --url "$SR_URL"
$SRCTL stats --url "$SR_URL"

echo ""
success "All schema splitting tests completed successfully!"
echo ""
echo "Registered subjects:"
curl -s "$SR_URL/subjects" | python3 -m json.tool 2>/dev/null || curl -s "$SR_URL/subjects"
