#!/bin/bash
# CI-friendly integration test for srctl
#
# Runs against a local Schema Registry (docker compose).
# No hardcoded credentials -- uses localhost:8081 with no auth.
#
# Usage:
#   cd examples/split-demo
#   docker compose up -d
#   ./integration-test.sh
#   docker compose down -v
set -e

SR_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SRCTL="${SRCTL_BINARY:-../../srctl}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

pass=0
fail=0

step() { echo -e "\n${BLUE}--- $1 ---${NC}"; }
ok() { echo -e "${GREEN}[PASS]${NC} $1"; pass=$((pass+1)); }
err() { echo -e "${RED}[FAIL]${NC} $1"; fail=$((fail+1)); }

run() {
  desc="$1"; shift
  if "$@" > /tmp/srctl-integ-out 2>&1; then
    ok "$desc"
  else
    err "$desc"
    head -20 /tmp/srctl-integ-out
  fi
}

run_expect_fail() {
  desc="$1"; shift
  if "$@" > /tmp/srctl-integ-out 2>&1; then
    err "$desc (expected failure)"
  else
    ok "$desc"
  fi
}

# Build if needed
if [ ! -f "$SRCTL" ]; then
  echo "Building srctl..."
  (cd "$SCRIPT_DIR/../.." && go build -o srctl .)
  SRCTL="$SCRIPT_DIR/../../srctl"
fi

# Wait for Schema Registry
step "Waiting for Schema Registry at $SR_URL"
for i in $(seq 1 30); do
  if curl -s "$SR_URL/" > /dev/null 2>&1; then
    ok "Schema Registry is ready"
    break
  fi
  if [ "$i" -eq 30 ]; then
    err "Schema Registry not ready after 30 seconds"
    exit 1
  fi
  sleep 1
done

# ========================
step "1. Health"
# ========================
run "Health check" $SRCTL health --url "$SR_URL"

# ========================
step "2. Register Avro schemas with references"
# ========================
echo '{"type":"record","name":"Address","namespace":"com.test","fields":[{"name":"street","type":"string"},{"name":"city","type":"string"}]}' > /tmp/it-address.avsc
echo '{"type":"record","name":"Customer","namespace":"com.test","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"},{"name":"address","type":"com.test.Address"}]}' > /tmp/it-customer.avsc
echo '{"type":"record","name":"Order","namespace":"com.test","fields":[{"name":"orderId","type":"string"},{"name":"customer","type":"com.test.Customer"},{"name":"total","type":"double"}]}' > /tmp/it-order.avsc

run "Register Address" $SRCTL register com.test.Address --file /tmp/it-address.avsc --url "$SR_URL"
run "Register Customer (ref Address)" $SRCTL register com.test.Customer --file /tmp/it-customer.avsc --ref "com.test.Address=com.test.Address:1" --url "$SR_URL"
run "Register Order (ref Customer)" $SRCTL register com.test.Order --file /tmp/it-order.avsc --ref "com.test.Customer=com.test.Customer:1" --url "$SR_URL"

# Evolve Address (add optional field)
echo '{"type":"record","name":"Address","namespace":"com.test","fields":[{"name":"street","type":"string"},{"name":"city","type":"string"},{"name":"zip","type":["null","string"],"default":null}]}' > /tmp/it-address-v2.avsc
run "Register Address v2" $SRCTL register com.test.Address --file /tmp/it-address-v2.avsc --url "$SR_URL"

# ========================
step "3. Register Protobuf schema"
# ========================
echo 'syntax = "proto3";
package com.test;
message Event {
  string event_id = 1;
  string event_type = 2;
  int64 timestamp = 3;
}' > /tmp/it-event.proto

run "Register Protobuf" $SRCTL register com.test.Event --file /tmp/it-event.proto --type PROTOBUF --url "$SR_URL"

# ========================
step "4. Register JSON Schema"
# ========================
echo '{"$schema":"http://json-schema.org/draft-07/schema#","$id":"product.json","type":"object","properties":{"id":{"type":"string"},"price":{"type":"number"}},"required":["id","price"]}' > /tmp/it-product.json

run "Register JSON Schema" $SRCTL register com.test.Product --file /tmp/it-product.json --type JSON --url "$SR_URL"

# ========================
step "5. List & Get"
# ========================
run "List subjects" $SRCTL list --url "$SR_URL"
run "Get Order with refs" $SRCTL get com.test.Order --with-refs --url "$SR_URL"
run "Get Address versions" $SRCTL versions com.test.Address --url "$SR_URL"
run "Get Address v1" $SRCTL get com.test.Address --version 1 --url "$SR_URL"
run "Get Address v2" $SRCTL get com.test.Address --version 2 --url "$SR_URL"

# ========================
step "6. Stats"
# ========================
run "Stats" $SRCTL stats --url "$SR_URL"

# ========================
step "7. Diff"
# ========================
run "Diff Address v1 vs v2" $SRCTL diff com.test.Address@1 com.test.Address@2 --url "$SR_URL"

# ========================
step "8. Evolve"
# ========================
run "Evolve Address" $SRCTL evolve com.test.Address --url "$SR_URL"

# ========================
step "9. Validate - Syntax"
# ========================
run "Validate Avro" $SRCTL validate --file /tmp/it-address.avsc
run "Validate Proto" $SRCTL validate --file /tmp/it-event.proto --type PROTOBUF
run "Validate JSON" $SRCTL validate --file /tmp/it-product.json --type JSON
run "Validate dir" $SRCTL validate --dir /tmp/ 2>&1 | head -20 # will find all our test files

# ========================
step "10. Validate - Compatibility"
# ========================
run "Compat Address v1->v2 (BACKWARD)" $SRCTL validate --file /tmp/it-address-v2.avsc --against /tmp/it-address.avsc

# Breaking change
echo '{"type":"record","name":"Address","namespace":"com.test","fields":[{"name":"street","type":"string"}]}' > /tmp/it-address-breaking.avsc
run_expect_fail "Compat breaking change detected" $SRCTL validate --file /tmp/it-address-breaking.avsc --against /tmp/it-address.avsc

# Type change
echo '{"type":"record","name":"Address","namespace":"com.test","fields":[{"name":"street","type":"int"},{"name":"city","type":"string"}]}' > /tmp/it-address-typechange.avsc
run_expect_fail "Compat type change detected" $SRCTL validate --file /tmp/it-address-typechange.avsc --against /tmp/it-address.avsc

# ========================
step "11. Search"
# ========================
run "Search field: street" $SRCTL search --field street --url "$SR_URL"
run "Search field: id" $SRCTL search --field id --url "$SR_URL"
run "Search field glob: *id*" $SRCTL search --field "*id*" --url "$SR_URL"
run "Search text: com.test" $SRCTL search --text "com.test" --url "$SR_URL"
run "Search JSON output" $SRCTL search --field street -o json --url "$SR_URL"

# ========================
step "12. Explain"
# ========================
run "Explain from registry" $SRCTL explain com.test.Order --url "$SR_URL"
run "Explain local file" $SRCTL explain --file /tmp/it-address.avsc
run "Explain proto" $SRCTL explain --file /tmp/it-event.proto --type PROTOBUF
run "Explain JSON" $SRCTL explain --file /tmp/it-product.json --type JSON
run "Explain JSON output" $SRCTL explain com.test.Address -o json --url "$SR_URL"

# ========================
step "13. Suggest"
# ========================
run "Suggest add field" $SRCTL suggest --file /tmp/it-address.avsc "add zip code"
run "Suggest remove field" $SRCTL suggest --file /tmp/it-address.avsc "remove city"
run "Suggest rename" $SRCTL suggest --file /tmp/it-address.avsc "rename street to streetAddress"
run "Suggest against registry" $SRCTL suggest com.test.Address "add country" --url "$SR_URL"

# ========================
step "14. Generate"
# ========================
run "Generate Avro" bash -c 'echo "{\"id\":\"123\",\"amount\":49.99,\"active\":true}" | '"$SRCTL"' generate --name Order --namespace com.test'
run "Generate Protobuf" bash -c 'echo "{\"id\":\"123\",\"count\":5}" | '"$SRCTL"' generate --type PROTOBUF --name Event'
run "Generate JSON Schema" bash -c 'echo "{\"id\":\"123\",\"email\":\"test@example.com\"}" | '"$SRCTL"' generate --type JSON'
run "Generate with format detection" bash -c 'echo "{\"id\":\"550e8400-e29b-41d4-a716-446655440000\",\"created\":\"2024-01-15T10:30:00Z\",\"email\":\"user@example.com\"}" | '"$SRCTL"' generate --name Event'

# ========================
step "15. Split"
# ========================
run "Split analyze Avro" $SRCTL split analyze --file "$SCRIPT_DIR/schemas/order-monolithic.avsc"
run "Split analyze Proto" $SRCTL split analyze --file "$SCRIPT_DIR/schemas/order-monolithic.proto" --type PROTOBUF
run "Split analyze JSON" $SRCTL split analyze --file "$SCRIPT_DIR/schemas/order-monolithic.json" --type JSON

rm -rf /tmp/srctl-integ-split
run "Split extract" $SRCTL split extract --file "$SCRIPT_DIR/schemas/order-monolithic.avsc" --output-dir /tmp/srctl-integ-split

run "Split register dry-run" $SRCTL split register --file "$SCRIPT_DIR/schemas/order-monolithic.avsc" --subject com.test.SplitOrder --dry-run --url "$SR_URL"
run "Split register" $SRCTL split register --file "$SCRIPT_DIR/schemas/order-monolithic.avsc" --subject com.test.SplitOrder --url "$SR_URL"
run "Verify split" $SRCTL get com.test.SplitOrder --with-refs --url "$SR_URL"

# ========================
step "16. Dangling"
# ========================
run "Dangling check" $SRCTL dangling --url "$SR_URL"

# ========================
# SUMMARY
# ========================
echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  INTEGRATION TEST RESULTS${NC}"
echo -e "${BLUE}============================================${NC}"
echo -e "  ${GREEN}Passed: $pass${NC}"
echo -e "  ${RED}Failed: $fail${NC}"
echo -e "  Total:  $((pass + fail))"
echo -e "${BLUE}============================================${NC}"

# Cleanup test files
rm -f /tmp/it-*.avsc /tmp/it-*.proto /tmp/it-*.json /tmp/it-*.avsc
rm -rf /tmp/srctl-integ-split /tmp/srctl-integ-out

if [ $fail -gt 0 ]; then
  exit 1
fi
