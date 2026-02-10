#!/bin/bash
# Comprehensive live test against Confluent Cloud Schema Registry
# Tests: split, validate, search across Avro, Protobuf, JSON Schema
# Uses context .srctl-test to isolate from existing schemas
set -e

SR_URL="${SR_URL:?Set SR_URL to your Schema Registry URL}"
SR_USER="${SR_USER:?Set SR_USER to your API key}"
SR_PASS="${SR_PASS:?Set SR_PASS to your API secret}"
SRCTL="${SRCTL_BINARY:-../../srctl}"
CTX="${SR_CONTEXT:-.srctl-test}"
FLAGS="--url $SR_URL --username $SR_USER --password $SR_PASS --context $CTX"

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

pass=0
fail=0

step() { echo -e "\n${BLUE}━━━ $1 ━━━${NC}\n"; }
ok() { echo -e "${GREEN}[PASS]${NC} $1"; pass=$((pass+1)); }
err() { echo -e "${RED}[FAIL]${NC} $1"; fail=$((fail+1)); }

run() {
  desc="$1"; shift
  if "$@" > /tmp/srctl-out 2>&1; then
    ok "$desc"
  else
    err "$desc"
    cat /tmp/srctl-out
  fi
}

run_expect_fail() {
  desc="$1"; shift
  if "$@" > /tmp/srctl-out 2>&1; then
    err "$desc (expected failure but succeeded)"
  else
    ok "$desc"
  fi
}

# ========================
step "0. HEALTH CHECK"
# ========================
run "Health check" $SRCTL health $FLAGS

# ========================
step "1. AVRO SCHEMAS - Register with references (multi-version)"
# ========================

# V1: Register leaf types
echo '{"type":"record","name":"Address","namespace":"com.srctl.test","fields":[{"name":"street","type":"string"},{"name":"city","type":"string"},{"name":"zip","type":"string"},{"name":"country","type":"string","default":"US"}]}' > /tmp/address-v1.avsc

run "Register Address v1" $SRCTL register com.srctl.test.Address --file /tmp/address-v1.avsc $FLAGS

echo '{"type":"record","name":"Money","namespace":"com.srctl.test","fields":[{"name":"amount","type":"double"},{"name":"currency","type":"string","default":"USD"}]}' > /tmp/money-v1.avsc

run "Register Money v1" $SRCTL register com.srctl.test.Money --file /tmp/money-v1.avsc $FLAGS

# V1: Register Customer referencing Address
echo '{"type":"record","name":"Customer","namespace":"com.srctl.test","fields":[{"name":"customerId","type":"string"},{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"email","type":"string"},{"name":"address","type":"com.srctl.test.Address"}]}' > /tmp/customer-v1.avsc

run "Register Customer v1 (refs Address)" $SRCTL register com.srctl.test.Customer --file /tmp/customer-v1.avsc --ref "com.srctl.test.Address=:.srctl-test:com.srctl.test.Address:1" $FLAGS

# V1: Register LineItem referencing Money
echo '{"type":"record","name":"LineItem","namespace":"com.srctl.test","fields":[{"name":"productId","type":"string"},{"name":"productName","type":"string"},{"name":"quantity","type":"int"},{"name":"unitPrice","type":"com.srctl.test.Money"}]}' > /tmp/lineitem-v1.avsc

run "Register LineItem v1 (refs Money)" $SRCTL register com.srctl.test.LineItem --file /tmp/lineitem-v1.avsc --ref "com.srctl.test.Money=:.srctl-test:com.srctl.test.Money:1" $FLAGS

# V1: Register root Order referencing Customer + LineItem
echo '{"type":"record","name":"Order","namespace":"com.srctl.test","fields":[{"name":"orderId","type":"string"},{"name":"customer","type":"com.srctl.test.Customer"},{"name":"items","type":{"type":"array","items":"com.srctl.test.LineItem"}},{"name":"total","type":"com.srctl.test.Money"}]}' > /tmp/order-v1.avsc

run "Register Order v1 (refs Customer, LineItem, Money)" $SRCTL register com.srctl.test.Order --file /tmp/order-v1.avsc \
  --ref "com.srctl.test.Customer=:.srctl-test:com.srctl.test.Customer:1" \
  --ref "com.srctl.test.LineItem=:.srctl-test:com.srctl.test.LineItem:1" \
  --ref "com.srctl.test.Money=:.srctl-test:com.srctl.test.Money:1" \
  $FLAGS

# V2: Evolve Address with optional field
echo '{"type":"record","name":"Address","namespace":"com.srctl.test","fields":[{"name":"street","type":"string"},{"name":"city","type":"string"},{"name":"zip","type":"string"},{"name":"country","type":"string","default":"US"},{"name":"state","type":["null","string"],"default":null}]}' > /tmp/address-v2.avsc

run "Register Address v2 (add optional state)" $SRCTL register com.srctl.test.Address --file /tmp/address-v2.avsc $FLAGS

# V2: Evolve Customer with optional phone
echo '{"type":"record","name":"Customer","namespace":"com.srctl.test","fields":[{"name":"customerId","type":"string"},{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"email","type":"string"},{"name":"phone","type":["null","string"],"default":null},{"name":"address","type":"com.srctl.test.Address"}]}' > /tmp/customer-v2.avsc

run "Register Customer v2 (add optional phone, refs Address v2)" $SRCTL register com.srctl.test.Customer --file /tmp/customer-v2.avsc --ref "com.srctl.test.Address=:.srctl-test:com.srctl.test.Address:2" $FLAGS

# ========================
step "2. PROTOBUF SCHEMAS - Register with references"
# ========================

echo 'syntax = "proto3";
package com.srctl.test;

message GeoLocation {
  double latitude = 1;
  double longitude = 2;
  string label = 3;
}' > /tmp/geolocation.proto

run "Register GeoLocation proto" $SRCTL register com.srctl.test.GeoLocation --file /tmp/geolocation.proto --type PROTOBUF $FLAGS

echo 'syntax = "proto3";
package com.srctl.test;

import "geolocation.proto";

message Warehouse {
  string warehouse_id = 1;
  string name = 2;
  com.srctl.test.GeoLocation location = 3;
  int32 capacity = 4;
}' > /tmp/warehouse.proto

run "Register Warehouse proto (refs GeoLocation)" $SRCTL register com.srctl.test.Warehouse --file /tmp/warehouse.proto --type PROTOBUF --ref "geolocation.proto=:.srctl-test:com.srctl.test.GeoLocation:1" $FLAGS

# ========================
step "3. JSON SCHEMA - Register with references"
# ========================

echo '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "metadata.json",
  "type": "object",
  "properties": {
    "createdAt": {"type": "string", "format": "date-time"},
    "updatedAt": {"type": "string", "format": "date-time"},
    "createdBy": {"type": "string"},
    "version": {"type": "integer"}
  },
  "required": ["createdAt", "createdBy"]
}' > /tmp/metadata.json

run "Register Metadata JSON schema" $SRCTL register com.srctl.test.Metadata --file /tmp/metadata.json --type JSON $FLAGS

echo '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "product.json",
  "type": "object",
  "properties": {
    "productId": {"type": "string"},
    "name": {"type": "string"},
    "description": {"type": "string"},
    "price": {"type": "number"},
    "category": {"type": "string"},
    "metadata": {"$ref": "metadata.json"}
  },
  "required": ["productId", "name", "price"]
}' > /tmp/product.json

run "Register Product JSON schema (refs Metadata)" $SRCTL register com.srctl.test.Product --file /tmp/product.json --type JSON --ref "metadata.json=:.srctl-test:com.srctl.test.Metadata:1" $FLAGS

# ========================
step "4. LIST & GET - Verify registrations"
# ========================

run "List all subjects in context" $SRCTL list $FLAGS
echo "Subjects:"
cat /tmp/srctl-out

run "Get Order schema with refs" $SRCTL get com.srctl.test.Order --with-refs $FLAGS
run "Get Customer versions" $SRCTL versions com.srctl.test.Customer $FLAGS
run "Get Address v1" $SRCTL get com.srctl.test.Address --version 1 $FLAGS
run "Get Address v2" $SRCTL get com.srctl.test.Address --version 2 $FLAGS
run "Get Warehouse proto with refs" $SRCTL get com.srctl.test.Warehouse --with-refs $FLAGS
run "Get Product JSON with refs" $SRCTL get com.srctl.test.Product --with-refs $FLAGS

# ========================
step "5. STATS"
# ========================

run "Registry stats" $SRCTL stats $FLAGS
echo "Stats:"
cat /tmp/srctl-out

# ========================
step "6. DIFF - Compare versions"
# ========================

run "Diff Address v1 vs v2" $SRCTL diff com.srctl.test.Address@1 com.srctl.test.Address@2 $FLAGS
echo "Diff output:"
cat /tmp/srctl-out

run "Diff Customer v1 vs v2" $SRCTL diff com.srctl.test.Customer@1 com.srctl.test.Customer@2 $FLAGS

# ========================
step "7. VALIDATE - Offline syntax validation"
# ========================

run "Validate Address v1 (Avro)" $SRCTL validate --file /tmp/address-v1.avsc
run "Validate Address v2 (Avro)" $SRCTL validate --file /tmp/address-v2.avsc
run "Validate GeoLocation (Proto)" $SRCTL validate --file /tmp/geolocation.proto --type PROTOBUF
run "Validate Warehouse (Proto)" $SRCTL validate --file /tmp/warehouse.proto --type PROTOBUF
run "Validate Metadata (JSON)" $SRCTL validate --file /tmp/metadata.json --type JSON
run "Validate Product (JSON)" $SRCTL validate --file /tmp/product.json --type JSON

# ========================
step "8. VALIDATE - Compatibility checks"
# ========================

run "Compat: Address v1 -> v2 (BACKWARD)" $SRCTL validate --file /tmp/address-v2.avsc --against /tmp/address-v1.avsc --compatibility BACKWARD
run "Compat: Customer v1 -> v2 (BACKWARD)" $SRCTL validate --file /tmp/customer-v2.avsc --against /tmp/customer-v1.avsc --compatibility BACKWARD

# Breaking change: remove a field
echo '{"type":"record","name":"Address","namespace":"com.srctl.test","fields":[{"name":"street","type":"string"},{"name":"city","type":"string"}]}' > /tmp/address-breaking.avsc

run_expect_fail "Compat: breaking change detected (BACKWARD)" $SRCTL validate --file /tmp/address-breaking.avsc --against /tmp/address-v2.avsc --compatibility BACKWARD
echo "Breaking change output:"
cat /tmp/srctl-out

# Type change
echo '{"type":"record","name":"Address","namespace":"com.srctl.test","fields":[{"name":"street","type":"int"},{"name":"city","type":"string"},{"name":"zip","type":"string"},{"name":"country","type":"string","default":"US"}]}' > /tmp/address-typechange.avsc

run_expect_fail "Compat: type change detected (BACKWARD)" $SRCTL validate --file /tmp/address-typechange.avsc --against /tmp/address-v1.avsc --compatibility BACKWARD
echo "Type change output:"
cat /tmp/srctl-out

# FULL compat check
run_expect_fail "Compat: FULL mode catches both directions" $SRCTL validate --file /tmp/address-breaking.avsc --against /tmp/address-v1.avsc --compatibility FULL

# Against registry
run "Compat: Address v2 against registry latest" $SRCTL validate --file /tmp/address-v2.avsc --subject com.srctl.test.Address $FLAGS

# ========================
step "9. VALIDATE - Directory validation"
# ========================

mkdir -p /tmp/srctl-schemas-test
cp /tmp/address-v1.avsc /tmp/srctl-schemas-test/
cp /tmp/money-v1.avsc /tmp/srctl-schemas-test/
cp /tmp/geolocation.proto /tmp/srctl-schemas-test/
cp /tmp/metadata.json /tmp/srctl-schemas-test/

run "Validate directory (4 valid files)" $SRCTL validate --dir /tmp/srctl-schemas-test/
echo "Dir validation:"
cat /tmp/srctl-out

# Add an invalid schema
echo '{"type":"record"}' > /tmp/srctl-schemas-test/invalid.avsc
run_expect_fail "Validate directory (1 invalid)" $SRCTL validate --dir /tmp/srctl-schemas-test/
echo "Dir with invalid:"
cat /tmp/srctl-out
rm /tmp/srctl-schemas-test/invalid.avsc

# ========================
step "10. SEARCH - Field search"
# ========================

run "Search field: email" $SRCTL search --field email $FLAGS
echo "Email search:"
cat /tmp/srctl-out

run "Search field: street" $SRCTL search --field street $FLAGS
echo "Street search:"
cat /tmp/srctl-out

run "Search field: productId" $SRCTL search --field productId $FLAGS

run "Search field glob: *Id" $SRCTL search --field "*Id" $FLAGS
echo "ID fields:"
cat /tmp/srctl-out

run "Search field glob: *name*" $SRCTL search --field "*name*" $FLAGS

run "Search field: latitude (proto)" $SRCTL search --field latitude $FLAGS

# ========================
step "11. SEARCH - Type filter"
# ========================

run "Search string fields named *" $SRCTL search --field "*" --field-type string $FLAGS --version latest
echo "String fields:"
cat /tmp/srctl-out

run "Search int fields" $SRCTL search --field "*" --field-type int $FLAGS --version latest

# ========================
step "12. SEARCH - Text search"
# ========================

run "Text search: srctl.test" $SRCTL search --text "srctl.test" $FLAGS
echo "Text search:"
cat /tmp/srctl-out

run "Text search: currency" $SRCTL search --text "currency" $FLAGS

run "Text search: date-time" $SRCTL search --text "date-time" $FLAGS

# ========================
step "13. SEARCH - Subject filter"
# ========================

run "Search with subject filter: *.Money" $SRCTL search --field "*" --filter "*.Money" $FLAGS

run "Search with filter: *.Customer" $SRCTL search --field "*" --filter "*.Customer" $FLAGS --version all

# ========================
step "14. SEARCH - JSON output"
# ========================

run "Search JSON output" $SRCTL search --field email -o json $FLAGS
echo "JSON output:"
cat /tmp/srctl-out

# ========================
step "15. SPLIT - Analyze monolithic schema"
# ========================

run "Split analyze: Avro order" $SRCTL split analyze --file "$(dirname $0)/schemas/order-monolithic.avsc"
echo "Split analysis:"
cat /tmp/srctl-out

run "Split analyze: Proto order" $SRCTL split analyze --file "$(dirname $0)/schemas/order-monolithic.proto" --type PROTOBUF

run "Split analyze: JSON order" $SRCTL split analyze --file "$(dirname $0)/schemas/order-monolithic.json" --type JSON

# ========================
step "16. SPLIT - Extract to files"
# ========================

rm -rf /tmp/srctl-split-output
run "Split extract: Avro" $SRCTL split extract --file "$(dirname $0)/schemas/order-monolithic.avsc" --output-dir /tmp/srctl-split-output/avro
echo "Extracted files:"
ls -la /tmp/srctl-split-output/avro/

run "Split extract: JSON" $SRCTL split extract --file "$(dirname $0)/schemas/order-monolithic.json" --type JSON --output-dir /tmp/srctl-split-output/json

# ========================
step "17. SPLIT - Register to registry"
# ========================

run "Split register: Avro monolithic (dry-run)" $SRCTL split register --file "$(dirname $0)/schemas/order-monolithic.avsc" --subject com.srctl.test.SplitOrder --dry-run $FLAGS
echo "Dry run:"
cat /tmp/srctl-out

run "Split register: Avro monolithic (actual)" $SRCTL split register --file "$(dirname $0)/schemas/order-monolithic.avsc" --subject com.srctl.test.SplitOrder $FLAGS
echo "Registration:"
cat /tmp/srctl-out

# Verify the split registration
run "Verify split: get with refs" $SRCTL get com.srctl.test.SplitOrder --with-refs $FLAGS

# ========================
step "18. DANGLING - Check references"
# ========================

run "Check dangling references" $SRCTL dangling $FLAGS

# ========================
step "19. EVOLVE - Schema evolution"
# ========================

run "Evolve Address" $SRCTL evolve com.srctl.test.Address $FLAGS
echo "Evolution:"
cat /tmp/srctl-out

run "Evolve Customer" $SRCTL evolve com.srctl.test.Customer $FLAGS

# ========================
step "20. CONFIG - Compatibility settings"
# ========================

run "Get config" $SRCTL config $FLAGS
run "Get Address config" $SRCTL config com.srctl.test.Address $FLAGS

# ========================
# FINAL SUMMARY
# ========================

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}  TEST RESULTS${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "  ${GREEN}Passed: $pass${NC}"
echo -e "  ${RED}Failed: $fail${NC}"
echo -e "  Total:  $((pass + fail))"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [ $fail -gt 0 ]; then
  exit 1
fi
