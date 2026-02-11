# Schema Splitting Guide for Confluent Schema Registry

## Problem Statement

Confluent Cloud Schema Registry enforces a **1MB limit per schema**. Large monolithic schemas -- common in enterprises with hundreds of fields, deeply nested structures, or extensive documentation -- can exceed this limit and fail to register.

The solution is **schema references**: decomposing a monolithic schema into smaller, independently registered sub-schemas that reference each other. This guide covers the end-to-end process for Avro, Protobuf, and JSON Schema.

---

## Table of Contents

- [1. How Schema References Work](#1-how-schema-references-work)
- [2. Splitting Avro Schemas](#2-splitting-avro-schemas)
- [3. Splitting Protobuf Schemas](#3-splitting-protobuf-schemas)
- [4. Splitting JSON Schemas](#4-splitting-json-schemas)
- [5. Decomposition Strategy](#5-decomposition-strategy)
- [6. Using srctl split](#6-using-srctl-split)
- [7. Client-Side Impact](#7-client-side-impact)
- [8. Migrating from Monolithic to Split (Same Subject)](#8-migrating-from-monolithic-to-split-same-subject)
- [9. Migration Rules and Data Contracts](#9-migration-rules-and-data-contracts)
- [10. Registration Workflow](#10-registration-workflow)
- [11. Best Practices](#11-best-practices)
- [12. Troubleshooting](#12-troubleshooting)

---

## 1. How Schema References Work

When registering a schema, you can include a `references` array that tells the registry to resolve types from other registered schemas:

```json
POST /subjects/{subject}/versions
{
  "schema": "<schema string>",
  "schemaType": "AVRO",
  "references": [
    {
      "name": "com.example.Address",
      "subject": "com.example.Address",
      "version": 1
    }
  ]
}
```

| Field     | Description |
|-----------|-------------|
| `name`    | The logical reference identifier used *inside* the schema. For Avro: fully qualified type name. For Protobuf: import path. For JSON Schema: the `$ref` URI. |
| `subject` | The Schema Registry subject where the referenced schema is registered. |
| `version` | The specific version of the referenced schema to use. |

### Key Properties

- **Transitive resolution**: If A references B, and B references C, the registry resolves the full chain automatically.
- **Independent versioning**: Each referenced schema is a normal subject with its own version history and compatibility checks.
- **Wire format unchanged**: Serialized messages contain only the root schema's ID (5-byte header: magic byte + 4-byte schema ID). Consumers fetch and resolve references transparently.
- **Bottom-up registration**: Leaf schemas must be registered first, then schemas that depend on them, then the root.
- **Referential integrity**: You cannot delete a schema that is referenced by other schemas.

---

## 2. Splitting Avro Schemas

### Before: Monolithic Schema

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.events",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "customer", "type": {
      "type": "record",
      "name": "Customer",
      "namespace": "com.example.types",
      "fields": [
        {"name": "customerId", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "address", "type": {
          "type": "record",
          "name": "Address",
          "namespace": "com.example.types",
          "fields": [
            {"name": "street", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "zip", "type": "string"}
          ]
        }}
      ]
    }},
    {"name": "items", "type": {
      "type": "array",
      "items": {
        "type": "record",
        "name": "LineItem",
        "namespace": "com.example.types",
        "fields": [
          {"name": "productId", "type": "string"},
          {"name": "quantity", "type": "int"},
          {"name": "price", "type": "double"}
        ]
      }
    }}
  ]
}
```

### After: Split Into Referenced Schemas

**Step 1** -- Register `com.example.types.Address` (leaf, no references):

```json
{
  "type": "record",
  "name": "Address",
  "namespace": "com.example.types",
  "fields": [
    {"name": "street", "type": "string"},
    {"name": "city", "type": "string"},
    {"name": "zip", "type": "string"}
  ]
}
```

**Step 2** -- Register `com.example.types.Customer` (references Address):

Schema:
```json
{
  "type": "record",
  "name": "Customer",
  "namespace": "com.example.types",
  "fields": [
    {"name": "customerId", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "address", "type": "com.example.types.Address"}
  ]
}
```

References:
```json
[{"name": "com.example.types.Address", "subject": "com.example.types.Address", "version": 1}]
```

**Step 3** -- Register `com.example.types.LineItem` (leaf, no references):

```json
{
  "type": "record",
  "name": "LineItem",
  "namespace": "com.example.types",
  "fields": [
    {"name": "productId", "type": "string"},
    {"name": "quantity", "type": "int"},
    {"name": "price", "type": "double"}
  ]
}
```

**Step 4** -- Register root schema `orders-value` (references Customer and LineItem):

Schema:
```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.events",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "customer", "type": "com.example.types.Customer"},
    {"name": "items", "type": {"type": "array", "items": "com.example.types.LineItem"}}
  ]
}
```

References:
```json
[
  {"name": "com.example.types.Customer", "subject": "com.example.types.Customer", "version": 1},
  {"name": "com.example.types.LineItem", "subject": "com.example.types.LineItem", "version": 1}
]
```

### Avro-Specific Rules

- The `name` in the reference **must match the fully qualified Avro type name** (namespace.name).
- Avro has no `import` statement -- cross-references rely on named types and registry reference resolution.
- Inline record definitions become type references by their fully qualified name.
- Union types work the same way: `["null", "com.example.types.Address"]`.
- Enums and fixed types can also be extracted as separate referenced schemas.

---

## 3. Splitting Protobuf Schemas

Protobuf has **native `import` statements**, making splitting natural.

### Before: Monolithic

```protobuf
syntax = "proto3";
package com.example.events;

message Order {
  string order_id = 1;
  Customer customer = 2;
  repeated LineItem items = 3;
}

message Customer {
  string customer_id = 1;
  string name = 2;
  Address address = 3;
}

message Address {
  string street = 1;
  string city = 2;
  string zip = 3;
}

message LineItem {
  string product_id = 1;
  int32 quantity = 2;
  double price = 3;
}
```

### After: Split With Imports

**Step 1** -- Register `address.proto` (subject: `address.proto`):

```protobuf
syntax = "proto3";
package com.example.types;

message Address {
  string street = 1;
  string city = 2;
  string zip = 3;
}
```

**Step 2** -- Register `customer.proto` with import:

```protobuf
syntax = "proto3";
package com.example.types;

import "address.proto";

message Customer {
  string customer_id = 1;
  string name = 2;
  com.example.types.Address address = 3;
}
```

References: `[{"name": "address.proto", "subject": "address.proto", "version": 1}]`

**Step 3** -- Register `line_item.proto` (leaf):

```protobuf
syntax = "proto3";
package com.example.types;

message LineItem {
  string product_id = 1;
  int32 quantity = 2;
  double price = 3;
}
```

**Step 4** -- Register root `order.proto` (subject: `orders-value`):

```protobuf
syntax = "proto3";
package com.example.events;

import "customer.proto";
import "line_item.proto";

message Order {
  string order_id = 1;
  com.example.types.Customer customer = 2;
  repeated com.example.types.LineItem items = 3;
}
```

References:
```json
[
  {"name": "customer.proto", "subject": "customer.proto", "version": 1},
  {"name": "line_item.proto", "subject": "line_item.proto", "version": 1}
]
```

### Protobuf-Specific Rules

- The `name` in the reference **must match the import path** exactly.
- Well-known types (`google/protobuf/timestamp.proto`, etc.) are built-in and do **not** need to be registered.
- Nested messages stay within a single schema. Only split at file/import boundaries.

---

## 4. Splitting JSON Schemas

JSON Schema uses `$ref` and `$id` for referencing.

### Before: Monolithic

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "orderId": {"type": "string"},
    "customer": {
      "type": "object",
      "properties": {
        "customerId": {"type": "string"},
        "name": {"type": "string"},
        "address": {
          "type": "object",
          "properties": {
            "street": {"type": "string"},
            "city": {"type": "string"},
            "zip": {"type": "string"}
          }
        }
      }
    }
  }
}
```

### After: Split With $ref

**Step 1** -- Register `address.json` (subject: `address.json`):

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "address.json",
  "type": "object",
  "properties": {
    "street": {"type": "string"},
    "city": {"type": "string"},
    "zip": {"type": "string"}
  },
  "required": ["street", "city", "zip"]
}
```

**Step 2** -- Register `customer.json` with `$ref`:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "customer.json",
  "type": "object",
  "properties": {
    "customerId": {"type": "string"},
    "name": {"type": "string"},
    "address": {"$ref": "address.json"}
  }
}
```

References: `[{"name": "address.json", "subject": "address.json", "version": 1}]`

**Step 3** -- Register root `orders-value`:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "order.json",
  "type": "object",
  "properties": {
    "orderId": {"type": "string"},
    "customer": {"$ref": "customer.json"},
    "items": {
      "type": "array",
      "items": {"$ref": "line_item.json"}
    }
  }
}
```

### JSON Schema-Specific Rules

- The `name` in the reference must match the `$ref` URI.
- The `$id` in the referenced schema should match the reference `name`.
- `$ref` with JSON Pointer works: `{"$ref": "common.json#/definitions/Money"}` -- the `name` is still `"common.json"`.
- Use `definitions`/`$defs` for intra-schema reuse before extracting to separate schemas.

---

## 5. Decomposition Strategy

### What to Extract

Prioritize extraction in this order:

1. **Shared/common types** -- Types used across multiple schemas (Address, Money, PhoneNumber). Best candidates because they enable reuse across topics.

2. **Large nested records** -- Deeply nested inline record definitions contributing the most bytes. Target the largest types first for maximum size reduction.

3. **Domain boundary types** -- Types belonging to different bounded contexts (DDD). Each domain gets its own subject for independent evolution.

4. **Stable vs. volatile types** -- Separate types that rarely change (common types) from types that change frequently (business types). Allows independent versioning.

### Layered Architecture

```
Layer 3 (Topic schemas):    orders-value, payments-value, shipments-value
                                |              |              |
Layer 2 (Domain types):     Customer,      Payment,       Shipment
                                |              |              |
Layer 1 (Common types):     Address, Money, PhoneNumber, Currency
```

### When NOT to Split

- Schema is well under the 1MB limit and not shared across topics.
- Types form circular dependencies (A -> B -> A). Break cycles with ID-based references instead.
- The added operational complexity of managing multiple subjects isn't justified.

### Size Reduction Before Splitting

Try reducing size within a single schema first:
- Remove verbose `doc` strings if they're the primary size contributor.
- Use `definitions`/`$defs` in JSON Schema for intra-schema reuse.
- Consolidate similar enum types.
- Remove redundant default values.

---

## 6. Using srctl split

The `srctl split` command automates schema decomposition.

### Analyze (Dry Run)

See what would be extracted without making changes:

```bash
# Analyze an Avro schema
srctl split analyze --file order.avsc

# Analyze with size threshold (only extract types > 10KB)
srctl split analyze --file order.avsc --min-size 10240

# Top-level split only (extract direct field types, keep nesting intact)
srctl split analyze --file order.avsc --depth 1

# Analyze a Protobuf schema
srctl split analyze --file order.proto --type PROTOBUF

# Analyze a JSON schema
srctl split analyze --file order.json --type JSON
```

Output shows the dependency tree, extracted types, estimated sizes, and registration order.

### Extract to Files

Extract sub-schemas to a directory for review:

```bash
# Split to output directory
srctl split extract --file order.avsc --output-dir ./split-schemas/

# Split with custom subject prefix
srctl split extract --file order.avsc --output-dir ./split-schemas/ \
  --subject-prefix "com.example.types."

# Top-level extraction only (fewer, larger sub-schemas)
srctl split extract --file order.avsc --output-dir ./split-schemas/ --depth 1

# Split a Protobuf schema
srctl split extract --file order.proto --type PROTOBUF --output-dir ./split-schemas/
```

### Split and Register

Split the schema and register all parts to Schema Registry in correct dependency order:

```bash
# Split and register
srctl split register --file order.avsc --subject orders-value

# Top-level split and register (recommended for large schemas)
srctl split register --file order.avsc --subject orders-value --depth 1

# Dry run first
srctl split register --file order.avsc --subject orders-value --dry-run

# With explicit schema type
srctl split register --file order.proto --type PROTOBUF --subject orders-value
```

### Flags Reference

| Flag | Description |
|------|-------------|
| `--file, -f` | Path to the schema file |
| `--type, -t` | Schema type: AVRO, PROTOBUF, JSON (auto-detected from extension) |
| `--output-dir` | Directory to write split schemas (extract subcommand) |
| `--subject` | Subject name for the root schema (register subcommand) |
| `--subject-prefix` | Prefix for extracted type subjects |
| `--min-size` | Minimum type size in bytes to extract (default: 0 = extract all named types) |
| `--depth` | Extraction depth: `0` = full recursive extraction of all named types (default), `1` = top-level fields only, keeping nested types inline. Currently Avro-only. |
| `--dry-run` | Show what would happen without registering |
| `--compatibility` | Set compatibility for extracted subjects (default: BACKWARD) |

### Depth Control

The `--depth` flag controls how aggressively nested types are extracted:

- **`--depth 0`** (default) -- Full recursive extraction. Every named type (record, enum, fixed) at every nesting level is extracted into its own subject. This maximizes reuse and modularity but can produce a large number of subjects.

- **`--depth 1`** -- Top-level only. Extracts only the direct field types of the root record. Each extracted type keeps all of its own nested types inline. This produces fewer, larger subjects that are easier to manage operationally.

```
Root schema
  ├─ Field A → Customer record
  │              └─ Nested → Address record
  ├─ Field B → LineItem record
  │              └─ Nested → Money record

--depth 0: Extracts Root, Customer, Address, LineItem, Money → 5 subjects
--depth 1: Extracts Root, Customer (with Address inline), LineItem (with Money inline) → 3 subjects
```

**When to use `--depth 1`:**

- Very large schemas (e.g., 4MB+) with hundreds of nested types where `--depth 0` would create thousands of subjects
- When operational simplicity is preferred over maximum type reuse
- When nested types are not shared across other schemas

**When to use `--depth 0`:**

- Types at deeper nesting levels are shared across multiple schemas (e.g., Address, Money)
- You want maximum granularity for independent versioning
- The total number of extracted subjects is manageable

**Note:** The `--depth` flag currently only affects Avro schemas. Protobuf and JSON Schema splitting are unaffected.

---

## 7. Client-Side Impact

### The Key Insight: Minimal to Zero Changes

The Confluent SerDes handle schema reference resolution **transparently**. The wire format is unchanged -- serialized messages contain only the root schema's 5-byte header (magic byte + 4-byte schema ID).

### Producer Side

```java
// NO CODE CHANGES NEEDED
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", StringSerializer.class);
props.put("value.serializer", KafkaAvroSerializer.class);
props.put("schema.registry.url", "http://localhost:8081");

// IMPORTANT for split schemas: disable auto-registration in production
// Pre-register split schemas via CI/CD pipeline
props.put("auto.register.schemas", false);
props.put("use.latest.version", true);
```

### Consumer Side

```java
// NO CODE CHANGES NEEDED
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.deserializer", StringDeserializer.class);
props.put("value.deserializer", KafkaAvroDeserializer.class);
props.put("schema.registry.url", "http://localhost:8081");
props.put("specific.avro.reader", true);
```

### Python (confluent-kafka)

```python
# NO CODE CHANGES NEEDED
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

serializer = AvroSerializer(schema_registry_client, schema_str)
deserializer = AvroDeserializer(schema_registry_client)
```

### Required Library Versions

Schema references require these minimum versions:

| Language | Library | Minimum Version |
|----------|---------|----------------|
| Java | `io.confluent:kafka-avro-serializer` | 5.5.0 |
| Python | `confluent-kafka[avro]` | 1.5.0 |
| .NET | `Confluent.SchemaRegistry.Serdes.Avro` | 1.5.0 |
| Go | `confluent-kafka-go` + SR client | 1.5.0 |

### What Does Change

| Aspect | Before (Monolithic) | After (Split) | Impact |
|--------|-------------------|---------------|--------|
| Auto-registration | Works for single schema | May fail for references | Disable in prod; pre-register via CI/CD |
| First deserialization | Fetches 1 schema | Fetches N+1 schemas | Negligible; cached after first fetch |
| Code generation | All types in one scope | Same types, separate files | **Same result** -- Avro codegen already creates per-type classes |
| Compatibility | Single subject | Multiple subjects | Each subject checked independently |

### Key Recommendation

**Pre-register schemas via CI/CD** rather than relying on auto-registration:

```bash
# In CI/CD pipeline
srctl split register \
  --file ./schemas/order.avsc \
  --subject orders-value \
  --registry production
```

---

## 8. Migrating from Monolithic to Split (Same Subject)

A common concern: if a client is already producing/consuming with the monolithic schema (v1) and you register a split version (v2) under the **same subject**, does it break compatibility?

**No. It works seamlessly.** Here's exactly what happens and why.

### How the Registry Checks Compatibility

When you register a split schema as v2, the Schema Registry does **not** compare the raw schema strings. It:

1. Fetches all referenced schemas (transitively)
2. Assembles the **fully resolved schema** (root + all references inlined)
3. Compares that resolved schema against v1 for compatibility

If you split without changing any fields, types, or defaults, the resolved v2 schema is **structurally identical** to v1. The compatibility check passes.

### Step-by-Step: What Happens

```
Subject: orders-value

v1 (monolithic):
  Schema: { full schema with Address, Customer, LineItem inline }
  References: none
  Schema ID: 100

v2 (split):
  Schema: { root with type references: "com.example.types.Address", etc. }
  References: [Address:1, Customer:1, LineItem:1]
  Schema ID: 200

Registry resolves v2 → identical logical schema to v1 → compatibility passes ✓
```

### Wire Format Is Identical

The Avro binary encoding is determined by the **resolved schema**, which is identical in both cases:

```
Producer using v1:  [magic byte][schema ID = 100][avro binary payload]
Producer using v2:  [magic byte][schema ID = 200][avro binary payload]
                                                  ↑ same bytes
```

A consumer reading either message fetches the schema ID, resolves it (including references for v2), and deserializes. The result is identical.

### What Each Client Sees

| Client | Behavior | Changes Needed |
|--------|----------|----------------|
| Existing producer (v1) | Continues writing with schema ID 100 | None |
| Existing consumer | Reads both v1 and v2 messages identically | None |
| New producer (v2) | Writes with schema ID 200, same binary format | None (if using `use.latest.version=true`) |
| New consumer | Resolves references transparently | None (SerDe 5.5.0+) |

### Verified on Confluent Cloud

This was tested against a live Confluent Cloud Schema Registry:

```bash
# 1. Register monolithic schema
srctl register orders-value --file order-monolithic.avsc

# 2. Register sub-schemas as separate subjects
srctl register com.example.types.Address --file address.avsc
srctl register com.example.types.Money   --file money.avsc

# 3. Register split version under SAME subject
srctl register orders-value --file order-split.avsc \
  --ref "com.example.types.Address=com.example.types.Address:1" \
  --ref "com.example.types.Money=com.example.types.Money:1"
# ✓ Registered as v2, compatibility check passed

# 4. Both versions coexist
srctl versions orders-value
# VERSION | SCHEMA ID
# 1       | 100      (monolithic)
# 2       | 200      (split with references)

# 5. Diff shows zero field changes
srctl diff orders-value@1 orders-value@2
# Added:     0 field(s)
# Removed:   0 field(s)
# Modified:  2 field(s)  ← representation changed, not structure
# Unchanged: 1 field(s)
```

The `Modified` fields reflect that the raw type representation changed from inline `record` to a reference string like `com.example.types.Address`, but the **resolved schema** and **binary encoding** are identical.

### When It WOULD Break

The split itself never breaks compatibility. It breaks only if you **also change the schema structure** during the split:

```
# Safe (pure split, no field changes):
v1: monolithic with [orderId, customer.name, customer.email]
v2: split references, same fields [orderId, customer.name, customer.email]
→ Compatible ✓

# Breaks (changed fields during split):
v1: monolithic with [orderId, customer.name, customer.email]
v2: split, but also removed customer.email
→ Incompatible ✗ (under BACKWARD)
```

Use `srctl validate` to check before registering:

```bash
# Check locally before touching the registry
srctl validate --file order-split.avsc --against order-monolithic.avsc --compatibility BACKWARD
```

### Recommended Migration Procedure

1. **Validate locally** -- `srctl validate --file split.avsc --against monolithic.avsc`
2. **Register sub-schemas** -- leaf types first, then dependents (bottom-up)
3. **Register split root as new version** -- under the same subject
4. **Verify** -- `srctl diff subject@1 subject@2` shows zero field additions/removals
5. **Roll out** -- no client changes needed; new producers pick up v2 automatically with `use.latest.version=true`

---

## 9. Migration Rules and Data Contracts

### When You Need Migration Rules

Migration rules are **not needed** for a purely structural split (same logical schema, decomposed into references). The serialized format is identical.

Migration rules become relevant when you **also restructure** during the split:
- Renaming fields
- Changing type structures (e.g., flattening nested objects)
- Supporting consumers on the old monolithic version while producers use the new split version

### Attaching Migration Rules

When registering the new split version, include migration rules in the `ruleSet`:

```json
{
  "schema": "<split root schema>",
  "schemaType": "AVRO",
  "references": [...],
  "ruleSet": {
    "migrationRules": [
      {
        "name": "upgrade-from-monolithic",
        "kind": "TRANSFORM",
        "type": "UPGRADE",
        "mode": "UPGRADE",
        "expr": "CEL expression or transformer class"
      },
      {
        "name": "downgrade-to-monolithic",
        "kind": "TRANSFORM",
        "type": "DOWNGRADE",
        "mode": "DOWNGRADE",
        "expr": "CEL expression or transformer class"
      }
    ]
  },
  "metadata": {
    "properties": {
      "owner": "team-orders",
      "description": "Order schema - split from monolithic v3"
    }
  }
}
```

### Safe Migration Workflow

**For a purely structural split** (recommended):

1. Register all reference schemas (leaf first, bottom-up)
2. Register new root schema version with references
3. Resolved schema is logically identical to the monolithic version
4. Compatibility checks pass automatically
5. Existing producers and consumers continue working without any changes

**For a split that includes restructuring:**

1. Register reference schemas
2. Register new root schema with UPGRADE/DOWNGRADE migration rules
3. Roll out new consumers first (they use UPGRADE rules to read old data)
4. Roll out new producers (they write in new format)
5. DOWNGRADE rules handle backward compatibility during the transition

### Built-in Transformers

Confluent provides built-in transformers for migration rules:
- **CEL** (Common Expression Language) for field-level transformations
- **JSONata** for JSON Schema transformations
- **JOLT** for JSON-to-JSON transformations

### Using srctl for Data Contracts

```bash
# View existing data contract rules
srctl contract get orders-value

# Set migration rules from a file
srctl contract set orders-value --rules migration-rules.json

# Validate schema against rules
srctl contract validate orders-value --schema order-v2.avsc
```

---

## 10. Registration Workflow

### Step-by-Step Process

```
1. Analyze    srctl split analyze --file schema.avsc
                 |
2. Extract    srctl split extract --file schema.avsc --output-dir ./split/
                 |
3. Review     Inspect generated files, adjust subject names if needed
                 |
4. Test       srctl split register --file schema.avsc --subject my-topic-value --dry-run
                 |
5. Register   srctl split register --file schema.avsc --subject my-topic-value
                 |
6. Verify     srctl get my-topic-value --with-refs
```

### CI/CD Integration

```bash
#!/bin/bash
# deploy-schemas.sh
set -e

# Split and register -- handles dependency ordering automatically
srctl split register \
  --file ./schemas/order.avsc \
  --subject orders-value \
  --url "$SCHEMA_REGISTRY_URL" \
  --username "$SR_API_KEY" \
  --password "$SR_API_SECRET"

echo "Schema registration complete"

# Verify
srctl get orders-value --with-refs \
  --url "$SCHEMA_REGISTRY_URL" \
  --username "$SR_API_KEY" \
  --password "$SR_API_SECRET"
```

---

## 11. Best Practices

### Subject Naming for References

| Format | Convention | Example |
|--------|-----------|---------|
| Avro | Fully qualified type name | `com.example.types.Address` |
| Protobuf | Import file path | `address.proto` |
| JSON Schema | `$id` value | `address.json` |

### Compatibility Settings

| Subject Type | Recommended | Rationale |
|-------------|-------------|-----------|
| Shared/leaf types | `FULL_TRANSITIVE` | Maximum safety -- many schemas depend on these |
| Domain types | `BACKWARD_TRANSITIVE` | Balance of safety and flexibility |
| Root/topic schemas | `BACKWARD` | Consumers can always read older data |

### Version Pinning

- **Production**: Always pin references to specific versions (`"version": 3`)
- **Development**: Using latest (`"version": -1`) is acceptable for fast iteration

### Monitoring

```bash
# Check for broken references
srctl dangling

# View schema sizes and identify candidates for splitting
srctl stats --detailed

# Check dependency graph for a schema
srctl get my-subject --with-refs
```

---

## 12. Troubleshooting

### "Schema too large" Error
Your schema exceeds the 1MB limit. Use `srctl split analyze` to identify extractable types and plan the decomposition.

### "Reference not found" Error
References must be registered before the schema that uses them. Use `srctl split register` which handles ordering automatically, or manually register leaf schemas first.

### Compatibility Check Failures After Split
The resolved schema (root + all references) must be compatible with the previous monolithic version. If you split without changing the logical structure, this should pass. Verify with `srctl split register --dry-run`.

### Consumer Fails to Deserialize
Ensure consumer libraries are version 5.5.0+ (Java) or equivalent. Older versions don't support schema references.

### Auto-Registration Fails
When `auto.register.schemas=true`, the serializer may fail if referenced schemas aren't pre-registered. Disable auto-registration in production and pre-register via CI/CD.

### Circular Dependencies
Schema Registry does not support circular references. Break cycles by using ID-based references (e.g., `customerId: string` instead of embedding the full `Customer` type).
