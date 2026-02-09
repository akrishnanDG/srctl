# Schema Splitting Demo

End-to-end demonstration of splitting large monolithic schemas into referenced sub-schemas using `srctl split`.

## Quick Start

### 1. Start Infrastructure

```bash
docker compose up -d
```

Wait for services to be ready (Schema Registry takes a few seconds after Kafka):

```bash
curl http://localhost:8081/
```

### 2. Build srctl

```bash
cd ../..
make build
```

### 3. Run the Split Demo

```bash
./test-split.sh
```

This script will:
- Analyze Avro, Protobuf, and JSON monolithic schemas
- Extract sub-schemas to files
- Register split schemas to local Schema Registry
- Verify registration with `srctl get --with-refs`

### 4. Run Sample Producer/Consumer (Optional)

Requires Python 3.8+ and the confluent-kafka library.

```bash
cd sample-app
pip install -r requirements.txt

# First register the split schemas (if not already done by test-split.sh)
../../srctl split register \
  --file ../schemas/order-monolithic.avsc \
  --subject orders-value \
  --url http://localhost:8081

# Run producer
python producer.py

# Run consumer (in another terminal)
python consumer.py
```

## What This Demonstrates

### No Client Code Changes

The producer and consumer code is **identical** whether the schema is monolithic or split into references. The Confluent SerDes handle reference resolution transparently:

1. **Producer**: Serializes using the root schema. The wire format includes only the root schema's 5-byte header (magic byte + 4-byte schema ID).
2. **Consumer**: Reads the schema ID, fetches the root schema from the registry, which includes its references. The registry resolves all referenced schemas transitively. The consumer deserializes without knowing the schema is split.

### Schema Files

| File | Format | Description |
|------|--------|-------------|
| `schemas/order-monolithic.avsc` | Avro | Monolithic order schema with nested types (Address, Customer, LineItem, Money, Payment) |
| `schemas/order-monolithic.proto` | Protobuf | Same order schema in Protobuf format |
| `schemas/order-monolithic.json` | JSON Schema | Same order schema in JSON Schema format |

### Output

After running `test-split.sh`, the `output/` directory contains extracted sub-schemas:

```
output/
  avro/
    com_example_types_Address.avsc    # Address type
    com_example_types_Customer.avsc   # Customer (references Address)
    com_example_types_Money.avsc      # Money type
    com_example_types_LineItem.avsc   # LineItem (references Money)
    com_example_events_Order.avsc     # Root (references all above)
    manifest.json                     # Registration order and metadata
```

## Cleanup

```bash
docker compose down -v
rm -rf output/
```
