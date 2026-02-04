# Example Schemas

This folder contains example Avro schemas demonstrating schema references and evolution.

## Schema Hierarchy

```
address (base schema)
    └── person (references address)
            └── order (references person and address)
```

## Files

### Base Schema
- `address-v1.avsc` - Initial address schema with street, city, country
- `address-v2.avsc` - Evolved schema with optional zipCode field (backward compatible)

### Schema with Single Reference
- `person-v1.avsc` - Person schema referencing Address
- `person-v2.avsc` - Evolved with optional phone field (backward compatible)

### Schema with Multiple References
- `order-v1.avsc` - Order schema referencing both Person and Address

## Usage Example

```bash
# Register base schema first
srctl register address-value --file examples/address-v1.avsc

# Evolve the schema
srctl register address-value --file examples/address-v2.avsc

# Register schema with reference
srctl register person-value --file examples/person-v1.avsc \
  --ref "com.example.Address=address-value:2"

# Register schema with multiple references
srctl register order-value --file examples/order-v1.avsc \
  --ref "com.example.Address=address-value:2" \
  --ref "com.example.Person=person-value:1"
```
