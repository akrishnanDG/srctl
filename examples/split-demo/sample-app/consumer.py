"""
Sample Avro Consumer using split schemas.

Demonstrates that NO CODE CHANGES are needed when switching from
a monolithic schema to split referenced schemas. The Confluent SerDes
handle reference resolution transparently.

Prerequisites:
  pip install confluent-kafka[avro]

Usage:
  python consumer.py
"""

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


# Configuration -- no special config needed for split schemas
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"
GROUP_ID = "split-schema-demo-consumer"


def main():
    print("=== Avro Consumer with Split Schemas ===")
    print(f"Kafka:           {KAFKA_BOOTSTRAP}")
    print(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"Topic:           {TOPIC}")
    print(f"Group:           {GROUP_ID}")
    print()

    # Schema Registry client
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # The key insight: AvroDeserializer resolves schema references
    # automatically. It reads the schema ID from the message header,
    # fetches the root schema, fetches all referenced schemas, and
    # deserializes. No code changes needed.
    avro_deserializer = AvroDeserializer(sr_client)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })

    consumer.subscribe([TOPIC])

    print(f"Consuming from topic '{TOPIC}'... (Ctrl+C to stop)\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            order = avro_deserializer(
                msg.value(),
                SerializationContext(TOPIC, MessageField.VALUE),
            )

            if order is not None:
                print(f"Order: {order['orderId']}")
                print(f"  Customer: {order['customer']['firstName']} {order['customer']['lastName']}")
                print(f"  Items: {len(order['items'])}")
                print(f"  Total: {order['total']['amount']} {order['total']['currency']}")
                print(f"  Status: {order['status']}")
                print()

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
