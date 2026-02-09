"""
Sample Avro Producer using split schemas.

Demonstrates that NO CODE CHANGES are needed when switching from
a monolithic schema to split referenced schemas. The Confluent SerDes
handle reference resolution transparently.

Prerequisites:
  pip install confluent-kafka[avro]

Usage:
  # First, register the split schemas using srctl:
  #   srctl split register --file ../schemas/order-monolithic.avsc --subject orders-value
  #
  # Then run this producer:
  python producer.py
"""

import json
import uuid
from datetime import datetime

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# Configuration -- no special config needed for split schemas
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"


def get_schema_str():
    """
    Fetch the schema from Schema Registry. The registry resolves all
    references automatically -- the producer doesn't need to know
    whether the schema is monolithic or split.
    """
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    # Get the latest version of the root schema
    schema = sr_client.get_latest_version("orders-value")
    return schema.schema.schema_str


def create_sample_order():
    """Create a sample order record."""
    return {
        "orderId": str(uuid.uuid4()),
        "orderDate": datetime.now().isoformat(),
        "status": "PENDING",
        "customer": {
            "customerId": "CUST-001",
            "firstName": "Jane",
            "lastName": "Doe",
            "email": "jane.doe@example.com",
            "phone": {"string": "+1-555-0100"},
            "billingAddress": {
                "street": "123 Main St",
                "street2": None,
                "city": "Springfield",
                "state": "IL",
                "zip": "62701",
                "country": "US",
            },
            "shippingAddress": None,
        },
        "items": [
            {
                "productId": "PROD-001",
                "productName": "Widget A",
                "quantity": 2,
                "unitPrice": {"amount": 29.99, "currency": "USD"},
                "discount": None,
            },
            {
                "productId": "PROD-002",
                "productName": "Widget B",
                "quantity": 1,
                "unitPrice": {"amount": 49.99, "currency": "USD"},
                "discount": {"com.example.types.Money": {"amount": 5.00, "currency": "USD"}},
            },
        ],
        "subtotal": {"amount": 109.97, "currency": "USD"},
        "tax": {"amount": 8.80, "currency": "USD"},
        "total": {"amount": 118.77, "currency": "USD"},
        "payment": {
            "method": "CREDIT_CARD",
            "transactionId": {"string": "TXN-" + str(uuid.uuid4())[:8]},
            "amount": {"amount": 118.77, "currency": "USD"},
        },
        "notes": None,
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def main():
    print("=== Avro Producer with Split Schemas ===")
    print(f"Kafka:           {KAFKA_BOOTSTRAP}")
    print(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"Topic:           {TOPIC}")
    print()

    # Schema Registry client
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # The key insight: we use the SAME AvroSerializer regardless of
    # whether the schema is monolithic or split. The SerDe handles
    # reference resolution transparently.
    schema_str = get_schema_str()
    avro_serializer = AvroSerializer(sr_client, schema_str)
    string_serializer = StringSerializer("utf_8")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    # Produce 5 sample orders
    for i in range(5):
        order = create_sample_order()
        print(f"Producing order {i+1}: {order['orderId']}")

        producer.produce(
            topic=TOPIC,
            key=string_serializer(order["orderId"]),
            value=avro_serializer(
                order, SerializationContext(TOPIC, MessageField.VALUE)
            ),
            on_delivery=delivery_report,
        )

    producer.flush()
    print(f"\nProduced 5 orders to topic '{TOPIC}'")


if __name__ == "__main__":
    main()
