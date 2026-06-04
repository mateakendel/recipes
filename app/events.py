from kafka import KafkaProducer
import json
import time
import os
import uuid

KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka:9092")
TOPIC_NAME = "recipe-events"

producer = None
kafka_disabled = False


def get_producer():
    global producer, kafka_disabled

    if kafka_disabled:
        return None

    if producer is not None:
        return producer

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_HOST,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=1000,
            api_version_auto_timeout_ms=1000,
            max_block_ms=1000,
            retries=0
        )
        return producer

    except Exception as e:
        print("Kafka not available:", e)
        kafka_disabled = True
        return None


def send_event(event_type, recipe_id=None, username=None, extra=None):
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "recipe_id": recipe_id,
        "username": username,
        "timestamp": time.time(),
        "extra": extra or {}
    }

    producer_instance = get_producer()

    if producer_instance:
        try:
            producer_instance.send(TOPIC_NAME, event)
        except Exception as e:
            print("Kafka send failed:", e)

    return event