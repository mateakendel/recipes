from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
import os
import redis

KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka:9092")
MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
TOPIC_NAME = "recipe-events"

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["recepti_db"]

recipe_events = db.recipe_events
ratings = db.ratings

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)


def wait_for_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_HOST,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="recipe-analytics-consumer"
            )
            return consumer
        except Exception as e:
            print("Waiting for Kafka:", e)
            time.sleep(3)


def save_event(event):
    event_id = event.get("event_id")

    if event_id:
        existing = recipe_events.find_one({"event_id": event_id})
        if existing:
            return False

    recipe_events.insert_one(event)
    return True


def process_event(event):
    inserted = save_event(event)

    if not inserted:
        return

    event_type = event.get("event_type")
    recipe_id = event.get("recipe_id")
    username = event.get("username")
    extra = event.get("extra", {})

    if event_type == "recipe_view" and recipe_id:
        redis_client.incr(f"recipe:views:{recipe_id}")

    if event_type == "recipe_rating" and recipe_id and username:
        rating = int(extra.get("rating", 0))

        if rating >= 1 and rating <= 5:
            ratings.update_one(
                {
                    "recipe_id": recipe_id,
                    "username": username
                },
                {
                    "$set": {
                        "rating": rating,
                        "updated_at": event.get("timestamp", time.time())
                    },
                    "$setOnInsert": {
                        "created_at": event.get("timestamp", time.time())
                    }
                },
                upsert=True
            )


def main():
    consumer = wait_for_kafka()

    print("Kafka consumer started.")

    for message in consumer:
        event = message.value
        print("Received:", event)
        process_event(event)


if __name__ == "__main__":
    main()