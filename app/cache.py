import redis
import json
import os

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)


def get_cached_recipe(recipe_id):
    key = f"recipe:{recipe_id}"
    data = redis_client.get(key)

    if data:
        return json.loads(data)

    return None


def cache_recipe(recipe_id, recipe):
    key = f"recipe:{recipe_id}"

    redis_client.setex(
        key,
        300,
        json.dumps(recipe)
    )


def delete_cached_recipe(recipe_id):
    key = f"recipe:{recipe_id}"
    redis_client.delete(key)


def increment_recipe_views(recipe_id):
    key = f"recipe:views:{recipe_id}"
    return redis_client.incr(key)


def get_recipe_views(recipe_id):
    key = f"recipe:views:{recipe_id}"
    value = redis_client.get(key)
    return int(value) if value else 0