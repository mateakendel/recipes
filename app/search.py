from elasticsearch import Elasticsearch
import time
import os

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
INDEX_NAME = "recipes"

es = Elasticsearch(ES_HOST)


def wait_for_es():
    for _ in range(60):
        try:
            if es.ping():
                return True
        except Exception:
            pass

        time.sleep(2)

    print("Elasticsearch not available, continuing without search.")
    return False


def create_index():
    available = wait_for_es()

    if not available:
        return

    try:
        if es.indices.exists(index=INDEX_NAME):
            return

        es.indices.create(
            index=INDEX_NAME,
            mappings={
                "properties": {
                    "title": {"type": "text"},
                    "ingredients": {"type": "text"},
                    "steps": {"type": "text"},
                    "category": {"type": "text"},
                    "author": {"type": "keyword"},
                    "prep_time": {"type": "integer"}
                }
            }
        )

    except Exception as e:
        print("Elasticsearch index creation failed:", e)


def normalize_recipe(recipe):
    return {
        "title": recipe.get("title", "").lower(),
        "ingredients": " ".join(i.lower() for i in recipe.get("ingredients", [])),
        "steps": recipe.get("steps", "").lower(),
        "category": recipe.get("category", "").lower(),
        "author": recipe.get("author", ""),
        "prep_time": recipe.get("prep_time", 0)
    }


def index_recipe(recipe_id, recipe):
    try:
        if not es.ping():
            print("Elasticsearch not ready, recipe was saved only to MongoDB.")
            return

        es.index(
            index=INDEX_NAME,
            id=str(recipe_id),
            document=normalize_recipe(recipe),
            refresh=True
        )

    except Exception as e:
        print("Elasticsearch indexing failed:", e)


def search_recipes(q=None, category=None, max_time=None):
    try:
        if not es.ping():
            return []

        must = []

        if q:
            must.append({
                "multi_match": {
                    "query": q.lower(),
                    "fields": ["title", "ingredients", "steps"]
                }
            })

        if category:
            must.append({"match": {"category": category.lower()}})

        if max_time is not None:
            must.append({"range": {"prep_time": {"lte": max_time}}})

        query = {"bool": {"must": must}} if must else {"match_all": {}}

        res = es.search(index=INDEX_NAME, query=query)

        return [
            {"_id": hit["_id"], **hit["_source"]}
            for hit in res["hits"]["hits"]
        ]

    except Exception as e:
        print("Elasticsearch search failed:", e)
        return []