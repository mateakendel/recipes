from flask import Blueprint, request, jsonify
from app.db import recipes, ratings
from bson.objectid import ObjectId
from bson.errors import InvalidId
from app.search import create_index, index_recipe, search_recipes
from app.cache import (
    get_cached_recipe,
    cache_recipe,
    delete_cached_recipe,
    get_recipe_views
)
from app.events import send_event
import time

recipes_bp = Blueprint("recipes", __name__)

create_index()


def safe_recipe(recipe):
    return {
        "_id": str(recipe.get("_id")),
        "title": recipe.get("title", ""),
        "ingredients": recipe.get("ingredients", []),
        "steps": recipe.get("steps", ""),
        "category": recipe.get("category", ""),
        "author": recipe.get("author", ""),
        "prep_time": recipe.get("prep_time", 0),
        "published": recipe.get("published", True)
    }


def get_rating_summary(recipe_id):
    result = list(ratings.aggregate([
        {"$match": {"recipe_id": recipe_id}},
        {
            "$group": {
                "_id": "$recipe_id",
                "average_rating": {"$avg": "$rating"},
                "rating_count": {"$sum": 1}
            }
        }
    ]))

    if not result:
        return {
            "average_rating": 0,
            "rating_count": 0
        }

    return {
        "average_rating": round(result[0]["average_rating"], 2),
        "rating_count": result[0]["rating_count"]
    }


def add_extra_data(recipe):
    recipe_id = recipe["_id"]

    rating_summary = get_rating_summary(recipe_id)

    recipe["average_rating"] = rating_summary["average_rating"]
    recipe["rating_count"] = rating_summary["rating_count"]
    recipe["views"] = get_recipe_views(recipe_id)

    return recipe


@recipes_bp.route("/recipes", methods=["POST"])
def add_recipe():
    data = request.json or {}

    title = data.get("title", "").strip()
    category = data.get("category", "").strip()
    author = data.get("author", "").strip()
    steps = data.get("steps", "").strip()

    ingredients = data.get("ingredients", [])
    ingredients = [i.strip() for i in ingredients if i.strip()]

    if not title or not category or not author or not steps or not ingredients:
        return {"message": "All fields are required"}, 400

    recipe = {
        "title": title,
        "ingredients": ingredients,
        "steps": steps,
        "category": category,
        "author": author,
        "prep_time": int(data.get("prep_time", 0)),
        "published": True,
        "created_at": time.time()
    }

    result = recipes.insert_one(recipe)

    index_recipe(str(result.inserted_id), recipe)

    return {"message": "Recipe added"}, 201


@recipes_bp.route("/recipes", methods=["GET"])
def get_recipes():
    mode = request.args.get("mode", "all")
    user = request.args.get("user")

    query = {"author": user} if mode == "mine" else {"published": True}

    out = []

    for r in recipes.find(query):
        recipe = safe_recipe(r)
        recipe = add_extra_data(recipe)
        out.append(recipe)

    return jsonify(out)


@recipes_bp.route("/recipes/search", methods=["GET"])
def search():
    q = request.args.get("q")
    category = request.args.get("category")
    username = request.args.get("user")

    send_event(
        event_type="recipe_search",
        recipe_id=None,
        username=username,
        extra={
            "query": q,
            "category": category
        }
    )

    results = search_recipes(
        q=q,
        category=category,
        max_time=int(request.args["time"]) if request.args.get("time") else None
    )

    out = []

    for r in results:
        recipe_id = r["_id"]

        try:
            db_recipe = recipes.find_one({"_id": ObjectId(recipe_id)})
        except Exception:
            db_recipe = None

        if db_recipe:
            recipe = safe_recipe(db_recipe)
            recipe = add_extra_data(recipe)
            out.append(recipe)

    return jsonify(out)


@recipes_bp.route("/recipes/reindex", methods=["POST"])
def reindex_all():
    count = 0

    for r in recipes.find({"published": True}):
        recipe_id = str(r["_id"])

        doc = {
            "title": r.get("title", ""),
            "ingredients": r.get("ingredients", []),
            "steps": r.get("steps", ""),
            "category": r.get("category", ""),
            "author": r.get("author", ""),
            "prep_time": r.get("prep_time", 0)
        }

        index_recipe(recipe_id, doc)
        count += 1

    return {"message": f"Reindexed {count} recipes"}, 200


@recipes_bp.route("/recipes/<recipe_id>", methods=["GET"])
def get_recipe_detail(recipe_id):
    try:
        object_id = ObjectId(recipe_id)
    except InvalidId:
        return {"message": "Invalid recipe id"}, 400

    username = request.args.get("user")

    send_event("recipe_view", recipe_id, username)

    cached = get_cached_recipe(recipe_id)

    if cached:
        cached["views"] = get_recipe_views(recipe_id)
        return jsonify(cached)

    recipe = recipes.find_one({"_id": object_id})

    if not recipe:
        return {"message": "Recipe not found"}, 404

    recipe = safe_recipe(recipe)
    recipe = add_extra_data(recipe)

    cache_recipe(recipe_id, recipe)

    return jsonify(recipe)


@recipes_bp.route("/recipes/<recipe_id>/rate", methods=["POST"])
def rate_recipe(recipe_id):
    data = request.json or {}

    username = data.get("username")
    rating_value = int(data.get("rating", 0))

    if not username:
        return {"message": "Username is required"}, 400

    if rating_value < 1 or rating_value > 5:
        return {"message": "Rating must be between 1 and 5"}, 400

    try:
        ObjectId(recipe_id)
    except InvalidId:
        return {"message": "Invalid recipe id"}, 400

    ratings.update_one(
        {
            "recipe_id": recipe_id,
            "username": username
        },
        {
            "$set": {
                "rating": rating_value,
                "updated_at": time.time()
            },
            "$setOnInsert": {
                "created_at": time.time()
            }
        },
        upsert=True
    )

    delete_cached_recipe(recipe_id)

    send_event(
        "recipe_rating",
        recipe_id,
        username,
        {"rating": rating_value}
    )

    rating_summary = get_rating_summary(recipe_id)

    return {
        "message": "Rating saved",
        "average_rating": rating_summary["average_rating"],
        "rating_count": rating_summary["rating_count"]
    }, 200


@recipes_bp.route("/recipes/<recipe_id>/rating", methods=["GET"])
def recipe_rating(recipe_id):
    username = request.args.get("username")

    rating_summary = get_rating_summary(recipe_id)

    user_rating = None

    if username:
        existing_rating = ratings.find_one({
            "recipe_id": recipe_id,
            "username": username
        })

        if existing_rating:
            user_rating = existing_rating["rating"]

    return {
        "average_rating": rating_summary["average_rating"],
        "rating_count": rating_summary["rating_count"],
        "user_rating": user_rating
    }


@recipes_bp.route("/recipes/popular", methods=["GET"])
def popular_recipes():
    out = []

    for r in recipes.find({"published": True}):
        recipe = safe_recipe(r)
        recipe = add_extra_data(recipe)

        recipe["popularity_score"] = round(
            recipe["views"] + recipe["rating_count"] + recipe["average_rating"] * 2,
            2
        )

        out.append(recipe)

    out.sort(key=lambda x: x["popularity_score"], reverse=True)

    return jsonify(out[:10])