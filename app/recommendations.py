from flask import Blueprint, request, jsonify
from app.db import recipes, ratings, model_metrics
from app.ml_model import load_model, build_model_input
from app.cache import get_recipe_views
from bson.objectid import ObjectId

recommendations_bp = Blueprint("recommendations", __name__)

model = None


def get_model():
    global model

    if model is None:
        model = load_model()

    return model


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


@recommendations_bp.route("/recommendations", methods=["GET"])
def recommendations():
    username = request.args.get("user")

    clf = get_model()

    if clf is None:
        return {
            "message": "Model is not trained yet. Run: docker compose exec recepti_app python -m scripts.train_model",
            "recommendations": []
        }, 200

    user_rated_ids = set()

    if username:
        for r in ratings.find({"username": username}):
            user_rated_ids.add(r["recipe_id"])

    output = []

    for recipe in recipes.find({"published": True}):
        recipe_id = str(recipe["_id"])

        if recipe_id in user_rated_ids:
            continue

        text = build_model_input(
            username=username,
            recipe=recipe,
            ratings_collection=ratings,
            recipes_collection=recipes
        )

        try:
            probability = clf.predict_proba([text])[0][1]
            prediction = int(clf.predict([text])[0])
        except Exception:
            probability = 0
            prediction = 0

        rating_summary = get_rating_summary(recipe_id)

        item = {
            "_id": recipe_id,
            "title": recipe.get("title", ""),
            "category": recipe.get("category", ""),
            "author": recipe.get("author", ""),
            "views": get_recipe_views(recipe_id),
            "average_rating": rating_summary["average_rating"],
            "rating_count": rating_summary["rating_count"],
            "liked_prediction": prediction,
            "like_probability": round(float(probability), 3)
        }

        output.append(item)

    output.sort(key=lambda x: x["like_probability"], reverse=True)

    latest_metrics = model_metrics.find_one(sort=[("trained_at", -1)])

    metrics = None
    if latest_metrics:
        metrics = {
            "accuracy": latest_metrics.get("accuracy"),
            "precision": latest_metrics.get("precision"),
            "dataset_size": latest_metrics.get("dataset_size"),
            "trained_at": latest_metrics.get("trained_at"),
            "model_type": latest_metrics.get("model_type"),
            "features": latest_metrics.get("features")
        }

    return {
        "user": username,
        "model_metrics": metrics,
        "recommendations": output[:10]
    }