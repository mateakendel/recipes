import os
import joblib
from bson.objectid import ObjectId

MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/recipe_nb_model.joblib")


def load_model():
    if not os.path.exists(MODEL_PATH):
        return None

    try:
        return joblib.load(MODEL_PATH)
    except Exception as e:
        print("Model loading failed:", e)
        return None


def recipe_to_text(recipe):
    title = recipe.get("title", "")
    category = recipe.get("category", "")
    ingredients = " ".join(recipe.get("ingredients", []))
    steps = recipe.get("steps", "")

    return f"{title} {category} {ingredients} {steps}".lower()


def build_user_profile_text(username, ratings_collection, recipes_collection, exclude_recipe_id=None):
    if not username:
        return ""

    positive_categories = []
    positive_ingredients = []
    negative_categories = []
    negative_ingredients = []

    user_ratings = ratings_collection.find({"username": username})

    for rating in user_ratings:
        recipe_id = rating.get("recipe_id")

        if exclude_recipe_id and recipe_id == exclude_recipe_id:
            continue

        try:
            recipe = recipes_collection.find_one({"_id": ObjectId(recipe_id)})
        except Exception:
            recipe = None

        if not recipe:
            continue

        rating_value = int(rating.get("rating", 0))
        category = recipe.get("category", "")
        ingredients = recipe.get("ingredients", [])

        if rating_value >= 4:
            positive_categories.append(category)
            positive_ingredients.extend(ingredients)
        else:
            negative_categories.append(category)
            negative_ingredients.extend(ingredients)

    profile_text = f"""
    user {username}
    likes categories {' '.join(positive_categories)}
    likes ingredients {' '.join(positive_ingredients)}
    dislikes categories {' '.join(negative_categories)}
    dislikes ingredients {' '.join(negative_ingredients)}
    """

    return profile_text.lower()


def build_model_input(
    username,
    recipe,
    ratings_collection=None,
    recipes_collection=None,
    exclude_recipe_id=None
):
    recipe_text = recipe_to_text(recipe)

    if ratings_collection is None or recipes_collection is None:
        return f"user {username} {recipe_text}".lower()

    user_profile = build_user_profile_text(
        username=username,
        ratings_collection=ratings_collection,
        recipes_collection=recipes_collection,
        exclude_recipe_id=exclude_recipe_id
    )

    return f"user {username} {user_profile} recipe {recipe_text}".lower()