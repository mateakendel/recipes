from pymongo import MongoClient
import pandas as pd
import time
import joblib
import os

from bson.objectid import ObjectId

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, precision_score, classification_report

from app.ml_model import build_model_input

MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/recipe_nb_model.joblib")

client = MongoClient(MONGO_URI)
db = client["recepti_db"]

recipes = db.recipes
ratings = db.ratings
ml_datasets = db.ml_datasets
model_metrics = db.model_metrics


def build_dataset():
    rows = []

    for rating in ratings.find():
        recipe_id = rating.get("recipe_id")
        username = rating.get("username")
        rating_value = int(rating.get("rating", 0))

        if not recipe_id or not username:
            continue

        try:
            recipe = recipes.find_one({"_id": ObjectId(recipe_id)})
        except Exception:
            recipe = None

        if not recipe:
            continue

        text = build_model_input(
            username=username,
            recipe=recipe,
            ratings_collection=ratings,
            recipes_collection=recipes,
            exclude_recipe_id=recipe_id
        )

        if not text.strip():
            continue

        label = 1 if rating_value >= 4 else 0

        rows.append({
            "username": username,
            "recipe_id": recipe_id,
            "rating": rating_value,
            "text": text,
            "label": label
        })

    return pd.DataFrame(rows)


def train():
    df = build_dataset()

    if len(df) < 10:
        print("Not enough data for training.")
        print("First run data generator:")
        print("docker compose exec recepti_app python -m scripts.data_generator")
        return

    if df["label"].nunique() < 2:
        print("Dataset has only one class. Need both positive and negative ratings.")
        return

    ml_datasets.insert_one({
        "created_at": time.time(),
        "rows": len(df),
        "positive_count": int(df["label"].sum()),
        "negative_count": int((df["label"] == 0).sum()),
        "features": "username + user preference profile + recipe title + category + ingredients + steps",
        "label_definition": "rating >= 4 is positive class 1, rating <= 3 is negative class 0"
    })

    X = df["text"]
    y = df["label"]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.25,
        random_state=42,
        stratify=y
    )

    pipeline = Pipeline([
        ("tfidf", TfidfVectorizer(
            max_features=2000,
            ngram_range=(1, 2)
        )),
        ("model", MultinomialNB())
    ])

    pipeline.fit(X_train, y_train)

    predictions = pipeline.predict(X_test)

    accuracy = accuracy_score(y_test, predictions)
    precision = precision_score(y_test, predictions, zero_division=0)

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)

    metrics = {
        "trained_at": time.time(),
        "dataset_size": int(len(df)),
        "train_size": int(len(X_train)),
        "test_size": int(len(X_test)),
        "accuracy": round(float(accuracy), 3),
        "precision": round(float(precision), 3),
        "model_path": MODEL_PATH,
        "model_type": "TF-IDF + Multinomial Naive Bayes",
        "features": "username + user preference profile + recipe text"
    }

    model_metrics.insert_one(metrics)

    print("Model trained successfully.")
    print("Metrics:")
    print(metrics)
    print()
    print(classification_report(y_test, predictions, zero_division=0))


if __name__ == "__main__":
    train()