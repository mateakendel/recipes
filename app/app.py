from flask import Flask, render_template
from app.auth import auth_bp
from app.recipes import recipes_bp
from app.recommendations import recommendations_bp
import time

app = Flask(__name__, template_folder="templates", static_folder="static")

app.register_blueprint(auth_bp)
app.register_blueprint(recipes_bp)
app.register_blueprint(recommendations_bp)


@app.route("/")
def index():
    return render_template("main_home.html")


@app.route("/login-page")
def login_page():
    return render_template("login.html")


@app.route("/register-page")
def register_page():
    return render_template("register.html")


@app.route("/home")
def home():
    return render_template("home.html")


@app.route("/all-recipes")
def all_recipes():
    return render_template("all_recipes.html")


@app.route("/recipe/<recipe_id>")
def recipe_detail(recipe_id):
    return render_template("recipe_detail.html", recipe_id=recipe_id)


@app.route("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    time.sleep(2)
    app.run(host="0.0.0.0", port=80)