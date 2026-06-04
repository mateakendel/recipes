from flask import Blueprint, request, jsonify
from app.db import users
import hashlib


auth_bp = Blueprint("auth", __name__)

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

@auth_bp.route("/register", methods=["POST"])
def register():
    data = request.json
    if users.find_one({"username": data["username"]}):
        return {"message": "User exists"}, 400

    users.insert_one({
        "username": data["username"],
        "password": hash_password(data["password"])
    })
    return {"message": "User created"}, 201

@auth_bp.route("/login", methods=["POST"])
def login():
    user = users.find_one({
        "username": request.json["username"],
        "password": hash_password(request.json["password"])
    })
    if not user:
        return {"message": "Invalid credentials"}, 401
    return {"message": "Login OK"}, 200
