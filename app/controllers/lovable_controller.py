# app/controllers/lovable_controller.py
from flask import Blueprint, request, jsonify
from ..services.lovable_service import LovableService

lovable_bp = Blueprint("lovable", __name__)
svc = LovableService()

@lovable_bp.route("/assets", methods=["GET"])
def list_assets():
    brand_name = request.args.get("brand_name")
    category   = request.args.get("category")
    subcategory = request.args.get("subcategory")

    items = svc.get_assets(brand_name=brand_name, category=category, subcategory=subcategory)

    return jsonify(items)

@lovable_bp.get("/colors")
def list_colors():
    brand_name = request.args.get("brand_name")
    if not brand_name:
        return jsonify({"error": "brand_name is required"}), 400
    svc = LovableService()
    return jsonify(svc.get_colors(brand_name))