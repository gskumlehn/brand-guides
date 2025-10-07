# app/controllers/assets_controller.py
from flask import Blueprint, request, jsonify
from ..services.assets_service import AssetsService

assets_bp = Blueprint("assets", __name__)
_service = AssetsService()

@assets_bp.get("/assets/sidebar")
def sidebar():
    brand = (request.args.get("brand_name") or "").strip()
    if not brand:
        return jsonify({"ok": False, "error": "brand_name obrigatório"}), 400
    return jsonify(_service.sidebar(brand))

@assets_bp.get("/assets/gallery")
def gallery():
    brand = (request.args.get("brand_name") or "").strip()
    if not brand:
        return jsonify({"ok": False, "error": "brand_name obrigatório"}), 400
    return jsonify(_service.gallery(brand))
