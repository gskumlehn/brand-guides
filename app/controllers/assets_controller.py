# app/controllers/asset_delivery_controller.py
from flask import Blueprint, request, jsonify
from typing import Optional
from ..services.assets_service import AssetsService

delivery_bp = Blueprint("assets", __name__)
_service = AssetsService()

@delivery_bp.get("/assets/sidebar")
def assets_sidebar():
    brand = (request.args.get("brand_name") or "").strip()
    if not brand:
        return jsonify({"ok": False, "error": "brand_name obrigatório"}), 400
    return jsonify(_service.sidebar(brand))

@delivery_bp.get("/assets/gallery")
def assets_gallery():
    brand = (request.args.get("brand_name") or "").strip()
    if not brand:
        return jsonify({"ok": False, "error": "brand_name obrigatório"}), 400

    category_key: Optional[str] = request.args.get("category_key")
    sseq_raw = request.args.get("subcategory_seq")
    subcategory_seq: Optional[int] = int(sseq_raw) if (sseq_raw and sseq_raw.isdigit()) else None

    return jsonify(_service.gallery(brand, category_key, subcategory_seq))
