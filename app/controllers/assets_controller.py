# app/controllers/assets_controller.py
import json
from flask import Blueprint, request, jsonify, Response
from ..services.assets_service import AssetsService
from ..repositories.storage_repository import build_prefix, stream as storage_stream

assets_bp = Blueprint("assets", __name__)
svc = AssetsService()

@assets_bp.get("/assets/sidebar")
def sidebar():
    brand = (request.args.get("brand_name") or "").strip()
    if not brand:
        return jsonify({"error": "brand_name é obrigatório"}), 400
    return jsonify(svc.sidebar(brand)), 200

@assets_bp.get("/assets/gallery")
def gallery():
    brand = (request.args.get("brand_name") or "").strip()
    if not brand:
        return jsonify({"error": "brand_name é obrigatório"}), 400
    return jsonify(svc.gallery(brand)), 200

@assets_bp.get("/assets/stream")
def stream():
    brand = (request.args.get("brand_name") or "").strip()
    cat   = (request.args.get("category_key") or "").strip() or None
    sub   = (request.args.get("subcategory_key") or "").strip() or None
    if not brand:
        return jsonify({"error": "brand_name é obrigatório"}), 400

    prefix = build_prefix(brand, cat, sub)
    def gen():
        for meta in storage_stream(prefix):
            yield json.dumps(meta, ensure_ascii=False) + "\n"
    return Response(gen(), mimetype="application/x-ndjson")
