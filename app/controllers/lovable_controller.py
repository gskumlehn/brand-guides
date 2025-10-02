from flask import Blueprint, request, jsonify
from ..services.lovable_service import LovableService

lovable_bp = Blueprint("lovable", __name__, url_prefix="/lovable")
svc = LovableService()

@lovable_bp.get("/assets")
def list_assets():
    brand_name = request.args.get("brand_name") or request.args.get("brand")
    category = request.args.get("category")

    if not brand_name or not category:
        return jsonify({"ok": False, "error": "Parâmetros obrigatórios: brand_name e category."}), 400

    items = svc.get_assets(brand_name=brand_name, category=category)
    return jsonify({"ok": True, "items": items})

@lovable_bp.get("/colors")
def list_colors():
    brand_name = request.args.get("brand_name") or request.args.get("brand")
    if not brand_name:
        return jsonify({"ok": False, "error": "Parâmetro obrigatório: brand_name."}), 400

    items = svc.get_colors(brand_name=brand_name)
    return jsonify({"ok": True, "items": items})
