from flask import Blueprint, jsonify, request, Response
from flask_cors import cross_origin
from ..services.lovable_service import LovableService

lovable_bp = Blueprint("lovable", __name__)

@lovable_bp.after_request
def after_request(response):
    origin = request.headers.get('Origin')
    if origin and (
        origin.endswith('.lovableproject.com') or 
        origin.endswith('.lovable.app')
    ):
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Access-Control-Allow-Credentials'] = 'true'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response


@lovable_bp.get("/assets")
def list_assets():
    brand_name = request.args.get("brand_name")
    category = request.args.get("category")
    subcategory = request.args.get("subcategory")
    include_urls = (request.args.get("include_urls", "true").lower() == "true")

    if not brand_name or not category:
        return jsonify({ "error": "brand_name e category são obrigatórios" }), 400

    svc = LovableService()
    items = svc.get_assets(
        brand_name = brand_name,
        category   = category,
        subcategory = subcategory
    )

    if include_urls:
        for it in items:
            path = it.get("path")
            it["file_url"] = f"/files/{path}"
            it["stream_url"] = f"/stream/{path}"

    return jsonify(items)


@lovable_bp.get("/webfonts.css")
def webfonts_css():
    brand_name = request.args.get("brand_name")
    if not brand_name:
        return Response("/* brand_name é obrigatório */", mimetype="text/css", status=400)

    svc = LovableService()
    css = svc.generate_webfonts_css(
        brand_name = brand_name,
        prefer_stream = True
    )
    return Response(css, mimetype="text/css", status=200)
