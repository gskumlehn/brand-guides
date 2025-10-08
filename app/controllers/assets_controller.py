# app/controllers/asset_delivery_controller.py
from flask import Blueprint, request, jsonify, redirect
from typing import Optional
from ..services.assets_service import AssetsService
from ..infra.bucket.gcs_client import GCSClient
import os

delivery_bp = Blueprint("assets", __name__)
_service = AssetsService()
_gcs = GCSClient()
_BUCKET = os.getenv("GCS_BUCKET", "brand-guides")


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

@delivery_bp.get("/assets/file")
def assets_file():
    """
    Redireciona (302) para uma URL assinada do objeto GCS.
    Requer: ?path=<path no bucket>  (ex.: ccba/logo/01-principal-01/01.png)
    Opcional: &download=1 (força attachment)
    """
    path = (request.args.get("path") or "").strip()
    if not path or "/" not in path:
        return jsonify({"ok": False, "error": "path inválido"}), 400

    # (Opcional) checagem simples de sanitização: sem caminhos ascendentes
    if ".." in path or path.startswith("/"):
        return jsonify({"ok": False, "error": "path inválido"}), 400

    as_attachment = (request.args.get("download") == "1")
    url = _gcs.signed_url(_BUCKET, path, minutes=15, as_attachment=as_attachment)
    return redirect(url, code=302)
