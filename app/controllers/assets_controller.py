# app/controllers/asset_delivery_controller.py
import io, re
import os
import zipfile
from flask import Blueprint, request, jsonify, send_file
from typing import Optional, List
from ..services.assets_service import AssetsService
from ..infra.bucket.gcs_client import GCSClient

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


# app/controllers/asset_delivery_controller.py  (apenas a função /assets/gallery)
@delivery_bp.get("/assets/gallery")
def assets_gallery():
    brand = (request.args.get("brand_name") or "").strip()
    if not brand:
        return jsonify({"ok": False, "error": "brand_name obrigatório"}), 400

    category_key = (request.args.get("category_key") or "").strip().lower() or None

    sseq_raw = request.args.get("subcategory_seq")
    subcategory_seq = None
    if sseq_raw is not None:
        sseq_raw = sseq_raw.strip()
        # aceita "01", "1", etc.
        if re.fullmatch(r"\d+", sseq_raw):
            subcategory_seq = int(sseq_raw)
        else:
            return jsonify({"ok": False, "error": "subcategory_seq inválido"}), 400

    # se pediu subcategoria, categoria é obrigatória (evita 500 por consulta ambígua)
    if subcategory_seq is not None and not category_key:
        return jsonify({"ok": False, "error": "category_key é obrigatório quando subcategory_seq é usado"}), 400

    try:
        return jsonify(_service.gallery(brand, category_key, subcategory_seq))
    except Exception as e:
        # erro defensivo (evita HTML 500 do Flask e devolve JSON)
        return jsonify({"ok": False, "error": f"falha em /assets/gallery: {e}"}), 500



@delivery_bp.get("/assets/colors")
def assets_colors():
    """
    Tabela de cores por marca, com textos (principal/secundaria) e grupos.
    GET /assets/colors?brand_name=CCBA
    """
    brand = (request.args.get("brand_name") or "").strip()
    if not brand:
        return jsonify({"ok": False, "error": "brand_name obrigatório"}), 400
    return jsonify(_service.colors(brand))


@delivery_bp.get("/assets/originais.zip")
def download_originais_zip():
    """
    Download do pacote 'originais' de uma categoria.
    GET /assets/originais.zip?brand_name=CCBA&category_key=logo
    """
    brand = (request.args.get("brand_name") or "").strip()
    category_key = (request.args.get("category_key") or "").strip().lower()
    if not brand or not category_key:
        return jsonify({"ok": False, "error": "brand_name e category_key são obrigatórios"}), 400

    prefix = f"{brand.lower()}/{category_key}/originais/"
    paths: List[str] = _gcs.list_paths(_BUCKET, prefix)

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in paths:
            try:
                data = _gcs.read_bytes(_BUCKET, p)
                arcname = p[len(prefix):] if p.startswith(prefix) else os.path.basename(p)
                z.writestr(arcname, data)
            except Exception:
                continue

    mem.seek(0)
    fname = f"{brand.lower()}-{category_key}-originais.zip"
    return send_file(mem, mimetype="application/zip", as_attachment=True, download_name=fname)
