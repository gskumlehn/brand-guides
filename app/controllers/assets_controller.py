# app/controllers/asset_delivery_controller.py
import io
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


@delivery_bp.get("/assets/gallery")
def assets_gallery():
    brand = (request.args.get("brand_name") or "").strip()
    if not brand:
        return jsonify({"ok": False, "error": "brand_name obrigatório"}), 400
    category_key: Optional[str] = request.args.get("category_key")
    sseq_raw = request.args.get("subcategory_seq")
    subcategory_seq: Optional[int] = int(sseq_raw) if (sseq_raw and sseq_raw.isdigit()) else None
    return jsonify(_service.gallery(brand, category_key, subcategory_seq))


@delivery_bp.get("/assets/originais.zip")
def download_originais_zip():
    """
    Gera um .zip com os arquivos de 'originais' de uma categoria:
      GET /assets/originais.zip?brand_name=CCBA&category_key=logo
    """
    brand = (request.args.get("brand_name") or "").strip()
    category_key = (request.args.get("category_key") or "").strip().lower()
    if not brand or not category_key:
        return jsonify({"ok": False, "error": "brand_name e category_key são obrigatórios"}), 400

    prefix = f"{brand.lower()}/{category_key}/originais/"
    paths: List[str] = _gcs.list_paths(_BUCKET, prefix)

    if not paths:
        # Sem originais — retorna zip vazio mesmo assim
        paths = []

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in paths:
            try:
                data = _gcs.read_bytes(_BUCKET, p)
                # nome do arquivo dentro do zip sem o prefixo
                arcname = p[len(prefix):] if p.startswith(prefix) else os.path.basename(p)
                z.writestr(arcname, data)
            except Exception as e:
                # ignora arquivos com erro de leitura
                continue

    mem.seek(0)
    fname = f"{brand.lower()}-{category_key}-originais.zip"
    return send_file(
        mem,
        mimetype="application/zip",
        as_attachment=True,
        download_name=fname
    )
