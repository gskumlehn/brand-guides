# app/controllers/asset_delivery_controller.py
import io, re, os, zipfile, mimetypes
from typing import Optional, List
from flask import Blueprint, request, jsonify, send_file, abort, Response
from ..services.assets_service import AssetsService
from ..infra.bucket.gcs_client import GCSClient

delivery_bp = Blueprint("assets", __name__)
_service = AssetsService()
_gcs = GCSClient()
_BUCKET = os.getenv("GCS_BUCKET", "brand-guides")
SAFE_PATH_RE = re.compile(r"^[a-z0-9/_\-.@ ]+$", re.IGNORECASE)

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

    category_key: Optional[str] = (request.args.get("category_key") or "").strip().lower() or None

    sseq_raw = request.args.get("subcategory_seq")
    subcategory_seq: Optional[int] = None
    if sseq_raw is not None:
        sseq_raw = sseq_raw.strip()
        if re.fullmatch(r"\d+", sseq_raw):
            subcategory_seq = int(sseq_raw)
        else:
            return jsonify({"ok": False, "error": "subcategory_seq inválido"}), 400

    if subcategory_seq is not None and not category_key:
        return jsonify({"ok": False, "error": "category_key é obrigatório quando subcategory_seq é usado"}), 400

    try:
        return jsonify(_service.gallery(brand, category_key, subcategory_seq))
    except Exception as e:
        return jsonify({"ok": False, "error": f"falha em /assets/gallery: {e}"}), 500

@delivery_bp.get("/assets/colors")
def assets_colors():
    brand = (request.args.get("brand_name") or "").strip()
    if not brand:
        return jsonify({"ok": False, "error": "brand_name obrigatório"}), 400
    return jsonify(_service.colors(brand))

@delivery_bp.get("/assets/originais.zip")
def download_originais_zip():
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

@delivery_bp.get("/assets/stream")
def assets_stream():
    brand = (request.args.get("brand_name") or "").strip()
    path = (request.args.get("path") or "").strip()
    if not brand or not path:
        return jsonify({"ok": False, "error": "brand_name e path são obrigatórios"}), 400

    # segurança básica do caminho e escopo da marca
    if ".." in path or not path.lower().startswith(brand.lower() + "/") or not SAFE_PATH_RE.match(path):
        return abort(403)

    try:
        data = _gcs.read_bytes(_BUCKET, path)
    except Exception:
        return abort(404)

    ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
    resp: Response = send_file(
        io.BytesIO(data),
        mimetype=ctype,
        as_attachment=False,
        download_name=os.path.basename(path),
        max_age=60,
        conditional=True,
    )
    resp.headers["Cache-Control"] = "private, max-age=60"
    return resp

@delivery_bp.get("/assets/originais/exists")
def exists_originais():
    brand = (request.args.get("brand_name") or "").strip()
    category_key = (request.args.get("category_key") or "").strip().lower()
    if not brand or not category_key:
        return jsonify({"ok": False, "error": "brand_name e category_key são obrigatórios"}), 400
    res = _service.has_originais(brand, category_key)
    return jsonify(res), (200 if res.get("ok") else 500)