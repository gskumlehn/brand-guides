# app/controllers/ingestion_controller.py
from io import BytesIO
from flask import Blueprint, request, jsonify, send_file
from ..services.ingestion_service import IngestionService
from ..utils.zip_utils import build_template_zip_bytes

ingestion_bp = Blueprint("ingestion", __name__)
_service = IngestionService()

@ingestion_bp.post("/ingest")
def ingest_zip():
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "Campo 'file' obrigatório"}), 400
    brand_name = (request.form.get("brand_name") or "").strip()
    if not brand_name:
        return jsonify({"ok": False, "error": "brand_name obrigatório"}), 400
    res = _service.ingest_zip(brand_name, request.files["file"])
    return jsonify(res), (200 if res.get("ok") else 400)

@ingestion_bp.get("/template.zip")
def get_template_zip():
    return send_file(BytesIO(build_template_zip_bytes()), mimetype="application/zip", as_attachment=True, download_name="brand-ingestion-template.zip")
