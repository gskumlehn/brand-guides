from flask import Blueprint, request, jsonify
from ..services.ingestion_service import IngestionService

ingestion_bp = Blueprint("ingestions", __name__)

@ingestion_bp.post("/upload-zip")
def upload_zip():
    f = request.files.get("file")
    brand_name = request.form.get("brand_name") or request.form.get("nome_marca")
    if not brand_name or not f or not f.filename.lower().endswith(".zip"):
        return jsonify({"error": "Envie nome da marca e um .zip válido"}), 400
    svc = IngestionService()  # lazy
    res = svc.ingest_zip(brand_name=brand_name, zip_file=f)
    return jsonify(res), 201
