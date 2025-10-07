# app/controllers/ui_controller.py  (já acima, repetido aqui apenas para integridade)
import logging
from io import BytesIO
from flask import Blueprint, render_template, send_file, Response
from ..utils.zip_utils import empty_colors_json, build_template_zip_bytes

logger = logging.getLogger(__name__)
ui_bp = Blueprint("ui", __name__)

@ui_bp.get("/")
def index():
    logger.info("Render ingestion UI")
    return render_template("ingestion.html", empty_colors_json=empty_colors_json())

@ui_bp.get("/template.zip")
def template_zip():
    data = build_template_zip_bytes()
    return send_file(BytesIO(data), mimetype="application/zip", as_attachment=True, download_name="brand-ingestion-template.zip")

@ui_bp.get("/colors.json")
def colors_json():
    return Response(empty_colors_json(), mimetype="application/json")
