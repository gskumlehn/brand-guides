# app/controllers/ui_controller.py
import json
from flask import Blueprint, render_template, current_app

ui_bp = Blueprint("ui", __name__)

EMPTY_COLORS_JSON = {
    "primary":   {"name": "", "hex": ""},
    "secondary": {"name": "", "hex": ""},
    "others": []  # lista de {"name":"", "hex":""}
}

@ui_bp.route("/", methods=["GET"])
def index():
    current_app.logger.info("Render ingestion UI")
    # injeta o JSON de exemplo na página (mostra no <pre>{{ empty_colors_json }}</pre>)
    return render_template(
        "ingestion.html",
        empty_colors_json=json.dumps(EMPTY_COLORS_JSON, ensure_ascii=False, indent=2),
    )
