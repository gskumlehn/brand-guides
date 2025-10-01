import logging
from flask import Blueprint, render_template
from ..utils.zip_utils import empty_colors_json

logger = logging.getLogger(__name__)
ui_bp = Blueprint("ui", __name__)

@ui_bp.get("/")
def index():
    logger.info("Render ingestion UI")
    return render_template("ingestion.html", empty_colors_json=empty_colors_json())
