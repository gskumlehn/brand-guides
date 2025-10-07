# app/__init__.py
import re
from flask import Flask
from flask_cors import CORS
from .infra.db.bq_client import ensure_assets_tables

ALLOWED_ORIGINS = [
    r"https://.*\.lovableproject\.com",
    r"https://.*\.lovable\.app",
]

def create_app():
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="public",
    )

    CORS(
        app,
        resources={r"/*": {
            "origins": ALLOWED_ORIGINS,
            "supports_credentials": True,
            "allow_headers": ["Content-Type", "Authorization"],
            "methods": ["GET", "POST", "OPTIONS"],
            "expose_headers": ["Content-Length", "Content-Type"],
        }},
    )

    ensure_assets_tables()

    from .controllers.ui_controller import ui_bp
    from .controllers.ingestion_controller import ingestion_bp
    from .controllers.assets_controller import assets_bp

    app.register_blueprint(ui_bp)
    app.register_blueprint(ingestion_bp, url_prefix="/ingest")
    app.register_blueprint(assets_bp)

    return app
