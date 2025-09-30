# app/__init__.py
import re
from flask import Flask
from flask_cors import CORS

def create_app():
    app = Flask(__name__)

    # --- CORS CORRETO (regex, sem função) ---
    allowed_origins = [
        r"https://.*\.lovableproject\.com",
        r"https://.*\.lovable\.app",
        r"https://.*\.lovable\.site",          # opcional se usar domínio novo
        r"http://localhost:\d+",               # útil p/ dev local
        "*"
    ]

    CORS(
        app,
        resources={
            r"/*": {
                "origins": allowed_origins,
                "methods": ["GET", "POST", "OPTIONS"],
                "allow_headers": ["Content-Type", "Authorization"],
                "expose_headers": ["Content-Length", "Content-Type"],
                "supports_credentials": False,  # não precisa cookie
                "send_wildcard": False,
                "vary_header": True,
            }
        },
    )
    # ----------------------------------------

    # Blueprints
    from .controllers.ui_controller import ui_bp
    from .controllers.ingestion_controller import ingestion_bp
    from .controllers.asset_delivery_controller import delivery_bp
    from .controllers.lovable_controller import lovable_bp

    app.register_blueprint(ui_bp)
    app.register_blueprint(ingestion_bp, url_prefix="/ingest")
    app.register_blueprint(delivery_bp)         # /stream e /files
    app.register_blueprint(lovable_bp, url_prefix="/lovable")

    # BigQuery: garantir tabela (opcional mover p/ 1a request)
    from .infra.db.bq_client import ensure_assets_table
    ensure_assets_table()

    return app
