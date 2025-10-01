# app/__init__.py
import os
from pathlib import Path
from flask import Flask, jsonify
from flask_cors import CORS

ALLOWED_ORIGINS = [
    r"https://.*\.lovableproject\.com$",
    r"https://.*\.lovable\.app$",
    r"https?://localhost(:\d+)?$",
    r"https?://127\.0\.0\.1(:\d+)?$",
]

def create_app():
    base_dir = Path(__file__).resolve().parent
    templates_dir = str(base_dir / "templates")
    static_dir    = str(base_dir / "public")

    app = Flask(
        __name__,
        template_folder=templates_dir,   # evita procurar em app/app/templates
        static_folder=static_dir,        # evita procurar em app/app/public
    )

    # CORS global (regex como string, nada de função em `origins`)
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

    # Blueprints
    from .controllers.ui_controller import ui_bp
    from .controllers.lovable_controller import lovable_bp
    from .controllers.asset_delivery_controller import delivery_bp
    from .controllers.ingestion_controller import ingestion_bp

    app.register_blueprint(ui_bp)                              # “/”
    app.register_blueprint(lovable_bp,   url_prefix="/lovable")
    app.register_blueprint(delivery_bp)                        # /files, /stream
    app.register_blueprint(ingestion_bp, url_prefix="/ingestion")

    # health endpoints
    @app.get("/healthz")
    def healthz():
        return jsonify(status="ok")

    @app.get("/readyz")
    def readyz():
        bq_ok, gcs_ok = True, True
        try:
            from .infra.db.bq_client import client as bq_client
            _ = bq_client()
        except Exception:
            bq_ok = False
        try:
            from .infra.bucket.gcs_client import GCSClient
            GCSClient().ping()
        except Exception:
            gcs_ok = False
        return jsonify(bq=bq_ok, gcs=gcs_ok), (200 if bq_ok and gcs_ok else 503)

    return app
