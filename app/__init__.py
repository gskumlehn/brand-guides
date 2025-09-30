from flask import Flask
from flask_cors import CORS

from .infra.db.bq_client import ensure_assets_tables

ALLOWED_ORIGINS = [
    r"https://.*\.lovableproject\.com",
    r"https://.*\.lovable\.app",
]

def create_app():
    app = Flask(__name__)

    # CORS global — não use função em `origins` (causa TypeError no flask-cors)
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

    # Garante tabelas/colunas no boot
    ensure_assets_tables()

    # Blueprints
    from .controllers.ui_controller import ui_bp
    from .controllers.lovable_controller import lovable_bp
    from .controllers.asset_delivery_controller import delivery_bp
    from .controllers.ingestion_controller import ingestion_bp

    app.register_blueprint(ui_bp)
    app.register_blueprint(lovable_bp, url_prefix="/lovable")
    app.register_blueprint(delivery_bp)
    app.register_blueprint(ingestion_bp, url_prefix="/ingest")

    return app
