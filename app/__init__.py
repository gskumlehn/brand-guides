import logging, os
from flask import Flask
from .config import load_config
from .infra.db.bq_client import init_bq
from .controllers.ui_controller import ui_bp
from .controllers.ingestion_controller import ingestion_bp
from .controllers.lovable_controller import lovable_bp
from .controllers.asset_delivery_controller import assets_bp

def create_app():
    logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL","INFO").upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    app = Flask(__name__, static_folder="public", template_folder="templates")

    app.config.update(load_config())
    init_bq(dataset=app.config["BQ_DATASET"], project=app.config["BQ_PROJECT"])

    app.register_blueprint(ui_bp)
    app.register_blueprint(ingestion_bp, url_prefix="/ingestions")
    app.register_blueprint(lovable_bp,   url_prefix="/lovable")
    app.register_blueprint(assets_bp)    # /files/* e /stream/*

    @app.get("/health")
    def health():
        return {"status": "ok"}, 200

    return app
