from flask import Flask
from flask_cors import CORS
import mimetypes

def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")

    CORS(
        app,
        resources={
            r"/lovable/*": {"origins": "*"},
            r"/stream/*":  {"origins": "*"},
            r"/files/*":   {"origins": "*"},
            r"/":          {"origins": "*"},
        },
        supports_credentials=False,
        allow_headers="*",
        methods=["GET","POST","OPTIONS"],
        expose_headers=["Content-Length","Content-Type"],
    )

    mimetypes.add_type("font/ttf", ".ttf")
    mimetypes.add_type("font/otf", ".otf")
    mimetypes.add_type("font/woff", ".woff")
    mimetypes.add_type("font/woff2", ".woff2")

    from .infra.db.bq_client import ensure_assets_table
    ensure_assets_table()

    from .controllers.ui_controller import ui_bp
    from .controllers.ingestion_controller import ingestion_bp
    from .controllers.lovable_controller import lovable_bp
    from .controllers.asset_delivery_controller import assets_bp

    app.register_blueprint(ui_bp)
    app.register_blueprint(ingestion_bp, url_prefix="/ingestions")
    app.register_blueprint(lovable_bp,   url_prefix="/lovable")
    app.register_blueprint(assets_bp)

    return app
