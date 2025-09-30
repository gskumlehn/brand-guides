from flask import Flask
from flask_cors import CORS
import mimetypes

def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")

    # CORS: liberar apenas o necessário (sem credenciais)
    CORS(
        app,
        resources={
            r"/lovable/*": {"origins": "*"},
            r"/stream/*":   {"origins": "*"},
            r"/files/*":    {"origins": "*"},
            r"/":           {"origins": "*"},   # página inicial/ingestão
        },
        supports_credentials=False,
        allow_headers="*",
        methods=["GET", "POST", "OPTIONS"],
        expose_headers=["Content-Length", "Content-Type"],
    )

    # MIME corretos de fontes
    mimetypes.add_type("font/ttf", ".ttf")
    mimetypes.add_type("font/otf", ".otf")
    mimetypes.add_type("font/woff", ".woff")
    mimetypes.add_type("font/woff2", ".woff2")

    # Blueprints (importes dentro da factory p/ evitar side-effects no import)
    from .controllers.ui_controller import ui_bp
    from .controllers.ingestion_controller import ingestion_bp
    from .controllers.lovable_controller import lovable_bp
    from .controllers.asset_delivery_controller import assets_bp

    app.register_blueprint(ui_bp)                                  # "/" (UI ingestão)
    app.register_blueprint(ingestion_bp, url_prefix="/ingestions") # API ingestão
    app.register_blueprint(lovable_bp,   url_prefix="/lovable")    # API para o Lovable
    app.register_blueprint(assets_bp)                              # /stream e /files

    return app
