# app/controllers/asset_delivery_controller.py
from flask import Blueprint, Response, request, abort
from mimetypes import guess_type
from app.infra.bucket.gcs_client import GCSClient

delivery_bp = Blueprint("delivery", __name__)
gcs = GCSClient()

@delivery_bp.route("/stream/<path:key>", methods=["GET", "OPTIONS"])
def stream_file(key: str):
    if request.method == "OPTIONS":
        return ("", 204)
    ctype = guess_type(key)[0] or "application/octet-stream"
    if not gcs.exists(key):
        abort(404)
    headers = {
        "Content-Type": ctype,
        "Cache-Control": "public, max-age=31536000" if "fonts" in key else "private, max-age=0",
        "Accept-Ranges": "none",
    }
    return Response(gcs.open_stream(key), headers=headers, mimetype=ctype)

@delivery_bp.route("/files/<path:key>", methods=["GET", "OPTIONS"])
def redirect_file(key: str):
    if request.method == "OPTIONS":
        return ("", 204)
    ctype = guess_type(key)[0] or "application/octet-stream"
    if not gcs.exists(key):
        abort(404)
    headers = {
        "Content-Type": ctype,
        "Cache-Control": "public, max-age=31536000",
    }
    return Response(gcs.download_as_bytes(key), headers=headers, mimetype=ctype)
