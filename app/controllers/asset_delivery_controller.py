import mimetypes
from flask import Blueprint, redirect, request, abort, Response, stream_with_context
from ..infra.bucket.gcs_client import GCSClient

assets_bp = Blueprint("assets_delivery", __name__)

@assets_bp.options("/files/<path:path>")
@assets_bp.options("/stream/<path:path>")
def cors_preflight(path: str):
    return ("", 204)

def _auth():
    return True

def _gcs():
    return GCSClient()

@assets_bp.get("/files/<path:path>")
def signed_redirect(path: str):
    _auth()
    ttl = int(request.args.get("ttl", 3600))
    return redirect(_gcs().signed_get_url(path, ttl), code=302)

@assets_bp.get("/stream/<path:path>")
def stream_object(path: str):
    _auth()
    gcs = _gcs()
    blob = gcs.bucket.blob(path)
    if not blob.exists():
        abort(404)
    ctype = blob.content_type or mimetypes.guess_type(path)[0] or "application/octet-stream"
    def gen():
        with blob.open("rb") as fh:
            while True:
                chunk = fh.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk

    headers = {
        "Content-Type": ctype,
        "Cache-Control": "private, max-age=0",
        "Accept-Ranges": "none",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
    }

    return Response(stream_with_context(gen()), status=200, headers=headers)
