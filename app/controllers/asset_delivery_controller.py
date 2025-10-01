from flask import Blueprint, Response, request
from ..infra.bucket.gcs_client import GCSClient

delivery_bp = Blueprint("delivery", __name__)
_gcs = GCSClient()

def _resp_headers(as_attachment: bool, filename: str | None):
    disp = 'attachment' if as_attachment else 'inline'
    if not filename:
        return {"Content-Disposition": disp}
    return {"Content-Disposition": f'{disp}; filename="{filename}"'}

@delivery_bp.route("/stream/<path:blob_path>", methods=["GET", "OPTIONS"])
def stream(blob_path: str):
    bucket = request.args.get("bucket") or "brand-guides"
    data, content_type, filename = _gcs.read_object(bucket, blob_path)
    return Response(
        data,
        mimetype=content_type or "application/octet-stream",
        headers=_resp_headers(False, filename)
    )

@delivery_bp.route("/download/<path:blob_path>", methods=["GET", "OPTIONS"])
def download(blob_path: str):
    bucket = request.args.get("bucket") or "brand-guides"
    data, content_type, filename = _gcs.read_object(bucket, blob_path)
    return Response(
        data,
        mimetype=content_type or "application/octet-stream",
        headers=_resp_headers(True, filename)
    )
