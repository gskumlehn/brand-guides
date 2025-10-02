from io import BytesIO
from typing import Any, Dict, Tuple

from flask import (
    Blueprint,
    Response,
    jsonify,
    request,
    send_file,
)

from ..utils.zip_utils import build_template_zip_bytes, empty_colors_json
from ..services.ingestion_service import IngestionService  # assinatura canônica

ingestion_bp = Blueprint("ingestion", __name__)


@ingestion_bp.get("/template.zip")
def download_template_zip():
    blob = build_template_zip_bytes()
    return send_file(
        BytesIO(blob),
        mimetype="application/zip",
        as_attachment=True,
        download_name="brand-guide-template.zip",
        conditional=False,
    )


@ingestion_bp.get("/colors.json")
def get_empty_colors_json():
    return Response(empty_colors_json(), mimetype="application/json; charset=utf-8")


def _normalize_service_result(result: Any) -> Tuple[bool, Dict[str, Any]]:
    """
    Normaliza o retorno do service para {ok, payload}.
    - Se vier dict com ok/error, propaga.
    - Caso contrário, ok=True e envelopa em {"result": str(...)}.
    """
    if isinstance(result, dict):
        ok = result.get("ok", True)
        if result.get("error"):
            ok = False
        return ok, result
    return True, {"result": str(result)}


@ingestion_bp.post("/upload")
def upload_zip():
    brand_name = (request.form.get("brand_name") or "").strip()
    if not brand_name:
        return jsonify({"ok": False, "error": "brand_name é obrigatório."}), 400

    if "zip_file" not in request.files:
        return jsonify({"ok": False, "error": "zip_file é obrigatório (multipart)."}), 400

    f = request.files["zip_file"]
    if not f or f.filename == "":
        return jsonify({"ok": False, "error": "Arquivo .zip inválido."}), 400

    try:
        raw = f.read()
        if not raw:
            return jsonify({"ok": False, "error": "ZIP vazio."}), 400
        zip_stream = BytesIO(raw)  # <<< sempre stream
    except Exception as e:
        return jsonify({"ok": False, "error": f"Falha ao ler ZIP: {e}"}), 400

    svc = IngestionService()
    try:
        # assinatura única e canônica
        res = svc.ingest_zip(brand_name, zip_stream)
        ok, payload = _normalize_service_result(res)
        status = 200 if ok else 400
        return jsonify({
            "ok": ok,
            "brand_name": brand_name,
            "summary": payload.get("summary", {}),
            "details": payload
        }), status
    except Exception as e:
        return jsonify({"ok": False, "error": f"Falha na ingestão do ZIP: {e}"}), 500
