# app/controllers/ingestion_controller.py

from io import BytesIO
from typing import Any, Dict

from flask import (
    Blueprint,
    Response,
    jsonify,
    request,
    send_file,
)

from ..utils.zip_utils import build_template_zip_bytes, empty_colors_json

# Se o serviço existir, faremos import opcional.
# Isso evita quebrar o boot caso ainda esteja ajustando o service.
try:
    from ..services.ingestion_service import IngestionService  # type: ignore
except Exception:  # pragma: no cover
    IngestionService = None  # type: ignore

ingestion_bp = Blueprint("ingestion", __name__)

# ---------------------------------------------------------
# GET /ingest/template.zip  -> baixa o zip modelo
# ---------------------------------------------------------
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

# ---------------------------------------------------------
# GET /ingest/colors.json   -> retorna o colors.json vazio
# ---------------------------------------------------------
@ingestion_bp.get("/colors.json")
def get_empty_colors_json():
    return Response(empty_colors_json(), mimetype="application/json; charset=utf-8")

# ---------------------------------------------------------
# POST /ingest/upload       -> recebe brand_name + zip_file
# body: multipart/form-data
#   - brand_name: str (obrigatório)
#   - zip_file:   file .zip (obrigatório)
# ---------------------------------------------------------
@ingestion_bp.post("/upload")
def upload_zip():
    # Validações básicas
    brand_name = (request.form.get("brand_name") or "").strip()
    if not brand_name:
        return jsonify({"ok": False, "error": "brand_name é obrigatório."}), 400

    if "zip_file" not in request.files:
        return jsonify({"ok": False, "error": "zip_file é obrigatório (multipart)."}), 400

    f = request.files["zip_file"]
    if not f or f.filename == "":
        return jsonify({"ok": False, "error": "Arquivo .zip inválido."}), 400

    # Lê bytes do ZIP
    try:
        zip_bytes = f.read()
        if not zip_bytes:
            return jsonify({"ok": False, "error": "ZIP vazio."}), 400
    except Exception as e:  # pragma: no cover
        return jsonify({"ok": False, "error": f"Falha ao ler ZIP: {e}"}), 400

    # Encaminha para o serviço de ingestão (se disponível)
    if IngestionService is None:
        # Serviço indisponível — retorna 501 para você ver rapidamente
        return jsonify({
            "ok": False,
            "error": "IngestionService não encontrado. Verifique app/services/ingestion_service.py",
        }), 501

    svc = IngestionService()

    # Tolerante a assinaturas diferentes — tentamos alguns nomes comuns:
    result: Dict[str, Any] = {}
    try:
        if hasattr(svc, "ingest_zip"):
            # assinatura preferida
            result = svc.ingest_zip(brand_name=brand_name, zip_bytes=zip_bytes)
        elif hasattr(svc, "process_zip"):
            result = svc.process_zip(brand_name=brand_name, zip_bytes=zip_bytes)
        elif hasattr(svc, "run"):
            result = svc.run(brand_name=brand_name, zip_bytes=zip_bytes)
        else:
            return jsonify({
                "ok": False,
                "error": (
                    "Nenhum método de ingestão encontrado em IngestionService. "
                    "Implemente um de: ingest_zip(brand_name, zip_bytes) | "
                    "process_zip(brand_name, zip_bytes) | run(brand_name, zip_bytes)."
                ),
            }), 501
    except Exception as e:
        # Erro de ingestão — retornamos detalhes para facilitar debug
        return jsonify({
            "ok": False,
            "error": f"Falha na ingestão: {e.__class__.__name__}: {e}",
        }), 500

    # Normaliza a resposta
    if not isinstance(result, dict):
        result = {"message": "Ingestão concluída.", "result": str(result)}

    # Estrutura sugerida de retorno
    payload = {
        "ok": True,
        "brand_name": brand_name,
        "summary": {
            # Se o service já preencheu, aproveitamos; senão deixamos valores default
            "assets_inserted": result.get("assets_inserted", 0),
            "colors_inserted": result.get("colors_inserted", 0),
            "warnings": result.get("warnings", []),
        },
        "details": result,  # ecoa tudo que o service devolveu p/ rastreio
    }
    return jsonify(payload), 200
