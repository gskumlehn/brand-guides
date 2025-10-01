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

# Import opcional do serviço para não quebrar o boot se ainda estiver em ajuste
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
# POST /ingest/upload       -> recebe brand_name + zip_file (multipart)
#   - brand_name: str (obrigatório)
#   - zip_file:   file .zip (obrigatório)
# ---------------------------------------------------------
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
        zip_bytes = f.read()
        if not zip_bytes:
            return jsonify({"ok": False, "error": "ZIP vazio."}), 400
    except Exception as e:  # pragma: no cover
        return jsonify({"ok": False, "error": f"Falha ao ler ZIP: {e}"}), 400

    if IngestionService is None:
        return jsonify({
            "ok": False,
            "error": "IngestionService não encontrado. Verifique app/services/ingestion_service.py",
        }), 501

    svc = IngestionService()

    # Tentativas de chamada em ordem (assinaturas comuns)
    errors: Dict[str, str] = {}

    def ok(result: Any) -> Dict[str, Any]:
        if not isinstance(result, dict):
            return {"message": "Ingestão concluída.", "result": str(result)}
        return result

    try:
        # Preferimos método chamado "ingest_zip" se existir
        if hasattr(svc, "ingest_zip"):
            # 1) Posicional: (brand_name, zip_bytes)
            try:
                return jsonify({
                    "ok": True,
                    "brand_name": brand_name,
                    "summary": {},
                    "details": ok(getattr(svc, "ingest_zip")(brand_name, zip_bytes)),
                }), 200
            except TypeError as e:
                errors["ingest_zip(brand, bytes)"] = str(e)

            # 2) Posicional: (brand_name, BytesIO(zip_bytes))
            try:
                return jsonify({
                    "ok": True,
                    "brand_name": brand_name,
                    "summary": {},
                    "details": ok(getattr(svc, "ingest_zip")(brand_name, BytesIO(zip_bytes))),
                }), 200
            except TypeError as e:
                errors["ingest_zip(brand, BytesIO)"] = str(e)

            # 3) Nomeados comuns
            for kw in ("zip_file", "data", "content", "blob", "file_bytes", "file", "stream"):
                try:
                    return jsonify({
                        "ok": True,
                        "brand_name": brand_name,
                        "summary": {},
                        "details": ok(getattr(svc, "ingest_zip")(brand_name=brand_name, **{kw: zip_bytes})),
                    }), 200
                except TypeError as e:
                    errors[f"ingest_zip(brand, {kw}=bytes)"] = str(e)
                except Exception as e:
                    # Se entrou no método mas falhou por outro motivo, propaga
                    return jsonify({"ok": False, "error": f"Falha na ingestão: {e}"}), 500

        # Fallback p/ outros nomes comuns
        for meth in ("process_zip", "run"):
            if hasattr(svc, meth):
                fn = getattr(svc, meth)
                # 1) Posicional
                try:
                    return jsonify({
                        "ok": True,
                        "brand_name": brand_name,
                        "summary": {},
                        "details": ok(fn(brand_name, zip_bytes)),
                    }), 200
                except TypeError as e:
                    errors[f"{meth}(brand, bytes)"] = str(e)

                # 2) Posicional usando BytesIO
                try:
                    return jsonify({
                        "ok": True,
                        "brand_name": brand_name,
                        "summary": {},
                        "details": ok(fn(brand_name, BytesIO(zip_bytes))),
                    }), 200
                except TypeError as e:
                    errors[f"{meth}(brand, BytesIO)"] = str(e)

                # 3) Nomeados comuns
                for kw in ("zip_file", "data", "content", "blob", "file_bytes", "file", "stream"):
                    try:
                        return jsonify({
                            "ok": True,
                            "brand_name": brand_name,
                            "summary": {},
                            "details": ok(fn(brand_name=brand_name, **{kw: zip_bytes})),
                        }), 200
                    except TypeError as e:
                        errors[f"{meth}(brand, {kw}=bytes)"] = str(e)
                    except Exception as e:
                        return jsonify({"ok": False, "error": f"Falha na ingestão: {e}"}), 500

        # Se chegou aqui: nenhuma assinatura casou
        return jsonify({
            "ok": False,
            "error": (
                "Nenhuma assinatura de ingestão compatível encontrada. "
                "Tente implementar um dos formatos:\n"
                "- ingest_zip(brand_name, zip_bytes)\n"
                "- ingest_zip(brand_name, zip_file=... | data=... | content=... | blob=... | file_bytes=... | file=... | stream=...)\n"
                "- process_zip(...)\n"
                "- run(...)\n"
                f"Detalhes de TypeError capturados: {errors}"
            )
        }), 501

    except Exception as e:
        return jsonify({"ok": False, "error": f"Falha na ingestão: {e}"}), 500
