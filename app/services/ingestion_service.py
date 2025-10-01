import io
import json
import re
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from ..infra.db.bq_client import load_json
from ..utils.naming import safe_str  # se não existir, troque por um simples strip
from ..utils.validators import is_png, is_jpg  # se não existir, não tem problema não usar aqui


HEX_RE = re.compile(r"#([0-9A-F]{3}|[0-9A-F]{6})$", re.IGNORECASE)


def _normalize_hex(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = value.strip()
    if not s.startswith("#"):
        s = "#" + s
    s = s.upper()
    return s if HEX_RE.match(s) else None


class IngestionService:
    """
    Serviço de ingestão de ZIPs e utilitários de parsing.
    - Cores: lê colors/colors.json no formato:
        {
          "primary":   {"name": "...", "hex": "..."},
          "secondary": {"name": "...", "hex": "..."},
          "others":    [{"name": "...", "hex": "..."}, ...]
        }
    """
    def __init__(self):
        pass

    # ---------- CORES (colors.json) ----------
    def parse_colors_dict(self, brand_name: str, data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Converte o dicionário de cores para linhas da tabela BigQuery `colors`.
        Retorna (rows, warnings).
        """
        rows: List[Dict[str, Any]] = []
        warnings: List[str] = []

        def add_row(role: str, name: Optional[str], hex_value: Optional[str]) -> None:
            hx = _normalize_hex(hex_value)
            if not hx:
                warnings.append(f"[colors] Ignorando '{role}' sem hex válido: {hex_value!r}")
                return
            rows.append({
                "brand_name": brand_name,
                "color_name": (name or "").strip(),
                "hex": hx,
                "role": role,
            })

        # primary
        if isinstance(data.get("primary"), dict):
            add_row("primary", data["primary"].get("name"), data["primary"].get("hex"))
        else:
            warnings.append("[colors] Campo 'primary' ausente ou inválido")

        # secondary
        if isinstance(data.get("secondary"), dict):
            add_row("secondary", data["secondary"].get("name"), data["secondary"].get("hex"))
        else:
            warnings.append("[colors] Campo 'secondary' ausente ou inválido")

        # others (lista)
        others = data.get("others", [])
        if others is None:
            others = []
        if not isinstance(others, list):
            warnings.append("[colors] Campo 'others' não é lista — ignorando")
            others = []

        for idx, item in enumerate(others, start=1):
            if not isinstance(item, dict):
                warnings.append(f"[colors] others[{idx}] inválido — ignorando")
                continue
            add_row("others", item.get("name"), item.get("hex"))

        return rows, warnings

    def ingest_colors_from_json_bytes(self, brand_name: str, payload: bytes) -> Dict[str, Any]:
        """
        Recebe bytes de um JSON, valida e persiste em BigQuery via LOAD JOB.
        """
        try:
            doc = json.loads(payload.decode("utf-8"))
        except Exception as e:
            return {"ok": False, "error": f"JSON inválido em colors.json: {e}"}

        rows, warnings = self.parse_colors_dict(brand_name, doc)
        if not rows:
            return {"ok": False, "error": "Nenhuma cor válida encontrada em colors.json", "warnings": warnings}

        load_json("colors", rows)
        return {"ok": True, "inserted": len(rows), "warnings": warnings}

    def ingest_colors_from_zip(self, brand_name: str, zf: zipfile.ZipFile) -> Dict[str, Any]:
        """
        Procura por colors/colors.json dentro do ZIP e persiste.
        """
        # localizar entrada (case-insensitive) para colors/colors.json
        candidate = None
        for name in zf.namelist():
            low = name.lower().replace("\\", "/")
            if low.endswith("colors/colors.json"):
                candidate = name
                break
        if not candidate:
            return {"ok": True, "inserted": 0, "warnings": ["colors/colors.json não encontrado no ZIP (opcional)."]}

        try:
            with zf.open(candidate) as fp:
                data = fp.read()
            return self.ingest_colors_from_json_bytes(brand_name, data)
        except KeyError:
            return {"ok": False, "error": "colors/colors.json não pôde ser aberto no ZIP"}
        except Exception as e:
            return {"ok": False, "error": f"Falha ao ler colors.json: {e}"}

    # ---------- ZIP (geral) ----------
    def ingest_zip(self, brand_name: str, file_obj: io.BytesIO) -> Dict[str, Any]:
        """
        Ingestão principal do ZIP. Aqui, chamamos sub-ingestões (cores, etc).
        Outras categorias continuam como já implementadas no projeto.
        """
        try:
            with zipfile.ZipFile(file_obj) as zf:
                result: Dict[str, Any] = {
                    "ok": True,
                    "brand_name": brand_name,
                    "colors": {},
                }

                # 1) CORES
                colors_res = self.ingest_colors_from_zip(brand_name, zf)
                result["colors"] = colors_res
                if not colors_res.get("ok"):
                    # Não falha o pacote inteiro por causa de cores; apenas reporta
                    result["ok"] = False

                # TODO: manter aqui as chamadas para ingestão de logos, guidelines, avatars, applications, graphics, icons, fonts…
                # (sem alterações agora, para não mudar seu fluxo existente)

                return result
        except zipfile.BadZipFile:
            return {"ok": False, "error": "Arquivo enviado não é um ZIP válido."}
        except Exception as e:
            return {"ok": False, "error": f"Falha na ingestão do ZIP: {e}"}
