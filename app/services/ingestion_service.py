# app/services/ingestion_service.py

import io
import os
import re
import json
import mimetypes
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from ..infra.db.bq_client import load_json
from ..infra.bucket.gcs_client import GCSClient
from ..utils.naming import safe_str
from ..utils.validators import parse_nn

HEX_RE = re.compile(r"#([0-9A-F]{3}|[0-9A-F]{6})$", re.IGNORECASE)
INT_PREFIX = re.compile(r"^(\d{1,4})")  # ex.: "10_27@2x-100.jpg" -> 10

IMG_EXTS = (".png", ".jpg", ".jpeg", ".svg", ".pdf")
FONT_EXTS = (".ttf", ".otf", ".woff", ".woff2")
SKIP_BASENAMES = {"readme.txt", "readme.md", ".ds_store", "ds_store", ".dsstore"}


def _normalize_hex(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = value.strip()
    if not v:
        return None
    if not v.startswith("#"):
        v = "#" + v
    if HEX_RE.match(v):
        return v.upper()
    return None


class IngestionService:
    """
    Estrutura esperada (case-insensitive, raiz flexível):
    - logos/{primary|secondary_horizontal|secondary_vertical|guidelines}/<files>
    - colors/colors.json
    - avatars/{round|square|app}/<files>
    - applications/<files>
    - graphics/<subcategoria>/<files>
    - icons/<files>
    - fonts/<files>

    Regras:
    - Ignora arquivos ocultos/sistema (._*, .DS_Store, README.*).
    - Deriva subcategoria como o segmento imediatamente após a categoria, se houver.
    - sequence: usa parse_nn(basename) ou, se não houver, número inicial (INT_PREFIX).
    - Faz upload dos arquivos para GCS e indexa na tabela BigQuery "assets".
    - Cores: lê colors/colors.json e insere na tabela "colors".
    """

    def __init__(self):
        self.gcs = GCSClient()
        self.bucket = os.getenv("GCS_BUCKET", "brand-guides")

    # ---------------- COLORS ----------------

    def parse_colors_dict(self, brand_name: str, data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
        rows: List[Dict[str, Any]] = []
        warnings: List[str] = []

        def add_row(role: str, name: Optional[str], hex_value: Optional[str]) -> None:
            hx = _normalize_hex(hex_value)
            if not hx:
                warnings.append(f"[colors] Ignorando '{role}' sem hex válido: {hex_value!r}")
                return
            rows.append(
                {
                    "brand_name": brand_name,
                    "color_name": (name or "").strip(),
                    "hex": hx,
                    "role": role,
                }
            )

        try:
            p = data.get("primary") or {}
            add_row("primary", p.get("name"), p.get("hex"))
        except Exception as e:
            warnings.append(f"[colors] Falha ao ler 'primary': {e}")

        try:
            s = data.get("secondary") or {}
            add_row("secondary", s.get("name"), s.get("hex"))
        except Exception as e:
            warnings.append(f"[colors] Falha ao ler 'secondary': {e}")

        try:
            others = data.get("others") or []
            if isinstance(others, list):
                for i, item in enumerate(others):
                    if not isinstance(item, dict):
                        warnings.append(f"[colors] others[{i}] inválido (não é dict).")
                        continue
                    add_row("other", item.get("name"), item.get("hex"))
        except Exception as e:
            warnings.append(f"[colors] Falha ao ler 'others': {e}")

        return rows, warnings

    def ingest_colors_from_json_bytes(self, brand_name: str, data: bytes) -> Dict[str, Any]:
        try:
            payload = json.loads(data.decode("utf-8"))
        except Exception:
            return {"ok": False, "error": "colors.json inválido (não é JSON UTF-8 válido)."}
        rows, warnings = self.parse_colors_dict(brand_name, payload)
        if rows:
            load_json("colors", rows)
        return {"ok": True, "inserted": len(rows), "warnings": warnings}

    def _find_colors_json(self, zf: zipfile.ZipFile) -> Optional[str]:
        for name in zf.namelist():
            low = name.lower().replace("\\", "/")
            if low.endswith("colors/colors.json"):
                return name
        return None

    def ingest_colors_from_zip(self, brand_name: str, zf: zipfile.ZipFile) -> Dict[str, Any]:
        candidate = self._find_colors_json(zf)
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

    # ---------------- ASSETS ----------------

    def _guess_content_type(self, filename: str) -> str:
        return mimetypes.guess_type(filename)[0] or "application/octet-stream"

    def _blob_path(self, brand: str, category: str, subcategory: Optional[str], basename: str) -> str:
        brand_s = safe_str(brand)
        category_s = category.strip()
        if subcategory:
            sub_s = subcategory.strip()
            return f"brands/{brand_s}/{category_s}/{sub_s}/{basename}"
        return f"brands/{brand_s}/{category_s}/{basename}"

    def _make_row(
        self,
        brand: str,
        category: str,
        subcategory: Optional[str],
        seq: int,
        basename: str,
        blob_path: str,
    ) -> Dict[str, Any]:
        return {
            "brand_name": brand,
            "category": category,
            "subcategory": subcategory,
            "sequence": int(seq or 0),
            "original_name": basename,
            "path": blob_path,
            "url": f"gs://{self.bucket}/{blob_path}",
        }

    def _iter_category_files(
        self,
        zf: zipfile.ZipFile,
        category: str,
        allowed_exts: Tuple[str, ...],
    ):
        cat = category.lower()
        for name in zf.namelist():
            low = name.lower().replace("\\", "/")
            if low.endswith("/"):
                continue
            parts = [p for p in low.split("/") if p]
            if cat not in parts:
                continue  # aceita pasta-raiz arbitrária (ex.: brand-guide-template/)
            base = parts[-1]
            if base.startswith("._") or base in SKIP_BASENAMES:
                continue
            if not any(base.endswith(ext) for ext in allowed_exts):
                continue
            yield name, parts  # devolve também os segmentos para derivar subcategoria

    def _parse_sequence(self, basename: str) -> int:
        seq = parse_nn(basename) or 0
        if seq == 0:
            m = INT_PREFIX.match(basename)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return 0
        return seq

    def _ingest_generic(self, brand: str, zf: zipfile.ZipFile, category: str) -> Dict[str, Any]:
        allowed_exts = FONT_EXTS if category.lower() == "fonts" else IMG_EXTS
        inserted = 0
        warnings: List[str] = []
        rows: List[Dict[str, Any]] = []

        for arcname, parts in self._iter_category_files(zf, category, allowed_exts):
            basename = parts[-1]
            cat_idx = parts.index(category.lower())
            # subcategoria = segmento imediatamente após a categoria, apenas se houver pelo menos
            # dois segmentos restantes (subcategoria + arquivo)
            subcat = parts[cat_idx + 1] if (cat_idx + 2) < len(parts) else None

            try:
                with zf.open(arcname) as fp:
                    data = fp.read()
            except Exception as e:
                warnings.append(f"[{category}] Falha ao ler {arcname}: {e}")
                continue

            blob_path = self._blob_path(brand, category, subcat, basename)
            try:
                self.gcs.write_object(self.bucket, blob_path, data, self._guess_content_type(basename))
            except Exception as e:
                warnings.append(f"[{category}] Falha ao subir GCS {blob_path}: {e}")
                continue

            rows.append(self._make_row(brand, category, subcat, self._parse_sequence(basename), basename, blob_path))
            inserted += 1

        if rows:
            try:
                load_json("assets", rows)
            except Exception as e:
                warnings.append(f"[{category}] Falha ao inserir no BigQuery: {e}")

        return {"ok": True, "inserted": inserted, "warnings": warnings}

    def ingest_logos(self, brand: str, zf: zipfile.ZipFile) -> Dict[str, Any]:
        return self._ingest_generic(brand, zf, "logos")

    def ingest_avatars(self, brand: str, zf: zipfile.ZipFile) -> Dict[str, Any]:
        return self._ingest_generic(brand, zf, "avatars")

    def ingest_applications(self, brand: str, zf: zipfile.ZipFile) -> Dict[str, Any]:
        return self._ingest_generic(brand, zf, "applications")

    def ingest_graphics(self, brand: str, zf: zipfile.ZipFile) -> Dict[str, Any]:
        return self._ingest_generic(brand, zf, "graphics")

    def ingest_icons(self, brand: str, zf: zipfile.ZipFile) -> Dict[str, Any]:
        return self._ingest_generic(brand, zf, "icons")

    def ingest_fonts(self, brand: str, zf: zipfile.ZipFile) -> Dict[str, Any]:
        return self._ingest_generic(brand, zf, "fonts")

    # ---------------- ZIP (geral) ----------------

    def ingest_zip(self, brand_name: str, file_obj: io.BytesIO) -> Dict[str, Any]:
        brand_name = safe_str(brand_name)
        if not brand_name:
            return {"ok": False, "error": "brand_name obrigatório"}

        outer_ok = True
        try:
            with zipfile.ZipFile(file_obj) as zf:
                details: Dict[str, Any] = {"brand_name": brand_name}

                colors_res = self.ingest_colors_from_zip(brand_name, zf)
                details["colors"] = colors_res
                if not colors_res.get("ok"):
                    outer_ok = False

                logos_res = self.ingest_logos(brand_name, zf)
                avatars_res = self.ingest_avatars(brand_name, zf)
                applications_res = self.ingest_applications(brand_name, zf)
                graphics_res = self.ingest_graphics(brand_name, zf)
                icons_res = self.ingest_icons(brand_name, zf)
                fonts_res = self.ingest_fonts(brand_name, zf)

                for k, res in {
                    "logos": logos_res,
                    "avatars": avatars_res,
                    "applications": applications_res,
                    "graphics": graphics_res,
                    "icons": icons_res,
                    "fonts": fonts_res,
                }.items():
                    details[k] = res
                    if not res.get("ok"):
                        outer_ok = False

                assets_total = sum(
                    details[k].get("inserted", 0) for k in ("logos", "avatars", "applications", "graphics", "icons", "fonts")
                )
                colors_total = details["colors"].get("inserted", 0)

                summary = {"assets": assets_total, "colors": colors_total}
                details["summary"] = summary
                details["ok"] = outer_ok

                return {
                    "ok": outer_ok,
                    "brand_name": brand_name,
                    "details": details,
                    "summary": summary,
                }

        except zipfile.BadZipFile:
            return {"ok": False, "error": "Arquivo enviado não é um ZIP válido."}
        except Exception as e:
            return {"ok": False, "error": f"Falha na ingestão do ZIP: {e}"}
