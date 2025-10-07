# app/services/ingestion_service.py
import os
import re
import json
import mimetypes
import zipfile
from typing import Any, Dict, List, Optional

from ..infra.db.bq_client import load_json
from ..infra.bucket.gcs_client import GCSClient
from ..utils.naming import safe_str
from ..utils.validators import (
    parse_category_dir, parse_subcategory_dir, file_prefix_sequence,
    enforce_even_when_cols_2
)

ORIG_DIRNAME = "originais"

class IngestionService:
    def __init__(self):
        self.gcs = GCSClient()
        self.bucket = os.getenv("GCS_BUCKET", "your-bucket")

    # ---------- helpers ----------
    def _guess_content_type(self, filename: str) -> str:
        return mimetypes.guess_type(filename)[0] or "application/octet-stream"

    def _upload(self, brand: str, cat_key: str, sub_label: Optional[str],
                filename: str, data: bytes, content_type: str, is_original: bool) -> Dict[str, str]:
        parts = [safe_str(brand).lower(), safe_str(cat_key).lower()]
        if is_original:
            parts.append(ORIG_DIRNAME)
        elif sub_label:
            parts.append(safe_str(sub_label).lower())
        parts.append(filename)
        path = "/".join(parts)
        url = self.gcs.write_object(self.bucket, path, data, content_type)
        return {"path": path, "url": url}

    def _list_level_txts(self, zf: zipfile.ZipFile, folder: str) -> List[str]:
        folder = folder.rstrip("/") + "/"
        txts = []
        for name in zf.namelist():
            if not name.startswith(folder):
                continue
            rel = name[len(folder):]
            if not rel or "/" in rel:
                continue
            low = rel.lower()
            if low.endswith(".txt") and os.path.basename(low) != "readme.md":
                txts.append(folder + rel)
        txts.sort()
        return txts

    def _concat_txts(self, zf: zipfile.ZipFile, paths: List[str]) -> str:
        out = []
        for p in paths:
            out.append(zf.read(p).decode("utf-8").strip())
        return "\n\n".join([t for t in out if t])

    # ---------- colors ----------
    def _find_cores_colors_json(self, zf: zipfile.ZipFile) -> Optional[str]:
        for name in zf.namelist():
            low = name.lower().replace("\\", "/")
            if re.search(r"/(cores|colors)/colors\.json$", low):
                return name
        return None

    def ingest_colors_from_json_bytes(self, brand_name: str, data: bytes) -> Dict[str, Any]:
        try:
            payload = json.loads(data.decode("utf-8"))
        except Exception:
            return {"ok": False, "error": "colors.json inválido (UTF-8/JSON)"}
        rows: List[Dict[str, Any]] = []
        seq = 0
        for item in payload.get("colors", []):
            seq += 1
            label = (item.get("label") or item.get("name") or "sem-nome").strip()
            rows.append({
                "brand_name": brand_name,
                "palette_key": "brand",
                "color_key": safe_str(label).lower(),
                "color_label": label,
                "hex": (item.get("hex") or "").strip(),
                "rgb_txt": (item.get("RGB") or item.get("rgb") or "").strip(),
                "cmic_txt": (item.get("CMYK") or item.get("CMIC") or "").strip(),  # compat
                "cmyk_txt": (item.get("CMYK") or "").strip(),
                "pantone_txt": (item.get("Pantone") or "").strip(),
                "category": (item.get("category") or "").strip() or None,       # main | secondary
                "subcategory": (item.get("subcategory") or "").strip() or None, # primary | secondary | others | None
                "sequence": int(item.get("sequence") or seq),
                "role": None,
                "raw_json": item,
            })
        if rows:
            load_json("colors", rows)
        return {"ok": True, "inserted": len(rows)}

    def ingest_colors_from_zip(self, brand_name: str, zf: zipfile.ZipFile) -> Dict[str, Any]:
        candidate = self._find_cores_colors_json(zf)
        if not candidate:
            return {"ok": True, "inserted": 0, "warnings": ["cores/colors.json não encontrado (opcional)."]}
        with zf.open(candidate) as fp:
            data = fp.read()
        return self.ingest_colors_from_json_bytes(brand_name, data)

    # ---------- ingestão principal ----------
    def _iter_direct_subdirs(self, zf: zipfile.ZipFile, parent: str) -> List[str]:
        parent = parent.rstrip("/") + "/"
        seen = set()
        for n in zf.namelist():
            if not n.startswith(parent):
                continue
            rel = n[len(parent):]
            if not rel:
                continue
            if "/" in rel:
                first = rel.split("/", 1)[0]
                seen.add(parent + first + "/")
        return sorted(seen)

    def _dir_exists(self, zf: zipfile.ZipFile, d: str) -> bool:
        d = d.rstrip("/") + "/"
        return any(n.startswith(d) for n in zf.namelist())

    def _iter_files(self, zf: zipfile.ZipFile, folder: str) -> List[str]:
        folder = folder.rstrip("/") + "/"
        return [n for n in zf.namelist() if n.startswith(folder) and n != folder and not n.endswith("/")]

    def _row_file(self, brand: str, cat_key: str, cat_label: str, cat_seq: int,
                  sub_key: Optional[str], sub_label: Optional[str], sub_seq: Optional[int], cols: Optional[int],
                  is_original: bool, sequence: int, original_name: str, path: str, url: str) -> Dict[str, Any]:
        return {
            "brand_name": brand,
            "category_key": cat_key, "category_label": cat_label, "category_seq": cat_seq,
            "subcategory_key": sub_key, "subcategory_label": sub_label, "subcategory_seq": sub_seq,
            "columns": cols, "is_original": is_original,
            "asset_type": "image", "text_content": None,
            "sequence": sequence, "original_name": original_name, "path": path, "url": url
        }

    def _row_text_category(self, brand: str, cat_key: str, cat_label: str, cat_seq: int, text: str) -> Dict[str, Any]:
        return {
            "brand_name": brand,
            "category_key": cat_key, "category_label": cat_label, "category_seq": cat_seq,
            "subcategory_key": None, "subcategory_label": None, "subcategory_seq": None,
            "columns": None, "is_original": False,
            "asset_type": "text", "text_content": text,
            "sequence": 0, "original_name": "", "path": "", "url": ""
        }

    def _row_text_subcategory(self, brand: str, cat_key: str, cat_label: str, cat_seq: int,
                              sub_label: Optional[str], sub_seq: int | None, cols: int | None, text: str) -> Dict[str, Any]:
        return {
            "brand_name": brand,
            "category_key": cat_key, "category_label": cat_label, "category_seq": cat_seq,
            "subcategory_key": safe_str(sub_label).lower() if sub_label else None,
            "subcategory_label": sub_label, "subcategory_seq": sub_seq,
            "columns": cols, "is_original": False,
            "asset_type": "text", "text_content": text,
            "sequence": 0, "original_name": "", "path": "", "url": ""
        }

    def ingest_zip(self, brand_name: str, file_obj) -> Dict[str, Any]:
        if not brand_name:
            return {"ok": False, "error": "brand_name obrigatório"}
        try:
            with zipfile.ZipFile(file_obj) as zf:
                cats = sorted({p.split("/")[0] for p in zf.namelist()
                               if p.endswith("/") and re.match(r"^\d{2}-", os.path.basename(p.rstrip("/")))})
                details: Dict[str, Any] = {"brand_name": brand_name, "errors": []}
                assets_rows: List[Dict[str, Any]] = []

                colors_res = self.ingest_colors_from_zip(brand_name, zf)
                details["colors"] = colors_res
                ok = colors_res.get("ok", True)

                for cat_dir in cats:
                    base = os.path.basename(cat_dir.rstrip("/"))
                    try:
                        cat_seq, cat_label = parse_category_dir(base)
                        cat_key = safe_str(cat_label).lower()
                        is_cores = (cat_key == "cores")

                        # TXT da categoria (exceto cores)
                        cat_txts = self._list_level_txts(zf, cat_dir)
                        if cat_txts and not is_cores:
                            assets_rows.append(self._row_text_category(
                                brand_name, cat_key, cat_label, cat_seq, self._concat_txts(zf, cat_txts)
                            ))

                        # originais obrigatório em tipografia
                        must_have_originals = (cat_key == "tipografia")
                        originals_dir = f"{cat_dir}{ORIG_DIRNAME}/"
                        has_originals = self._dir_exists(zf, originals_dir)
                        if must_have_originals and not has_originals:
                            raise ValueError("03-tipografia deve conter pasta 'originais/'")

                        if has_originals and not is_cores:
                            for p in self._iter_files(zf, originals_dir):
                                fname = os.path.basename(p)
                                content = zf.read(p)
                                up = self._upload(brand_name, cat_key, None, fname, content, self._guess_content_type(fname), is_original=True)
                                assets_rows.append(self._row_file(
                                    brand_name, cat_key, cat_label, cat_seq,
                                    None, None, None, None, True,
                                    file_prefix_sequence(fname), fname, up["path"], up["url"]
                                ))

                        if is_cores:
                            # textos específicos em 02-cores/
                            for fname, sublab in (("principal.txt", "principal"), ("secundaria.txt", "secundaria")):
                                path = f"{cat_dir}{fname}"
                                try:
                                    with zf.open(path) as fp:
                                        txt = fp.read().decode("utf-8").strip()
                                    assets_rows.append(self._row_text_subcategory(
                                        brand_name, cat_key, cat_label, cat_seq, sublab, 0, None, txt
                                    ))
                                except KeyError:
                                    pass
                            continue

                        # subcategorias padrão
                        for sub in self._iter_direct_subdirs(zf, cat_dir):
                            if os.path.basename(sub.rstrip("/")) == ORIG_DIRNAME:
                                continue
                            sub_seq, sub_label, cols = parse_subcategory_dir(os.path.basename(sub.rstrip("/")))

                            # TXT livre na subpasta
                            sub_txts = self._list_level_txts(zf, sub)
                            if sub_txts:
                                assets_rows.append(self._row_text_subcategory(
                                    brand_name, cat_key, cat_label, cat_seq, sub_label, sub_seq, cols,
                                    self._concat_txts(zf, sub_txts)
                                ))

                            files = [p for p in self._iter_files(zf, sub) if not p.lower().endswith(".txt")]
                            enforce_even_when_cols_2(len(files), cols, f"Subpasta {sub}")
                            for p in sorted(files):
                                fname = os.path.basename(p)
                                content = zf.read(p)
                                up = self._upload(brand_name, cat_key, sub_label, fname, content, self._guess_content_type(fname), is_original=False)
                                assets_rows.append(self._row_file(
                                    brand_name, cat_key, cat_label, cat_seq,
                                    safe_str(sub_label).lower() if sub_label else None, sub_label, sub_seq, cols,
                                    False, file_prefix_sequence(fname), fname, up["path"], up["url"]
                                ))
                    except Exception as e:
                        ok = False
                        details["errors"].append({"category": base, "error": str(e)})

                if assets_rows:
                    load_json("assets", assets_rows)

                summary = {"assets": len(assets_rows), "colors": colors_res.get("inserted", 0)}
                details["summary"] = summary
                details["ok"] = ok
                return {"ok": ok, "brand_name": brand_name, "details": details, "summary": summary}

        except zipfile.BadZipFile:
            return {"ok": False, "error": "Arquivo enviado não é um ZIP válido."}
        except Exception as e:
            return {"ok": False, "error": f"Falha na ingestão do ZIP: {e}"}
