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
    parse_category_dir, parse_subcategory_dir, file_prefix_sequence
)

ORIG_DIRNAME = "originais"
SYSTEM_ARTIFACTS = {"__macosx", ".ds_store", "thumbs.db", "desktop.ini"}


def _is_artifact_component(comp: str) -> bool:
    c = comp.strip().lower()
    return (not c) or (c in SYSTEM_ARTIFACTS) or c.startswith("._")


def _norm_zip_basename(name: Optional[str]) -> str:
    if not name:
        return ""
    base = os.path.splitext(os.path.basename(name))[0]
    return safe_str(base)


class IngestionService:
    def __init__(self):
        self.gcs = GCSClient()
        self.bucket = os.getenv("GCS_BUCKET", "brand-guides")

    # -------- ZIP helpers --------
    def _names(self, zf: zipfile.ZipFile) -> List[str]:
        out = []
        for n in zf.namelist():
            n = n.replace("\\", "/")
            comps = [c for c in n.split("/") if c != ""]
            if not comps:
                continue
            if any(_is_artifact_component(c) for c in comps):
                continue
            out.append(n)
        return out

    def _strip_single_container_root(self, zf: zipfile.ZipFile, zip_filename: Optional[str]) -> str:
        names = self._names(zf)
        if any("/" not in n for n in names):
            return ""
        first_level = set(n.split("/", 1)[0] for n in names if "/" in n)
        if len(first_level) != 1:
            return ""
        only = list(first_level)[0]
        if not all(n.startswith(only + "/") for n in names):
            return ""
        # sempre ignoramos container único
        return only + "/"

    def _iter_direct_subdirs(self, zf: zipfile.ZipFile, parent: str) -> List[str]:
        parent = parent.rstrip("/") + "/"
        seen = set()
        for n in self._names(zf):
            if not n.startswith(parent):
                continue
            rel = n[len(parent):]
            if not rel:
                continue
            if "/" in rel:
                first = rel.split("/", 1)[0].strip()
                if first and not _is_artifact_component(first):
                    seen.add(parent + first + "/")
        return sorted(seen)

    def _dir_exists(self, zf: zipfile.ZipFile, d: str) -> bool:
        d = d.rstrip("/") + "/"
        return any(n.startswith(d) for n in self._names(zf))

    def _iter_files(self, zf: zipfile.ZipFile, folder: str) -> List[str]:
        folder = folder.rstrip("/") + "/"
        return [
            n for n in self._names(zf)
            if n.startswith(folder) and n != folder and not n.endswith("/")
        ]

    def _list_level_txts(self, zf: zipfile.ZipFile, folder: str) -> List[str]:
        folder = folder.rstrip("/") + "/"
        txts = []
        for name in self._names(zf):
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

    # -------- Upload --------
    def _guess_content_type(self, filename: str) -> str:
        return mimetypes.guess_type(filename)[0] or "application/octet-stream"

    def _upload(self, brand: str, cat_key: str, sub_dirname: Optional[str],
                filename: str, data: bytes, content_type: str, is_original: bool) -> Dict[str, str]:
        parts = [safe_str(brand).lower(), safe_str(cat_key).lower()]
        if is_original:
            parts.append(ORIG_DIRNAME)
        elif sub_dirname:
            parts.append(sub_dirname.strip())  # preserva dirname EXATO, trim bordas
        parts.append(filename)
        path = "/".join(parts)
        url = self.gcs.write_object(self.bucket, path, data, content_type)
        return {"path": path, "url": url}

    # -------- Colors --------
    def _find_cores_colors_json(self, zf: zipfile.ZipFile, root: str) -> Optional[str]:
        """
        Aceita também categorias prefixadas com NN-:
          /cores/colors.json
          /cores/cores.json
          /colors/colors.json
          /NN-cores/colors.json
          /NN-cores/cores.json
          /NN-colors/colors.json
        """
        pat = re.compile(r"/(?:\d{2}-)?(?:cores|colors)/(?:colors|cores)\.json$", re.IGNORECASE)
        root_l = (root or "").lower()
        for name in self._names(zf):
            low = name.lower()
            if root_l and not low.startswith(root_l):
                continue
            if pat.search(low):
                return name
        return None

    def _json_relaxed_load(self, raw: bytes) -> Dict[str, Any]:
        """Suporta BOM e comentários // ou /* */."""
        txt = raw.decode("utf-8-sig")
        txt = re.sub(r"/\*.*?\*/", "", txt, flags=re.S)
        txt = re.sub(r"^\s*//.*?$", "", txt, flags=re.M)
        return json.loads(txt)

    def ingest_colors_from_json_bytes(self, brand_name: str, data: bytes) -> Dict[str, Any]:
        """
        Formatos suportados:

        A) Plano
        {
          "colors": [
            {"label":"Verde", "hex":"#123", "Pantone":"P 123", "CMYK":"0,0,0,0", "RGB":"0,0,0",
             "category":"main|secondary", "subcategory":"primary|secondary|others", "sequence":1}
          ]
        }

        B) Agrupado
        {
          "main": {
            "primary":   [ { "label": "...", "hex": "...", "Pantone": "...", "CMYK": "...", "RGB": "...", "sequence": 1 }, ... ],
            "secondary": [ ... ],
            "others":    [ ... ]
          },
          "secondary": [ { ... }, ... ]
        }
        """
        try:
            payload = self._json_relaxed_load(data)
        except Exception:
            return {"ok": False, "error": "colors.json inválido"}

        rows: List[Dict[str, Any]] = []
        seq = 0

        def _norm_val(d: Dict[str, Any], *keys: str) -> str:
            for k in keys:
                if k in d and d[k] is not None:
                    return str(d[k]).strip()
            return ""

        def _push(item: Dict[str, Any], category: Optional[str], subcategory: Optional[str]):
            nonlocal seq, rows
            seq += 1
            label = _norm_val(item, "label", "name")
            rows.append({
                "brand_name": brand_name,
                "palette_key": "brand",
                "color_key": safe_str(label).lower() or f"c{seq:04d}",
                "color_label": label or f"sem-nome-{seq}",
                "hex": _norm_val(item, "hex"),
                "rgb_txt": _norm_val(item, "RGB", "rgb"),
                "cmic_txt": _norm_val(item, "CMIC", "CMYK"),
                "cmyk_txt": _norm_val(item, "CMYK"),
                "pantone_txt": _norm_val(item, "Pantone", "pantone"),
                "category": (category or _norm_val(item, "category") or None),
                "subcategory": (subcategory or _norm_val(item, "subcategory") or None),
                "sequence": int(item.get("sequence") or seq),
                "role": None,
                "raw_json": item,
            })

        if isinstance(payload.get("colors"), list):
            for it in payload["colors"]:
                if isinstance(it, dict):
                    _push(it, None, None)
        else:
            main = payload.get("main") or payload.get("Main") or {}
            secondary = payload.get("secondary") or payload.get("Secondary") or []

            if isinstance(main, dict):
                for subk in ("primary", "secondary", "others"):
                    arr = main.get(subk) or []
                    if isinstance(arr, list):
                        for it in arr:
                            if isinstance(it, dict):
                                _push(it, "main", subk)

            if isinstance(secondary, list):
                for it in secondary:
                    if isinstance(it, dict):
                        _push(it, "secondary", None)

        if rows:
            load_json("colors", rows)
        return {"ok": True, "inserted": len(rows)}

    def ingest_colors_from_zip(self, brand_name: str, zf: zipfile.ZipFile, root: str) -> Dict[str, Any]:
        candidate = self._find_cores_colors_json(zf, root)
        if not candidate:
            return {"ok": True, "inserted": 0, "warnings": ["cores/{colors|cores}.json não encontrado (opcional)."]}
        with zf.open(candidate) as fp:
            data = fp.read()
        return self.ingest_colors_from_json_bytes(brand_name, data)

    # -------- Descoberta de categorias --------
    def _discover_categories_under_root(self, zf: zipfile.ZipFile, root: str) -> List[str]:
        first_dirs = set()
        for n in self._names(zf):
            if not n.startswith(root):
                continue
            rel = n[len(root):]
            if not rel or "/" not in rel:
                continue
            first = rel.split("/", 1)[0].strip()
            if first and not _is_artifact_component(first):
                first_dirs.add(first + "/")
        nn = [d for d in first_dirs if re.match(r"^\d{2}-", d)]
        if nn:
            return [root + d for d in sorted(nn)]
        return [root + d for d in sorted(first_dirs)]

    # -------- Ingestão principal --------
    def ingest_zip(self, brand_name: str, file_obj, filename: Optional[str] = None) -> Dict[str, Any]:
        if not brand_name:
            return {"ok": False, "error": "brand_name obrigatório"}
        try:
            with zipfile.ZipFile(file_obj) as zf:
                container = self._strip_single_container_root(zf, filename)
                root = container or ""

                details: Dict[str, Any] = {"brand_name": brand_name, "errors": []}
                assets_rows: List[Dict[str, Any]] = []

                colors_res = self.ingest_colors_from_zip(brand_name, zf, root)
                details["colors"] = colors_res
                ok = colors_res.get("ok", True)

                cats = self._discover_categories_under_root(zf, root)
                for cat_dir in cats:
                    base = os.path.basename(cat_dir.rstrip("/")).strip()
                    try:
                        if re.match(r"^\d{2}-", base):
                            cat_seq, cat_label = parse_category_dir(base)
                        else:
                            cat_seq, cat_label = (0, base)
                        if _is_artifact_component(cat_label):
                            continue

                        cat_key = safe_str(cat_label).lower()
                        is_cores = (cat_key == "cores" or cat_label.lower() == "colors")

                        # TXT categoria (exceto cores)
                        cat_txts = self._list_level_txts(zf, cat_dir)
                        if cat_txts and not is_cores:
                            assets_rows.append({
                                "brand_name": brand_name,
                                "category_key": cat_key, "category_label": cat_label, "category_seq": cat_seq,
                                "subcategory_key": None, "subcategory_label": None, "subcategory_seq": None,
                                "columns": None, "is_original": False,
                                "asset_type": "text", "text_content": self._concat_txts(zf, cat_txts),
                                "sequence": 0, "original_name": "", "path": "", "url": ""
                            })

                        must_have_originals = (cat_key == "tipografia")
                        originals_dir = f"{cat_dir}{ORIG_DIRNAME}/"
                        has_originals = self._dir_exists(zf, originals_dir)
                        if must_have_originals and not has_originals:
                            raise ValueError("03-tipografia deve conter pasta 'originais/'")

                        if has_originals and not is_cores:
                            for p in self._iter_files(zf, originals_dir):
                                fname = os.path.basename(p).strip()
                                content = zf.read(p)
                                up = self._upload(
                                    brand_name, cat_key, None, fname, content,
                                    self._guess_content_type(fname), is_original=True
                                )
                                assets_rows.append({
                                    "brand_name": brand_name,
                                    "category_key": cat_key, "category_label": cat_label, "category_seq": cat_seq,
                                    "subcategory_key": None, "subcategory_label": None, "subcategory_seq": None,
                                    "columns": None, "is_original": True,
                                    "asset_type": "image", "text_content": None,
                                    "sequence": file_prefix_sequence(fname),
                                    "original_name": fname, "path": up["path"], "url": up["url"]
                                })

                        if is_cores:
                            for fname, sublab in (("principal.txt", "principal"), ("secundaria.txt", "secundaria")):
                                path = f"{cat_dir}{fname}"
                                try:
                                    with zf.open(path) as fp:
                                        txt = fp.read().decode("utf-8").strip()
                                    assets_rows.append({
                                        "brand_name": brand_name,
                                        "category_key": cat_key, "category_label": cat_label, "category_seq": cat_seq,
                                        "subcategory_key": safe_str(sublab),
                                        "subcategory_label": sublab, "subcategory_seq": 0,
                                        "columns": None, "is_original": False,
                                        "asset_type": "text", "text_content": txt,
                                        "sequence": 0, "original_name": "", "path": "", "url": ""
                                    })
                                except KeyError:
                                    pass
                            continue

                        subdirs = self._iter_direct_subdirs(zf, cat_dir)
                        if subdirs:
                            for sub in subdirs:
                                if os.path.basename(sub.rstrip("/")).strip() == ORIG_DIRNAME:
                                    continue
                                sub_dirname = os.path.basename(sub.rstrip("/")).strip()
                                if re.match(r"^\d{2}($|-)", sub_dirname):
                                    sub_seq, sub_label, cols = parse_subcategory_dir(sub_dirname)
                                else:
                                    sub_seq, sub_label, cols = (0, None, None)

                                if (sub_label is not None) and (sub_label != "") and _is_artifact_component(sub_label):
                                    continue

                                tech_key = sub_dirname  # preserva NN-label-NN / NN-NN / NN--NN
                                display_label = sub_label if sub_label is not None else None

                                # txt da subpasta
                                sub_txts = self._list_level_txts(zf, sub)
                                if sub_txts:
                                    assets_rows.append({
                                        "brand_name": brand_name,
                                        "category_key": cat_key, "category_label": cat_label, "category_seq": cat_seq,
                                        "subcategory_key": tech_key,
                                        "subcategory_label": (display_label if display_label is not None else ""),
                                        "subcategory_seq": sub_seq,
                                        "columns": cols, "is_original": False,
                                        "asset_type": "text", "text_content": self._concat_txts(zf, sub_txts),
                                        "sequence": 0, "original_name": "", "path": "", "url": ""
                                    })

                                files = [p for p in self._iter_files(zf, sub) if not p.lower().endswith(".txt")]
                                # NENHUM bloqueio/aviso para quantidade impar em 2 colunas — sempre salvar
                                for p in sorted(files):
                                    fname = os.path.basename(p).strip()
                                    content = zf.read(p)
                                    up = self._upload(
                                        brand_name, cat_key, sub_dirname, fname, content,
                                        self._guess_content_type(fname), is_original=False
                                    )
                                    assets_rows.append({
                                        "brand_name": brand_name,
                                        "category_key": cat_key, "category_label": cat_label, "category_seq": cat_seq,
                                        "subcategory_key": tech_key,
                                        "subcategory_label": (display_label if display_label is not None else ""),
                                        "subcategory_seq": sub_seq,
                                        "columns": cols, "is_original": False,
                                        "asset_type": "image", "text_content": None,
                                        "sequence": file_prefix_sequence(fname),
                                        "original_name": fname,
                                        "path": up["path"],
                                        "url": up["url"]
                                    })
                        else:
                            files = [p for p in self._iter_files(zf, cat_dir) if not p.lower().endswith(".txt")]
                            for p in sorted(files):
                                fname = os.path.basename(p).strip()
                                content = zf.read(p)
                                up = self._upload(
                                    brand_name, cat_key, None, fname, content,
                                    self._guess_content_type(fname), is_original=False
                                )
                                assets_rows.append({
                                    "brand_name": brand_name,
                                    "category_key": cat_key, "category_label": cat_label, "category_seq": cat_seq,
                                    "subcategory_key": None, "subcategory_label": None, "subcategory_seq": None,
                                    "columns": None, "is_original": False,
                                    "asset_type": "image", "text_content": None,
                                    "sequence": file_prefix_sequence(fname),
                                    "original_name": fname,
                                    "path": up["path"],
                                    "url": up["url"]
                                })

                    except Exception as e:
                        ok = False
                        details["errors"].append({"category": base, "error": str(e)})

                if assets_rows:
                    load_json("assets", assets_rows)

                summary = {"assets": len(assets_rows), "colors": details["colors"].get("inserted", 0)}
                details["summary"] = summary
                details["ok"] = ok
                return {"ok": ok, "brand_name": brand_name, "details": details, "summary": summary}

        except zipfile.BadZipFile:
            return {"ok": False, "error": "Arquivo enviado não é um ZIP válido."}
        except Exception as e:
            return {"ok": False, "error": f"Falha na ingestão do ZIP: {e}"}
