# app/services/ingestion_service.py
from __future__ import annotations

import io
import posixpath
from zipfile import ZipFile
from typing import Iterable, List, Dict, Optional

from ..repositories.asset_repository import AssetRepository
from ..infra.bucket.gcs_client import GCSClient
from ..utils.naming import (
    is_png, is_jpg,
    parse_sequence,
    parse_logo_color_variant,
    parse_logo_guideline,
    parse_avatar_variant,
)

class IngestionService:
    """
    Lê um ZIP seguindo a estrutura predefinida, envia arquivos para o GCS
    e registra metadados no BigQuery via AssetRepository.
    """
    def __init__(self, repo: AssetRepository, gcs: GCSClient, bucket_name: str):
        self.repo = repo
        self.gcs = gcs
        self.bucket = bucket_name

    def _gcs_url(self, path: str) -> str:
        # NÃO normalizar nomes (preservar acentos/espacos conforme solicitado)
        return f"gs://{self.bucket}/{path}"

    def ingest_zip(self, brand_name: str, zip_bytes: bytes) -> Dict[str, int]:
        """
        Caminha pelas pastas conhecidas e aplica regras por categoria/subcategoria.
        """
        created = 0
        skipped = 0

        with ZipFile(io.BytesIO(zip_bytes)) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                rel_path = info.filename  # manter como vem
                parts = rel_path.split("/")
                if len(parts) < 2:
                    skipped += 1
                    continue

                top = parts[0].lower()
                filename = parts[-1]

                # ----- LOGOS (png) -----
                if top == "logos":
                    if len(parts) >= 2 and parts[1].lower() in {"primary", "secondary_horizontal", "secondary_vertical"}:
                        subcategory = parts[1].lower()
                        if not is_png(filename):
                            skipped += 1; continue
                        variant = parse_logo_color_variant(filename)
                        if not variant:
                            skipped += 1; continue

                        # upload
                        dest = posixpath.join(brand_name.lower(), rel_path)
                        self.gcs.upload_bytes(self.bucket, dest, z.read(info), content_type="image/png")

                        self.repo.insert({
                            "brand_name": brand_name,
                            "category": "logos",
                            "subcategory": subcategory,
                            "path": dest,
                            "original_name": filename,
                            "url": self._gcs_url(dest),
                            "sequence": None,
                            "applied_color": variant,  # persistimos a cor aplicada
                        })
                        created += 1
                        continue

                    # logos/guidelines/...
                    if len(parts) >= 3 and parts[1].lower() == "guidelines":
                        subfolder = parts[2].lower() if len(parts) >= 3 else None
                        if subfolder not in {"primary", "secondary_horizontal", "secondary_vertical"}:
                            skipped += 1; continue
                        if not is_jpg(filename):
                            skipped += 1; continue
                        meta = parse_logo_guideline(filename)
                        if not meta:
                            skipped += 1; continue

                        dest = posixpath.join(brand_name.lower(), rel_path)
                        self.gcs.upload_bytes(self.bucket, dest, z.read(info), content_type="image/jpeg")

                        self.repo.insert({
                            "brand_name": brand_name,
                            "category": "logos",
                            "subcategory": f"guidelines/{subfolder}",
                            "path": dest,
                            "original_name": filename,
                            "url": self._gcs_url(dest),
                            "sequence": meta["sequence"],
                            "applied_color": f"{meta['main_color']}_{meta['secondary_color']}",  # ex: primary_secondary
                        })
                        created += 1
                        continue

                # ----- COLORS -----
                if top == "colors":
                    if filename.lower() == "colors.json":
                        # envia como json
                        dest = posixpath.join(brand_name.lower(), rel_path)
                        self.gcs.upload_bytes(self.bucket, dest, z.read(info), content_type="application/json")
                        self.repo.insert({
                            "brand_name": brand_name,
                            "category": "colors",
                            "subcategory": None,
                            "path": dest,
                            "original_name": filename,
                            "url": self._gcs_url(dest),
                            "sequence": None,
                            "applied_color": None,
                        })
                        created += 1
                        continue
                    # JPGs com NN_
                    if not is_jpg(filename):
                        skipped += 1; continue
                    seq = parse_sequence(filename)
                    if seq is None:
                        skipped += 1; continue
                    dest = posixpath.join(brand_name.lower(), rel_path)
                    self.gcs.upload_bytes(self.bucket, dest, z.read(info), content_type="image/jpeg")
                    self.repo.insert({
                        "brand_name": brand_name,
                        "category": "colors",
                        "subcategory": "images",
                        "path": dest,
                        "original_name": filename,
                        "url": self._gcs_url(dest),
                        "sequence": seq,
                        "applied_color": None,
                    })
                    created += 1
                    continue

                # ----- AVATARS (NOVO) -----
                if top == "avatars":
                    if len(parts) < 3:
                        skipped += 1; continue
                    subcategory = parts[1].lower()  # round | square | app
                    if subcategory not in {"round", "square", "app"}:
                        skipped += 1; continue
                    if not is_png(filename):
                        skipped += 1; continue
                    variant = parse_avatar_variant(filename)  # primary | secondary
                    if not variant:
                        skipped += 1; continue

                    dest = posixpath.join(brand_name.lower(), rel_path)
                    self.gcs.upload_bytes(self.bucket, dest, z.read(info), content_type="image/png")
                    self.repo.insert({
                        "brand_name": brand_name,
                        "category": "avatars",
                        "subcategory": subcategory,  # round | square | app
                        "path": dest,
                        "original_name": filename,
                        "url": self._gcs_url(dest),
                        "sequence": None,            # não há NN_ em avatars
                        "applied_color": variant,    # primary ou secondary
                    })
                    created += 1
                    continue

                # ----- APPLICATIONS (NN_) -----
                if top == "applications":
                    seq = parse_sequence(filename)
                    if seq is None:
                        skipped += 1; continue
                    # aceitar png/jpg
                    ctype = "image/png" if is_png(filename) else "image/jpeg" if is_jpg(filename) else None
                    if not ctype:
                        skipped += 1; continue

                    dest = posixpath.join(brand_name.lower(), rel_path)
                    self.gcs.upload_bytes(self.bucket, dest, z.read(info), content_type=ctype)
                    self.repo.insert({
                        "brand_name": brand_name,
                        "category": "applications",
                        "subcategory": None,
                        "path": dest,
                        "original_name": filename,
                        "url": self._gcs_url(dest),
                        "sequence": seq,
                        "applied_color": None,
                    })
                    created += 1
                    continue

                # ----- ICONS (NN_) -----
                if top == "icons":
                    seq = parse_sequence(filename)
                    if seq is None:
                        skipped += 1; continue
                    ctype = "image/png" if is_png(filename) else "image/jpeg" if is_jpg(filename) else None
                    if not ctype:
                        skipped += 1; continue

                    dest = posixpath.join(brand_name.lower(), rel_path)
                    self.gcs.upload_bytes(self.bucket, dest, z.read(info), content_type=ctype)
                    self.repo.insert({
                        "brand_name": brand_name,
                        "category": "icons",
                        "subcategory": None,
                        "path": dest,
                        "original_name": filename,
                        "url": self._gcs_url(dest),
                        "sequence": seq,
                        "applied_color": None,
                    })
                    created += 1
                    continue

                # ----- GRAPHICS (livre) -----
                if top == "graphics":
                    ctype = "image/png" if is_png(filename) else "image/jpeg" if is_jpg(filename) else None
                    if not ctype:
                        skipped += 1; continue

                    dest = posixpath.join(brand_name.lower(), rel_path)
                    self.gcs.upload_bytes(self.bucket, dest, z.read(info), content_type=ctype)
                    self.repo.insert({
                        "brand_name": brand_name,
                        "category": "graphics",
                        "subcategory": None,
                        "path": dest,
                        "original_name": filename,
                        "url": self._gcs_url(dest),
                        "sequence": None,
                        "applied_color": None,
                    })
                    created += 1
                    continue

                # ----- FONTS -----
                if top == "fonts":
                    dest = posixpath.join(brand_name.lower(), rel_path)
                    self.gcs.upload_bytes(self.bucket, dest, z.read(info), content_type="application/octet-stream")
                    self.repo.insert({
                        "brand_name": brand_name,
                        "category": "fonts",
                        "subcategory": None,
                        "path": dest,
                        "original_name": filename,
                        "url": self._gcs_url(dest),
                        "sequence": None,
                        "applied_color": None,
                    })
                    created += 1
                    continue

                skipped += 1

        return {"created": created, "skipped": skipped}
