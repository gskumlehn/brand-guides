import os, zipfile
from ..repositories.asset_repository import AssetRepository
from ..infra.bucket.gcs_client import GCSClient
from ..utils.naming import slugify_ascii, sanitize_filename
from ..utils.zip_utils import group_by_first_component
from ..utils.validators import resolve_canonical_category, FLAT_CATEGORIES, LOGO_SUBTYPE_SYNONYMS
from ..utils.filters import is_sequenced_asset, extract_sequence

class IngestionService:
    def __init__(self):
        self.repo = AssetRepository()
        self.bucket = GCSClient()

    def _logo_subfolder_canon(self, name: str) -> str | None:
        s = sanitize_filename(name).replace("-", "_")
        for canon, aliases in LOGO_SUBTYPE_SYNONYMS.items():
            if s == canon or s in aliases:
                return canon
        return None

    def ingest_zip(self, *, brand_name: str, zip_file):
        brand_slug = slugify_ascii(brand_name)
        zf = zipfile.ZipFile(zip_file.stream)
        groups = group_by_first_component(zf)
        uploaded = {}

        for first, items in groups.items():
            category = resolve_canonical_category(first)
            if not category:
                continue

            # Categorias planas: somente arquivos diretamente dentro da pasta e com NN_
            if category in FLAT_CATEGORIES:
                flat = [(p, d) for p, d in items if p and "/" not in p and is_sequenced_asset(p)]
                if not flat:
                    continue
                for rel, data in flat:
                    name = os.path.basename(rel)                    # já vem com NN_
                    seq = extract_sequence(name)
                    if seq is None:
                        continue
                    path = f"{brand_slug}/{category}/{name}"        # mantém nome original
                    url = self.bucket.upload_bytes(path=path, data=data, content_type=None)
                    asset = self.repo.create(
                        brand_name=brand_name,
                        category=category,
                        subcategory=None,
                        sequence=seq,
                        original_name=name,
                        path=path,
                        url=url,
                    )
                    uploaded.setdefault(category, []).append(asset)
                continue

            # logos / graphics / avatars: exigem subpasta; somente arquivos NN_ dentro da subpasta
            submap = {}
            for rel, data in items:
                if "/" not in rel:
                    continue
                sub, filename = rel.split("/", 1)
                if not is_sequenced_asset(filename):
                    continue
                submap.setdefault(sub, []).append((filename, data))

            for sub, subitems in submap.items():
                if not subitems:
                    continue

                if category == "logos":
                    canon_sub = self._logo_subfolder_canon(sub)
                    if not canon_sub:
                        continue
                    for name, data in subitems:
                        seq = extract_sequence(name)
                        if seq is None:
                            continue
                        path = f"{brand_slug}/{category}/{canon_sub}/{name}"
                        url = self.bucket.upload_bytes(path=path, data=data, content_type=None)
                        asset = self.repo.create(
                            brand_name=brand_name,
                            category=category,
                            subcategory=canon_sub,
                            sequence=seq,
                            original_name=name,
                            path=path,
                            url=url,
                        )
                        uploaded.setdefault(category, []).append(asset)
                else:
                    # graphics / avatars: subpasta é persistida como subcategory (sanitizada)
                    canon_sub = sanitize_filename(sub)
                    for name, data in subitems:
                        seq = extract_sequence(name)
                        if seq is None:
                            continue
                        path = f"{brand_slug}/{category}/{canon_sub}/{name}"
                        url = self.bucket.upload_bytes(path=path, data=data, content_type=None)
                        asset = self.repo.create(
                            brand_name=brand_name,
                            category=category,
                            subcategory=canon_sub,
                            sequence=seq,
                            original_name=name,
                            path=path,
                            url=url,
                        )
                        uploaded.setdefault(category, []).append(asset)

        return {"brand_name": brand_name, "brand_slug": brand_slug, "uploaded": uploaded}
