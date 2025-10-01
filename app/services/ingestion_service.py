import os
from typing import Dict, Iterable, Optional
from ..repositories.asset_repository import AssetRepository
from ..utils.naming import guess_mime_type, file_ext, try_sequence_from_name, parse_logo_meta

class IngestionService:
    def __init__(self, bucket_name: str, base_gs_url: str = "gs://brand-guides"):
        self.bucket_name = bucket_name
        # Ex.: gs://brand-guides/ccba2/...
        self.base_gs_url = base_gs_url.rstrip("/")
        self.repo = AssetRepository()

    def _build_record(
        self,
        brand_name: str,
        path: str,
    ) -> Dict:
        """
        Monta o registro a partir do caminho no bucket (path relativo dentro do bucket),
        aplicando as novas regras de logos/guidelines.
        """
        original_name = os.path.basename(path)
        mime = guess_mime_type(path)
        ext = file_ext(path)
        seq = try_sequence_from_name(original_name)

        # Inferir category/subcategory a partir do path
        # Esperado path: {brand}/{category}/{subcategory?}/{file}
        parts = path.split("/")
        category = parts[1] if len(parts) > 2 else None
        subcategory = parts[2] if len(parts) > 3 else None

        # Metadados de logo/guideline
        logo_meta = parse_logo_meta(path)

        rec = {
            "brand_name": brand_name,
            "category": category,
            "subcategory": subcategory,
            "original_name": original_name,
            "path": path,  # manter EXATAMENTE como veio
            "sequence": seq,
            "url": f"{self.base_gs_url}/{path}",
            "mime_type": mime,
            "file_ext": ext,
            "logo_variant": logo_meta.get("logo_variant"),
            "logo_color": logo_meta.get("logo_color"),
            "color_primary": logo_meta.get("color_primary"),
            "color_secondary": logo_meta.get("color_secondary"),
        }
        return rec

    def ingest_paths(self, brand_name: str, object_paths: Iterable[str]) -> int:
        """
        Recebe uma lista de paths do bucket (ex.: 'ccba2/logos/primary/arquivo.png')
        e insere no BigQuery com as novas colunas.
        """
        count = 0
        for p in object_paths:
            # Ignore arquivos "ocultos" (ex.: .DS_Store) e pastas
            base = os.path.basename(p)
            if not base or base.startswith("."):
                continue
            # Cria registro e insere
            record = self._build_record(brand_name, p)
            self.repo.insert(record)
            count += 1
        return count

    # Caso você já tenha um coletor do GCS, mantenha e apenas alimente ingest_paths(...)
    # Exemplo (opcional):
    # def ingest_from_gcs_prefix(self, brand_name: str, prefix: str, gcs_client) -> int:
    #     paths = []
    #     for blob in gcs_client.list(prefix):
    #         if not blob.name.endswith("/"):
    #             paths.append(blob.name)
    #     return self.ingest_paths(brand_name, paths)
