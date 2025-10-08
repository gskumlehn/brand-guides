# app/services/assets_service.py  (arquivo completo, atualizado para usar /assets/stream)
from typing import Any, Dict, List, Optional
import os
from urllib.parse import quote
from ..repositories.assets_repository import AssetsRepository
from ..infra.bucket.gcs_client import GCSClient

_BUCKET = os.getenv("GCS_BUCKET", "brand-guides")
_BASE_PATH = os.getenv("BASE_PATH", "").rstrip("/")

class AssetsService:
    def __init__(self):
        self.repo = AssetsRepository()
        self.gcs = GCSClient()

    def sidebar(self, brand: str) -> List[Dict[str, Any]]:
        return self.repo.sidebar(brand)

    def _make_stream_url(self, brand: str, path: str) -> str:
        # Link interno da própria aplicação (proxy), sem expor Storage
        qp = f"brand_name={quote(brand)}&path={quote(path)}"
        base = f"{_BASE_PATH}/assets/stream" if _BASE_PATH else "/assets/stream"
        return f"{base}?{qp}"

    def gallery(
        self,
        brand: str,
        category_key: Optional[str] = None,
        subcategory_seq: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        data = self.repo.gallery(brand, category_key, subcategory_seq)

        # Para cada imagem, substituir o campo "url" por link interno /assets/stream
        # e remover qualquer URL externa eventualmente retornada pelo repositório.
        for cat in data:
            for sub in cat.get("subcategories", []):
                # opcional: também expor a lista já sequenciada para o front (mantemos)
                sub["stream"] = []
                for img in sub.get("images", []):
                    stream_url = self._make_stream_url(brand, img["path"])
                    # sobrescreve url para o link interno
                    img["url"] = stream_url
                    # remove qualquer traço de URL externa (defensivo)
                    # (já sobrescrevemos, mas garantimos que não há campo alternativo)
                    img.pop("signed_url", None)
                    sub["stream"].append(stream_url)
        return data

    def colors(self, brand: str) -> Dict[str, Any]:
        return self.repo.colors(brand)
