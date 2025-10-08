# app/services/assets_service.py
from typing import Any, Dict, List, Optional
import os
from ..repositories.assets_repository import AssetsRepository
from ..infra.bucket.gcs_client import GCSClient

_BUCKET = os.getenv("GCS_BUCKET", "brand-guides")

class AssetsService:
    def __init__(self):
        self.repo = AssetsRepository()
        self.gcs = GCSClient()

    def sidebar(self, brand: str) -> List[Dict[str, Any]]:
        return self.repo.sidebar(brand)

    def gallery(
        self,
        brand: str,
        category_key: Optional[str] = None,
        subcategory_seq: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        data = self.repo.gallery(brand, category_key, subcategory_seq)
        # Assina URLs (bucket privado)
        for cat in data:
            for sub in cat["subcategories"]:
                signed = []
                for img in sub["images"]:
                    path = img["path"]
                    signed.append(self.gcs.signed_url(_BUCKET, path, minutes=15))
                sub["stream"] = signed
        return data

    def colors(self, brand: str) -> Dict[str, Any]:
        return self.repo.colors(brand)
