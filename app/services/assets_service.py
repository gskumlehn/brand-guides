# app/services/assets_service.py
from typing import Any, Dict, List, Optional
from ..repositories.assets_repository import AssetsRepository


class AssetsService:
    def __init__(self):
        self.repo = AssetsRepository()

    def sidebar(self, brand: str) -> List[Dict[str, Any]]:
        return self.repo.sidebar(brand)

    def gallery(
        self,
        brand: str,
        category_key: Optional[str] = None,
        subcategory_seq: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        return self.repo.gallery(brand, category_key, subcategory_seq)
