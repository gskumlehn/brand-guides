from typing import List, Dict
from ..repositories.assets_repository import AssetsRepository
from ..repositories.colors_repository import ColorsRepository

class LovableService:
    def __init__(self):
        self.assets_repo = AssetsRepository()
        self.colors_repo = ColorsRepository()

    def get_assets(self, brand_name: str, category: str) -> List[Dict]:
        return self.assets_repo.list_assets(
            brand_name=brand_name.strip(),
            category=category.strip().lower(),
        )

    def get_colors(self, brand_name: str) -> List[Dict]:
        return self.colors_repo.list_colors(
            brand_name=brand_name.strip(),
        )
