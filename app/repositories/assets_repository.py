from typing import List, Dict, Any
from ..infra.db.bq_client import q, fq

class AssetsRepository:
    def list_assets(self, brand_name: str, category: str) -> List[Dict[str, Any]]:
        sql = f"""
        SELECT
          brand_name,
          category,
          subcategory,
          sequence,
          original_name,
          path,
          url,
          created_at
        FROM {fq('assets')}
        WHERE LOWER(brand_name) = LOWER(@brand_name)
          AND LOWER(category)   = LOWER(@category)
        ORDER BY COALESCE(sequence, 0), original_name
        """
        params = {
            "brand_name": brand_name,
            "category": category,
        }
        return q(sql, params=params)
