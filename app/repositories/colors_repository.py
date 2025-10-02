from typing import List, Dict, Any
from ..infra.db.bq_client import q, fq

class ColorsRepository:
    def list_colors(self, brand_name: str) -> List[Dict[str, Any]]:
        sql = f"""
        SELECT
          brand_name,
          color_name,
          hex,
          role,
          created_at
        FROM {fq('colors')}
        WHERE LOWER(brand_name) = LOWER(@brand_name)
        ORDER BY role, color_name
        """
        params = {
            "brand_name": brand_name,
        }
        return q(sql, params=params)
