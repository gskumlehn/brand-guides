from typing import List, Dict, Any
from ..infra.db.bq_client import q, fq

class ColorsRepository:
    def list_colors(self, brand_name: str) -> List[Dict[str, Any]]:
        sql = f"""
        SELECT
          brand_name,
          palette_key,
          color_key,
          color_label,
          hex,
          rgb_txt,
          cmic_txt,
          recs_txt,
          role,
          sequence,
          created_at
        FROM {fq('color')}
        WHERE LOWER(brand_name) = LOWER(@brand_name)
        ORDER BY COALESCE(sequence, 0), role, color_label
        """
        params = {"brand_name": brand_name}
        return q(sql, params=params)
