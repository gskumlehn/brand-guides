# app/repositories/assets_repository.py
from typing import List, Dict, Any
from ..infra.db.bq_client import q, fq

class AssetsRepository:
    def list_categories_and_subs(self, brand: str) -> List[Dict[str, Any]]:
        sql = f"""
        WITH base AS (
          SELECT
            category_key,
            ANY_VALUE(category_label) AS category_label,
            ANY_VALUE(category_seq) AS category_seq,
            subcategory_key,
            ANY_VALUE(subcategory_label) AS subcategory_label,
            ANY_VALUE(subcategory_seq) AS subcategory_seq,
            ANY_VALUE(columns) AS columns
          FROM {fq('assets')}
          WHERE brand_name = @brand
          GROUP BY category_key, subcategory_key
        ),
        cleaned AS (
          SELECT
            category_key, category_label, category_seq,
            subcategory_key, subcategory_label, subcategory_seq, columns
          FROM base
          WHERE category_key IS NOT NULL AND category_key != ''
        )
        SELECT
          category_key, category_label, category_seq,
          subcategory_key, subcategory_label, subcategory_seq, columns
        FROM cleaned
        ORDER BY category_seq, category_key, subcategory_seq
        """
        return q(sql, {"brand": brand})

    def list_gallery(self, brand: str) -> List[Dict[str, Any]]:
        sql = f"""
        SELECT
          category_key, category_label, category_seq,
          subcategory_key, subcategory_label, subcategory_seq, columns,
          is_original, original_name, path, url, sequence
        FROM {fq('assets')}
        WHERE brand_name=@brand AND asset_type='image'
        ORDER BY category_seq, subcategory_seq, sequence, original_name
        """
        return q(sql, {"brand": brand})
