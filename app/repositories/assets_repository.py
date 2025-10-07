# app/repositories/assets_repository.py
from typing import List, Dict, Any
from ..infra.db.bq_client import q, fq

class AssetsRepository:
    # Compat (se o projeto já possuía):
    def list_assets(self, brand_name: str, category: str) -> List[Dict[str, Any]]:
        sql = f"""
        SELECT * FROM {fq('assets')}
        WHERE LOWER(brand_name) = LOWER(@brand) AND COALESCE(category_key, category) = @category
        """
        return q(sql, {"brand": brand_name, "category": category})

    def get_nav_structure(self, brand_name: str) -> List[Dict[str, Any]]:
        sql = f"""
        WITH base AS (
          SELECT
            category_seq,
            ANY_VALUE(COALESCE(category_key, LOWER(category_label))) AS category_key,
            ANY_VALUE(COALESCE(category_label, category))            AS category_label,
            subcategory_seq,
            ANY_VALUE(COALESCE(subcategory_key, LOWER(subcategory_label))) AS subcategory_key,
            IFNULL(ANY_VALUE(subcategory_label), "") AS subcategory_label,
            ANY_VALUE(columns) AS columns
          FROM {fq('assets')}
          WHERE LOWER(brand_name) = LOWER(@brand)
          GROUP BY category_seq, subcategory_seq
        )
        SELECT
          category_seq, category_key, category_label,
          ARRAY_AGG(STRUCT(subcategory_seq, subcategory_key, subcategory_label, columns) ORDER BY subcategory_seq) AS subcategories
        FROM base
        GROUP BY category_seq, category_key, category_label
        ORDER BY category_seq
        """
        return q(sql, {"brand": brand_name})

    def get_gallery_rows(self, brand_name: str) -> List[Dict[str, Any]]:
        sql = f"""
        SELECT
          COALESCE(category_key, LOWER(category_label)) AS category_key,
          COALESCE(category_label, category)           AS category_label,
          category_seq,
          COALESCE(subcategory_key, LOWER(subcategory_label)) AS subcategory_key,
          IFNULL(subcategory_label, "") AS subcategory_label,
          subcategory_seq,
          columns,
          is_original,
          asset_type,
          text_content,
          sequence,
          original_name,
          path,
          url
        FROM {fq('assets')}
        WHERE LOWER(brand_name) = LOWER(@brand)
          AND (asset_type IS NULL OR asset_type = 'image')
        ORDER BY category_seq, subcategory_seq, COALESCE(sequence,0), original_name
        """
        return q(sql, {"brand": brand_name})
