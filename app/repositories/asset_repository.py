from typing import Iterable, Mapping, Optional, List
from ..infra.db.bq_client import q, insert, fq, ensure_assets_tables, merge_assets_from_stage, truncate

class AssetRepository:
    def list(self, brand_name: Optional[str]=None, category: Optional[str]=None, subcategory: Optional[str]=None) -> List[Mapping]:
        ensure_assets_tables()
        where = []
        params = {}
        if brand_name:
            where.append("brand_name = @brand_name")
            params["brand_name"] = brand_name
        if category:
            where.append("category = @category")
            params["category"] = category
        if subcategory:
            where.append("IFNULL(subcategory,'') = IFNULL(@subcategory,'')")
            params["subcategory"] = subcategory

        sql = f"""
        SELECT brand_name, category, subcategory, sequence, original_name, path, url, file_url, stream_url
        FROM {fq('assets')}
        {"WHERE " + " AND ".join(where) if where else ""}
        ORDER BY sequence, path
        """
        return q(sql, params)

    def upsert_batch(self, rows: Iterable[Mapping]):
        ensure_assets_tables()
        truncate("assets_stage")
        insert("assets_stage", rows)          # LOAD JOB
        merge_assets_from_stage("assets_stage")
        truncate("assets_stage")
