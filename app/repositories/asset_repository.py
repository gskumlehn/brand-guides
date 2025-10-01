from ..infra.db.bq_client import q, fq

class AssetRepository:
    def assets_table(self) -> str:
        return fq("assets")

    def colors_table(self) -> str:
        return fq("colors")

    def list(self, brand_name: str | None, category: str | None, subcategory: str | None):
        sql = f"""
        SELECT brand_name, category, subcategory, sequence, original_name, path, url, created_at
        FROM {self.assets_table()}
        WHERE (@brand_name IS NULL OR brand_name = @brand_name)
          AND (@category   IS NULL OR category   = @category)
          AND (@subcategory IS NULL OR subcategory = @subcategory)
        ORDER BY brand_name, category, subcategory, sequence
        """
        params = {}
        if brand_name is not None: params["brand_name"] = brand_name
        if category   is not None: params["category"]   = category
        if subcategory is not None: params["subcategory"] = subcategory
        return q(sql, params if params else None)
