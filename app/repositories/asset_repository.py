from ..infra.db.bq_client import q, insert, fq

class AssetRepository:

    def create(self, *, brand_name: str, category: str, subcategory: str | None, sequence: int, original_name: str, path: str, url: str | None = None):
        insert("assets", [{
            "brand_name": brand_name,
            "category": category,
            "subcategory": subcategory,
            "sequence": sequence,
            "original_name": original_name,
            "path": path,
            "url": url,
        }])
        return {
            "brand_name": brand_name,
            "category": category,
            "subcategory": subcategory,
            "sequence": sequence,
            "original_name": original_name,
            "path": path,
            "url": url,
        }

    def list(self, *, brand_name: str, category: str, subcategory: str | None = None):
        sql = f"""
        SELECT brand_name, category, subcategory, sequence, original_name, path, url
        FROM {fq('assets')}
        WHERE brand_name=@bn AND category=@c
        """

        params = {"bn": brand_name, "c": category}
        if subcategory:
            sql += " AND subcategory=@sc"
            params["sc"] = subcategory

        sql += " ORDER BY sequence ASC"

        return q(sql, params)
