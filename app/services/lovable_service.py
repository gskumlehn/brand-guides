from ..repositories.asset_repository import AssetRepository
from ..infra.db.bq_client import q

class LovableService:
    def __init__(self):
        self.repo = AssetRepository()

    def get_assets(self, brand_name: str | None, category: str | None, subcategory: str | None):
        rows = self.repo.list(brand_name=brand_name, category=category, subcategory=subcategory)
        # acrescenta URLs de conveniência de stream/download (sem depender de colunas novas)
        result = []
        for r in rows:
            gcs_url = r.get("url")
            path = r.get("path")
            result.append({
                **r,
                "stream_url": f"/stream/{path}" if path else None,
                "download_url": f"/download/{path}" if path else None,
                "url": gcs_url,  # mantém gs://...
            })
        return result

    def get_colors(self, brand_name: str | None):
        sql = """
        SELECT brand_name, color_name, hex, role
        FROM {colors}
        WHERE brand_name = @brand_name
        ORDER BY brand_name, role
        """.format(colors=self.repo.colors_table())
        return q(sql, {"brand_name": brand_name} if brand_name else None)
