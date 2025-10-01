from typing import Any, Dict, List, Optional
from ..infra.db.bq_client import q, fq

class AssetRepository:
    TABLE = "assets"

    def insert(self, record: Dict[str, Any]) -> None:
        """
        INSERT via DML job (sem streaming buffer).
        Campos esperados em record:
          - brand_name, category, subcategory, original_name, path, sequence, url
          - mime_type, file_ext
          - logo_variant, logo_color, color_primary, color_secondary
        """
        sql = f"""
        INSERT INTO {fq(self.TABLE)} (
          brand_name, category, subcategory, original_name, path, sequence, url,
          mime_type, file_ext, logo_variant, logo_color, color_primary, color_secondary
        )
        VALUES (
          @brand_name, @category, @subcategory, @original_name, @path, @sequence, @url,
          @mime_type, @file_ext, @logo_variant, @logo_color, @color_primary, @color_secondary
        )
        """
        q(sql, {
            "brand_name": record.get("brand_name"),
            "category": record.get("category"),
            "subcategory": record.get("subcategory"),
            "original_name": record.get("original_name"),
            "path": record.get("path"),
            "sequence": record.get("sequence"),
            "url": record.get("url"),
            "mime_type": record.get("mime_type"),
            "file_ext": record.get("file_ext"),
            "logo_variant": record.get("logo_variant"),
            "logo_color": record.get("logo_color"),
            "color_primary": record.get("color_primary"),
            "color_secondary": record.get("color_secondary"),
        })

    def list(
        self,
        brand_name: str,
        category: str,
        subcategory: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retorna os itens para o Lovable (sem file_url/stream_url direto da query;
        essas URLs são montadas na service para evitar colunas inexistentes).
        """
        sql = f"""
        SELECT
          brand_name, category, subcategory, original_name, path, sequence, url,
          mime_type, file_ext, logo_variant, logo_color, color_primary, color_secondary
        FROM {fq(self.TABLE)}
        WHERE brand_name = @brand_name
          AND category    = @category
          AND (@subcategory IS NULL OR subcategory = @subcategory)
        ORDER BY sequence ASC NULLS LAST, original_name ASC
        """
        return q(sql, {
            "brand_name": brand_name,
            "category": category,
            "subcategory": subcategory
        })
