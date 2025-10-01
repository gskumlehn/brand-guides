from typing import Dict, List
from ..repositories.asset_repository import AssetRepository
from ..infra.db.bq_client import q, fq
from ..utils.font_meta import (
    family_from_filename,
    weight_from_filename,
    style_from_filename,
    ext_to_format
)
import os

BASE = os.getenv("PUBLIC_BASE", "https://brand-guides-561373422085.southamerica-east1.run.app")

class LovableService:
    def __init__(self):
        self.repo = AssetRepository()

    def get_assets(self, brand_name: str, category: str, subcategory: str | None):
        rows = self.repo.list(brand_name, category, subcategory)
        out: List[Dict] = []
        for r in rows:
            # Montar stream_url e file_url sem mexer no path original
            path = r["path"]
            stream_url = f"{BASE}/stream/{path}"
            file_url = f"{BASE}/files/{path}"
            r_out = dict(r)
            r_out["stream_url"] = stream_url
            r_out["file_url"] = file_url
            out.append(r_out)
        return out

    def generate_webfonts_css(
        self,
        *,
        brand_name: str,
        prefer_stream: bool = True
    ) -> str:
        """
        Gera @font-face para todos os assets de category='fonts' da marca.
        Usa /stream/<path> por padrão (mesma origem, sem CORS).
        """
        fonts = self.repo.list(
            brand_name = brand_name,
            category   = "fonts",
            subcategory = None
        )

        lines: list[str] = []
        for f in fonts:
            path = f.get("path", "")
            name = f.get("original_name", "")

            src_url = f"/stream/{path}" if prefer_stream else f"/files/{path}"
            family  = family_from_filename(name)
            weight  = weight_from_filename(name)
            style   = style_from_filename(name)
            fmt     = ext_to_format(name)

            lines.append(
                "@font-face {"
                f"\n  font-family: '{family}';"
                f"\n  src: url('{src_url}') format('{fmt}');"
                f"\n  font-weight: {weight};"
                f"\n  font-style: {style};"
                f"\n  font-display: swap;"
                "\n}"
            )

        return "\n\n".join(lines) + ("\n" if lines else "/* sem fontes */\n")

    def get_colors(self, brand_name: str):
        sql = f"""
        SELECT
          color_name AS name,
          hex,
          role
        FROM {fq('colors')}
        WHERE brand_name = @brand
        ORDER BY
          CASE role
            WHEN 'primary' THEN 0
            WHEN 'secondary' THEN 1
            ELSE 2
          END,
          name
        """
        return q(sql, {"brand": brand_name.upper()})