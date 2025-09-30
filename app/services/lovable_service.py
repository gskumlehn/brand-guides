from ..repositories.asset_repository import AssetRepository
from ..utils.font_meta import (
    family_from_filename,
    weight_from_filename,
    style_from_filename,
    ext_to_format
)


class LovableService:

    def __init__(self):
        self.repo = AssetRepository()


    def get_assets(
        self,
        *,
        brand_name: str,
        category: str,
        subcategory: str | None = None
    ):
        return self.repo.list(
            brand_name = brand_name,
            category   = category,
            subcategory = subcategory
        )


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
