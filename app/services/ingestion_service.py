import os
import json
from typing import List, Dict, Optional
from ..infra.bucket.gcs_client import GCSClient
from ..infra.db.bq_client import ensure_all_tables, load_json
from ..utils.naming import parse_sequence


class IngestionService:
    """Serviço de ingestão a partir de um diretório local previamente extraído."""

    def __init__(self, bucket_name: str):
        self.bucket = GCSClient(bucket_name)

    def ingest(self, brand_slug: str, local_root: str) -> Dict[str, int]:
        """
        Foco aqui em 'colors':
          - aceita imagens .jpg/.jpeg (paleta completa)
          - aceita um 'colors.json' descrevendo as cores sem 'priority'
            Estrutura esperada:
            {
              "primary":   [ {"name":"...", "hex":"#..."} ],
              "secondary": [ {"name":"...", "hex":"#..."} ],
              "others":    [ {"name":"...", "hex":"#..."} ]
            }
        """
        ensure_all_tables()

        assets_rows: List[Dict] = []
        colors_rows: List[Dict] = []

        # ---- COLORS ----
        colors_dir = os.path.join(local_root, "colors")
        colors_json: Optional[dict] = None

        if os.path.isdir(colors_dir):
            for fname in sorted(os.listdir(colors_dir)):
                fpath = os.path.join(colors_dir, fname)
                if os.path.isdir(fpath):
                    continue

                low = fname.lower()
                name, ext = os.path.splitext(low)

                if low == "colors.json":
                    with open(fpath, "r", encoding="utf-8") as f:
                        colors_json = json.load(f)
                    continue

                # imagem(s) completa(s) da paleta
                if ext in [".jpg", ".jpeg"]:
                    gcs_path = f"{brand_slug}/colors/{fname}"
                    url = self.bucket.upload_file(fpath, gcs_path, public=False)

                    assets_rows.append(
                        {
                            "brand_name": brand_slug.upper(),
                            "category": "colors",
                            "subcategory": None,
                            "sequence": parse_sequence(fname),
                            "original_name": fname,
                            "path": gcs_path,
                            "url": url,
                        }
                    )

        # persiste assets (imagens da paleta)
        if assets_rows:
            load_json("assets", assets_rows)

        # persiste paleta do colors.json (SEM priority/source_*)
        if colors_json:
            colors_rows.extend(self._flatten_colors_json(brand_slug, colors_json))
            if colors_rows:
                load_json("colors", colors_rows)

        return {
            "assets_inserted": len(assets_rows),
            "colors_inserted": len(colors_rows),
        }

    # --------------------------

    @staticmethod
    def _flatten_colors_json(brand_slug: str, payload: dict) -> List[Dict]:
        """
        Mapeia primary/secondary/others -> linhas na tabela 'colors'
        Sem 'priority', sem 'source_path', sem 'source_file'.
        """
        out: List[Dict] = []

        def add_role(role_key: str) -> None:
            items = payload.get(role_key) or []
            for c in items:
                out.append(
                    {
                        "brand_name": brand_slug.upper(),
                        "color_name": c.get("name"),
                        "hex": c.get("hex"),
                        "role": role_key,
                    }
                )

        for role in ("primary", "secondary", "others"):
            add_role(role)

        return out
