# app/controllers/ingestion_controller.py
from __future__ import annotations

import io
import json
from zipfile import ZipFile, ZIP_DEFLATED
from flask import Blueprint, Response, render_template, send_file

ingestion_bp = Blueprint("ingestion", __name__)

# JSON de cores (SEM "primary" conforme combinado)
EMPTY_COLORS_JSON = {
    "secondary": [],
    "others": []
}

# READMEs
README_ROOT = """Estrutura base para ingestão do Guia de Marca.
Preencha as pastas conforme os READMEs de cada diretório e compacte tudo novamente para envio.
"""

README_LOGOS = """LOGOS (PNG, sem NN_)
Coloque arquivos PNG com fundo transparente nas pastas abaixo:
- logos/primary/{primary|secondary|black|white}.png
- logos/secondary_horizontal/{primary|secondary|black|white}.png
- logos/secondary_vertical/{primary|secondary|black|white}.png

O nome do arquivo define a variação de cor aplicada:
  primary.png, secondary.png, black.png, white.png
"""

README_GUIDELINES = """GUIDELINES (JPG, com NN_)
Dentro de logos/guidelines/{primary|secondary_horizontal|secondary_vertical} use:
  {NN}_{logoType}-{corPrincipal}_{corSecundaria}.jpg

Onde:
  logoType ∈ {primary, secondary_horizontal, secondary_vertical}
  cores    ∈ {primary, secondary, black, white}

Exemplos:
  01_primary-primary_secondary.jpg
  02_secondary_horizontal-black_white.jpg
"""

README_COLORS = """COLORS
- colors.json: estrutura de cores (SEM 'primary').
- Imagens .jpg com NN_ (ex: 01_palette.jpg).

Exemplo de colors.json (vazio):
{
  "secondary": [],
  "others": []
}
"""

README_AVATARS = """AVATARS (PNG, sem NN_)
Subpastas:
  - avatars/round/
  - avatars/square/
  - avatars/app/

Regra de nome do arquivo (apenas cor):
  primary.png
  secondary.png

Exemplos:
  avatars/round/primary.png
  avatars/square/secondary.png
  avatars/app/primary.png
"""

README_APPLICATIONS = """APPLICATIONS (com NN_)
Arquivos devem começar com NN_.
Exemplos:
  01_app-tile.png
  02_mock.jpg
"""

README_GRAPHICS = """GRAPHICS
.png/.jpg, numeração opcional.
"""

README_ICONS = """ICONS (com NN_)
Arquivos devem começar com NN_.
Exemplos:
  01_download.png
  02_upload.png
"""

README_FONTS = """FONTS
Coloque aqui os arquivos de fonte (ex: .ttf, .otf) se necessário.
"""


@ingestion_bp.get("/")
def index():
    # Renderiza a página de ingestão existente, injetando o JSON vazio
    return render_template(
        "ingestion.html",
        empty_colors_json=json.dumps(EMPTY_COLORS_JSON, ensure_ascii=False, indent=2),
    )


@ingestion_bp.get("/template.zip")
def download_template_zip():
    """
    Gera e retorna um .zip com TODAS as pastas da estrutura,
    incluindo explicitamente:
      logos/{primary,secondary_horizontal,secondary_vertical}
      logos/guidelines/{primary,secondary_horizontal,secondary_vertical}
      avatars/{round,square,app}
    Cada pasta recebe um README.txt para garantir presença no zip.
    """
    buf = io.BytesIO()
    with ZipFile(buf, mode="w", compression=ZIP_DEFLATED) as z:
        # raiz
        z.writestr("README.txt", README_ROOT)

        # ---- LOGOS ----
        z.writestr("logos/README.txt", README_LOGOS)
        z.writestr("logos/primary/README.txt", README_LOGOS)
        z.writestr("logos/secondary_horizontal/README.txt", README_LOGOS)
        z.writestr("logos/secondary_vertical/README.txt", README_LOGOS)

        # guidelines (subpastas)
        z.writestr("logos/guidelines/README.txt", README_GUIDELINES)
        z.writestr("logos/guidelines/primary/README.txt", README_GUIDELINES)
        z.writestr("logos/guidelines/secondary_horizontal/README.txt", README_GUIDELINES)
        z.writestr("logos/guidelines/secondary_vertical/README.txt", README_GUIDELINES)

        # ---- COLORS ----
        z.writestr("colors/README.txt", README_COLORS)
        z.writestr("colors/colors.json", json.dumps(EMPTY_COLORS_JSON, ensure_ascii=False, indent=2))

        # ---- AVATARS ----
        z.writestr("avatars/README.txt", README_AVATARS)
        z.writestr("avatars/round/README.txt", README_AVATARS)
        z.writestr("avatars/square/README.txt", README_AVATARS)
        z.writestr("avatars/app/README.txt", README_AVATARS)

        # ---- OUTRAS PASTAS ----
        z.writestr("applications/README.txt", README_APPLICATIONS)
        z.writestr("graphics/README.txt", README_GRAPHICS)
        z.writestr("icons/README.txt", README_ICONS)
        z.writestr("fonts/README.txt", README_FONTS)

    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name="template.zip",
        max_age=0,
        etag=False,
        conditional=False,
        last_modified=None,
    )


@ingestion_bp.get("/colors.json")
def download_empty_colors_json():
    payload = json.dumps(EMPTY_COLORS_JSON, ensure_ascii=False, indent=2)
    return Response(payload, mimetype="application/json")
