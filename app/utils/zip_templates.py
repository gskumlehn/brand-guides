from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
import textwrap
import json

def _root_readme() -> str:
    return textwrap.dedent("""\
    Brand Guides – Estrutura de Ingestão (Atualizada)
    ================================================

    Pastas de topo:
      /logos
      /colors
      /avatars
      /applications
      /graphics
      /icons
      /fonts

    LOGOS (PNG, SEM NN_)
    --------------------
    Layouts:
      logos/primary/
      logos/secondary_horizontal/
      logos/secondary_vertical/

    Nome dos arquivos (variação de cor aplicada):
      {variant}.png
    Onde {variant} ∈ {primary, secondary, black, white}

    Exemplos:
      primary.png
      secondary.png
      black.png
      white.png

    GUIDELINES (JPG) – dentro de logos/ (COM NN_)
    ---------------------------------------------
      logos/guidelines/primary/
      logos/guidelines/secondary_horizontal/
      logos/guidelines/secondary_vertical/

    Nome:
      {NN}_{logoType}-{corPrincipal}_{corSecundária}.jpg
    Onde logoType ∈ {primary, secondary_horizontal, secondary_vertical}
          cores ∈ {primary, secondary, black, white}

    Exemplos:
      01_primary-primary_secondary.jpg
      02_secondary_horizontal-black_white.jpg

    COLORS (paleta)
    ---------------
      colors/colors.json            # obrigatório (estrutura abaixo)
      colors/{NN}_nome.jpg          # imagens da paleta DEVEM ter NN_

    Estrutura de colors.json:
    {
      "primary":   [ { "name": "...", "hex": "#..." } ],
      "secondary": [ { "name": "...", "hex": "#..." } ],
      "others":    [ { "name": "...", "hex": "#..." } ]
    }

    AVATARS
    -------
      • Aceitam .png/.jpg (lógica por cor virá depois; por ora livre)

    APPLICATIONS (COM NN_)
    ----------------------
      • .png/.jpg com prefixo: {NN}_nome.ext (ex.: 01_tile.png)

    GRAPHICS
    --------
      • .png/.jpg (numeração opcional)

    ICONS (COM NN_)
    ---------------
      • .png/.jpg com prefixo: {NN}_nome.ext (ex.: 01_download.png)

    FONTS (opcional)
    ----------------
      • .ttf, .otf, .woff, .woff2

    Observações:
      - Preserve os nomes (o backend não altera acentuação).
      - PNGs das logos: fundo transparente, recorte justo.
      - JPGs dos guidelines: alta qualidade (80%+).
    """)

def _logos_readme() -> str:
    return textwrap.dedent("""\
    /logos – Logos (PNG, SEM NN_) e Guidelines (JPG, COM NN_)
    ---------------------------------------------------------

    LOGOS (.png)
    Subpastas:
      primary/
      secondary_horizontal/
      secondary_vertical/

    Nome:
      {variant}.png
    Onde {variant} ∈ {primary, secondary, black, white}

    Exemplos em logos/primary/:
      primary.png
      secondary.png
      black.png
      white.png

    GUIDELINES (.jpg) – dentro de logos/guidelines/
      guidelines/primary/
      guidelines/secondary_horizontal/
      guidelines/secondary_vertical/

    Nome:
      {NN}_{logoType}-{corPrincipal}_{corSecundária}.jpg

    Exemplos:
      01_primary-primary_secondary.jpg
      02_secondary_horizontal-black_white.jpg
    """)

def _colors_readme() -> str:
    return textwrap.dedent("""\
    /colors – Paleta de cores
    -------------------------

    Coloque aqui:
      - Um arquivo obrigatório colors.json com a estrutura:
        {
          "primary":   [],
          "secondary": [],
          "others":    []
        }
      - Uma ou mais imagens .jpg COM prefixo de numeração (NN_):
        01_palette.jpg
        02_gradients.jpg
    """)

def _generic_readme(title: str, require_seq: bool) -> str:
    if require_seq:
        body = "Aceitos: .png e .jpg (COM NN_)\nEx.: 01_nome.ext, 02_nome.ext"
    else:
        body = "Aceitos: .png e .jpg (numeração opcional)"
    return textwrap.dedent(f"""\
    /{title}
    -------
    {body}
    """)

def _fonts_readme() -> str:
    return textwrap.dedent("""\
    /fonts – Arquivos de fonte (opcional)
    -------------------------------------

    Tipos aceitos:
      .ttf, .otf, .woff, .woff2

    Recomendações:
      - Inclua Regular, Bold, Italic se possível.
    """)

def build_ingestion_template_zip() -> BytesIO:
    buf = BytesIO()
    with ZipFile(buf, "w", compression=ZIP_DEFLATED) as z:
        # Diretórios
        folders = [
            "logos/",
            "logos/primary/",
            "logos/secondary_horizontal/",
            "logos/secondary_vertical/",
            "logos/guidelines/",
            "logos/guidelines/primary/",
            "logos/guidelines/secondary_horizontal/",
            "logos/guidelines/secondary_vertical/",
            "colors/",
            "avatars/",
            "applications/",
            "graphics/",
            "icons/",
            "fonts/",
        ]
        for f in folders:
            z.writestr(f, "")

        # READMEs
        z.writestr("README.txt", _root_readme())
        z.writestr("logos/README.txt", _logos_readme())
        z.writestr("colors/README.txt", _colors_readme())
        z.writestr("avatars/README.txt", _generic_readme("avatars", require_seq=False))
        z.writestr("applications/README.txt", _generic_readme("applications", require_seq=True))
        z.writestr("graphics/README.txt", _generic_readme("graphics", require_seq=False))
        z.writestr("icons/README.txt", _generic_readme("icons", require_seq=True))
        z.writestr("fonts/README.txt", _fonts_readme())

        # colors.json vazio
        empty_colors = {"primary": [], "secondary": [], "others": []}
        z.writestr("colors/colors.json", json.dumps(empty_colors, ensure_ascii=False, indent=2))

    buf.seek(0)
    return buf
