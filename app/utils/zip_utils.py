import io
import json
import zipfile
from typing import Dict


def _w(z: zipfile.ZipFile, path: str, content: str = "") -> None:
    """Escreve arquivo (ou diretório virtual) no zip."""
    if path.endswith("/"):
        z.writestr(path, "")
    else:
        z.writestr(path, content)


def get_empty_colors_data() -> Dict:
    """Retorna o dicionário base vazio para colors.json."""
    return {
        "brand_name": "",
        "colors": {
            "primary":   {"name": "", "hex": ""},
            "secondary": {"name": "", "hex": ""},
            "others": []
        }
    }


def empty_colors_json() -> str:
    """Retorna o JSON (string) formatado do colors.json vazio."""
    return json.dumps(get_empty_colors_data(), ensure_ascii=False, indent=2)


def _readme_root() -> str:
    return """Brand Guides — Estrutura de Ingestão

Preencha as pastas conforme as instruções dos READMEs específicos.
Após organizar os arquivos, compacte tudo como .zip e envie na tela de Ingestão.

Resumo:
- logos/
  - primary/ (PNG, nomes: primary.png | secondary.png | black.png | white.png)
  - secondary_horizontal/ (PNG, mesmos nomes)
  - secondary_vertical/ (PNG, mesmos nomes)
  - guidelines/ (JPG, lista simples por sequência NN_: 01_.jpg, 02_.jpg, ...)
- colors/
  - colors.json (estrutura de cores)
  - {NN}_*.jpg (amostras, obrigatório NN_)
- avatars/
  - round/  (PNG: primary.png | secondary.png)
  - square/ (PNG: primary.png | secondary.png)
  - app/    (PNG: primary.png | secondary.png)
- applications/ (arquivos com NN_, ex: 01_mock.jpg)
- graphics/ (opcional)
- icons/ (arquivos com NN_, ex: 01_download.png)
- fonts/ (opcional)
"""


def _readme_logos() -> str:
    return """LOGOS

As logos devem ser PNG (fundo transparente).

Subpastas obrigatórias:
- logos/primary/
- logos/secondary_horizontal/
- logos/secondary_vertical/

Em cada uma dessas pastas, use exatamente estes nomes de arquivo:
- primary.png      → logo com a cor principal
- secondary.png    → logo com a cor secundária
- black.png        → logo em preto
- white.png        → logo em branco

GUIDELINES (JPG)
- A pasta logos/guidelines/ é uma lista simples baseada em sequência.
- Nomemclatura: NN_.jpg  (por exemplo: 01_.jpg, 02_.jpg, 03_.jpg)
- O nome após o NN_ não é utilizado; apenas a ordem NN_ importa.
"""


def _readme_colors() -> str:
    return """COLORS

Dentro de colors/:
- colors.json  → arquivo JSON com as cores
- {NN}_*.jpg   → amostras de cor (obrigatório prefixo NN_)

Estrutura esperada de colors.json:

{
  "brand_name": "SUA-MARCA",
  "colors": {
    "primary":   {"name": "Nome da cor principal",   "hex": "#RRGGBB"},
    "secondary": {"name": "Nome da cor secundária",  "hex": "#RRGGBB"},
    "others": [
      {"name": "Cor 1", "hex": "#RRGGBB"},
      {"name": "Cor 2", "hex": "#RRGGBB"}
    ]
  }
}

Observações:
- Não inclua campo priority, source_path ou source_file.
- Hex deve estar no formato #RRGGBB.
"""


def _readme_avatars() -> str:
    return """AVATARS

Subpastas:
- avatars/round/
- avatars/square/
- avatars/app/

Arquivos aceitos: PNG
Nomes dos arquivos (por cor):
- primary.png
- secondary.png

A lógica para outras cores pode ser adicionada futuramente.
"""


def _readme_applications() -> str:
    return """APPLICATIONS

Arquivos com numeração NN_ obrigatória.
Exemplos:
- 01_tile.png
- 02_mock.jpg
"""


def _readme_icons() -> str:
    return """ICONS

Arquivos com numeração NN_ obrigatória.
Exemplos:
- 01_download.png
- 02_upload.png
"""


def _readme_graphics() -> str:
    return """GRAPHICS

Arquivos .png/.jpg (numeração opcional).
"""


def build_template_zip_bytes() -> bytes:
    """
    Gera um ZIP em memória com a estrutura de pastas e READMEs,
    além de um colors.json vazio de exemplo.
    """
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # Diretórios (com barra no fim para criar a entrada)
        dirs = [
            "logos/",
            "logos/primary/",
            "logos/secondary_horizontal/",
            "logos/secondary_vertical/",
            "logos/guidelines/",
            "colors/",
            "avatars/",
            "avatars/round/",
            "avatars/square/",
            "avatars/app/",
            "applications/",
            "graphics/",
            "icons/",
            "fonts/",
        ]
        for d in dirs:
            _w(z, d, "")

        # READMEs
        _w(z, "README.txt", _readme_root())
        _w(z, "logos/README.txt", _readme_logos())
        _w(z, "colors/README.txt", _readme_colors())
        _w(z, "avatars/README.txt", _readme_avatars())
        _w(z, "applications/README.txt", _readme_applications())
        _w(z, "icons/README.txt", _readme_icons())
        _w(z, "graphics/README.txt", _readme_graphics())

        # colors.json vazio
        _w(z, "colors/colors.json", empty_colors_json())

    return mem.getvalue()
