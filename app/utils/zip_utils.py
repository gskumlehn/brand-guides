# app/utils/zip_utils.py
import io, json, zipfile
from typing import Dict, List, Optional

__all__ = [
    "build_template_zip_bytes",
    "default_spec",
    "empty_colors_json",
]

README_ROOT_MD = """# Brand Package – Estrutura de Ingestão (Genérica)

- Categorias: `NN-nome-da-categoria/`
- Subcategorias:
  - `NN-nome-da-subcategoria-NN` (com título; último NN = colunas `01..04`)
  - `NN-NN` ou `NN--NN` (sem título)
- Hífen em tudo (sem underscore).
- `originais/` é opcional em qualquer categoria; **obrigatório** em `03-tipografia/`.
- Em `02-cores/`: `colors.json` (obrigatório) + **opcionais** `principal.txt` e `secundaria.txt`.
- **Textos**: qualquer `.txt` colocado diretamente na pasta da **categoria** ou da **subcategoria** será ingerido (nome não importa, exceto em `02-cores`).
"""

README_CATEGORY_MD = """# Categoria

Coloque aqui **opcionalmente** arquivos `.txt` com instruções/descrições da categoria (nomes livres).
Se usar `originais/`, os arquivos serão disponibilizados para download no front.

Subpastas aceitas:
- `NN-nome-da-subcategoria-NN`
- `NN-NN` ou `NN--NN` (sem título)

Dentro de cada subpasta:
- `.txt` (nomes livres) → texto da subcategoria
- imagens/arquivos → exibidos conforme `columns` (NN final)
"""

def empty_colors_json() -> str:
    return json.dumps({
        "colors": [
            {
              "category": "main",
              "subcategory": "primary",
              "label": "Verde Escuro",
              "hex": "#003C2D",
              "RGB": "0,60,45",
              "CMYK": "93,5,75,70",
              "Pantone": "343 C",
              "sequence": 1
            }
        ]
    }, ensure_ascii=False, indent=2)

def _colors_json_full() -> str:
    return json.dumps({
        "colors": [
            {
              "category": "main",
              "subcategory": "primary",
              "label": "Verde Escuro",
              "hex": "#003C2D",
              "RGB": "0,60,45",
              "CMYK": "93,5,75,70",
              "Pantone": "343 C",
              "sequence": 1
            },
            {
              "category": "secondary",
              "label": "Azul",
              "hex": "#4F71F6",
              "RGB": "79,113,246",
              "CMYK": "79,55,0,4",
              "Pantone": "2727 C",
              "sequence": 2
            }
        ]
    }, ensure_ascii=False, indent=2)

def _w(z: zipfile.ZipFile, path: str, content: Optional[str] = "") -> None:
    if path.endswith("/"):
        z.writestr(path, "")
    else:
        z.writestr(path, content if content is not None else "")

def default_spec() -> List[Dict]:
    return [
        {
            "category_dir": "01-categoria-a",
            "with_texts": ["descricao-categoria.txt"],
            "with_originais": True,
            "originais_files": ["arquivo.ai", "manual.pdf"],
            "subs": [
                {"dir": "01-hero-01", "texts": ["notas.txt"], "files": ["01.png", "02.png"]},
                {"dir": "02-02", "texts": ["sub.txt"], "files": ["01.png", "02.png"]},
            ],
        },
        {
            "category_dir": "02-cores",
            "with_texts": [],
            "colors_json": True
        },
        {
            "category_dir": "03-tipografia",
            "with_texts": ["tipos.txt"],
            "with_originais": True,
            "originais_files": ["fontes.zip"],
            "subs": [
                {"dir": "01-titulos-01", "texts": ["sugestoes.txt"], "files": ["01.png"]},
                {"dir": "02-corpo-02", "texts": [], "files": ["01.png", "02.png"]},
            ],
        },
        {
            "category_dir": "04-categoria-b",
            "with_texts": ["desc.txt"],
            "with_originais": False,
            "subs": [
                {"dir": "01-conjuntos-03", "texts": ["bloco.txt"], "files": ["01.png", "02.png", "03.png"]}
            ],
        },
    ]

def _write_category(z: zipfile.ZipFile, root: str, spec: Dict) -> None:
    cat = spec["category_dir"].rstrip("/")
    cat_path = f"{root}{cat}/"
    _w(z, cat_path)
    _w(z, f"{cat_path}README.md", README_CATEGORY_MD)

    for t in spec.get("with_texts", []):
        _w(z, f"{cat_path}{t}", f"Texto livre da categoria {cat}.\n")

    # pasta especial 'cores'
    if cat.split("-", 1)[-1] == "cores":
        _w(z, f"{cat_path}colors.json", _colors_json_full())
        _w(z, f"{cat_path}principal.txt", "Texto para cores principais (main/primary).")
        _w(z, f"{cat_path}secundaria.txt", "Texto para cores secundárias (secondary).")
        return

    if spec.get("with_originais"):
        _w(z, f"{cat_path}originais/")
        for f in spec.get("originais_files", []):
            _w(z, f"{cat_path}originais/{f}", "")

    for sub in spec.get("subs", []):
        subdir = sub["dir"].rstrip("/")
        sub_path = f"{cat_path}{subdir}/"
        _w(z, sub_path)
        for t in sub.get("texts", []):
            _w(z, f"{sub_path}{t}", f"Texto livre da subcategoria {subdir}.\n")
        for f in sub.get("files", []):
            _w(z, f"{sub_path}{f}", "")

def build_template_zip_bytes(spec: Optional[List[Dict]] = None, root_dir: str = "brand-package/") -> bytes:
    spec = spec or default_spec()
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", compression=zipfile.ZIP_DEFLATED) as z:
        _w(z, root_dir)
        _w(z, f"{root_dir}README.md", README_ROOT_MD)
        for cat in spec:
            _write_category(z, root=root_dir, spec=cat)
    return bio.getvalue()
