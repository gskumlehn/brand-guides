import os
import re
import mimetypes
from typing import Dict, Optional

SAFE_COLORS = {"primary", "secondary", "black", "white"}
LOGO_VARIANTS = {"primary", "secondary_horizontal", "secondary_vertical"}

_mime_overrides = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".pdf": "application/pdf",
    ".ttf": "font/ttf",
    ".otf": "font/otf",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
}


def guess_mime_type(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext in _mime_overrides:
        return _mime_overrides[ext]
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"


def file_ext(path: str) -> str:
    return os.path.splitext(path)[1].lower().lstrip(".")


_seq_re = re.compile(r"^(\d+)_")

def try_sequence_from_name(name: str) -> Optional[int]:
    m = _seq_re.match(name)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def detect_logo_color_from_filename(name_lower: str) -> Optional[str]:
    """
    Para PNG de logos: extrai a cor única da logo pelo nome:
    'primary' | 'secondary' | 'black' | 'white'
    Regra: pegar a primeira ocorrência nesta ordem de preferência.
    """
    for token in ("primary", "secondary", "black", "white"):
        if re.search(rf"(?<![a-z0-9]){token}(?![a-z0-9])", name_lower):
            return token
    return None


_guideline_re = re.compile(
    r"^(?P<variant>primary|secondary_horizontal|secondary_vertical)"
    r"-(?P<c1>primary|secondary|black|white)"
    r"_(?P<c2>primary|secondary|black|white)$"
)

def parse_guideline_from_basename(basename_no_ext: str) -> Optional[Dict[str, str]]:
    """
    Para JPG de guidelines: padrão:
      VARIANTE-CORPRINCIPAL_CORSECUNDARIA
    Ex.: 'primary-primary_secondary'
         'secondary_horizontal-black_white'
    """
    m = _guideline_re.match(basename_no_ext.lower())
    if not m:
        return None
    variant = m.group("variant")
    c1 = m.group("c1")
    c2 = m.group("c2")
    return {"logo_variant": variant, "color_primary": c1, "color_secondary": c2}


def derive_category_and_subcategory(path: str) -> Dict[str, Optional[str]]:
    """
    A partir do path 'brand/category/subcategory/...' tenta inferir category e subcategory.
    Ex.: 'ccba2/logos/primary/arquivo.png' => category=logos, subcategory=primary
         'ccba2/logos/guidelines/arquivo.jpg' => subcategory=guidelines
    """
    parts = path.split("/")
    # Esperado: brand / category / [subcategory] / file
    category = parts[1] if len(parts) > 2 else None
    subcategory = parts[2] if len(parts) > 3 else None
    return {"category": category, "subcategory": subcategory}


def parse_logo_meta(path: str) -> Dict[str, Optional[str]]:
    """
    Extrai metadados de logo/guideline conforme regras do usuário.
    - PNG (logos): logo_color por nome do arquivo; logo_variant pelo subfolder (primary/secondary_horizontal/secondary_vertical)
    - JPG (guidelines): 'variant-primary_secondary' no basename (sem extensão)
    """
    base = os.path.basename(path)
    name_no_ext, ext = os.path.splitext(base)
    ext = ext.lower()
    meta: Dict[str, Optional[str]] = {
        "logo_variant": None,
        "logo_color": None,
        "color_primary": None,
        "color_secondary": None,
    }

    cat_sub = derive_category_and_subcategory(path)
    sub = (cat_sub.get("subcategory") or "").lower()

    if ext == ".png" and (cat_sub.get("category") == "logos") and (sub in LOGO_VARIANTS):
        # PNG de logo
        meta["logo_variant"] = sub
        color = detect_logo_color_from_filename(base.lower())
        if color in SAFE_COLORS:
            meta["logo_color"] = color
        return meta

    if ext in (".jpg", ".jpeg") and (cat_sub.get("category") == "logos") and (sub == "guidelines"):
        # JPG guideline
        g = parse_guideline_from_basename(name_no_ext)
        if g:
            meta.update(g)
        return meta

    # Outros arquivos / categorias: sem metadados de logo
    return meta

_SEQ_RE = re.compile(r"^(\d{2})[_-]")

def parse_sequence(filename: str) -> Optional[int]:
    """
    Extrai o prefixo de sequência NN_ (ex.: 01_, 02-) se existir.
    """
    m = _SEQ_RE.match(filename)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None