# app/utils/naming.py
from __future__ import annotations
import os
import re
from typing import Optional, Tuple

PNG_EXT = {".png"}
JPG_EXT = {".jpg", ".jpeg"}

LOGO_VARIANTS = {"primary", "secondary", "black", "white"}
AVATAR_VARIANTS = {"primary", "secondary"}

GUIDELINE_RE = re.compile(
    r"^(?P<nn>\d{2})_"
    r"(?P<type>primary|secondary_horizontal|secondary_vertical)-"
    r"(?P<main>primary|secondary|black|white)_"
    r"(?P<secondary>primary|secondary|black|white)$",
    re.IGNORECASE,
)

def split_ext(name: str) -> Tuple[str, str]:
    base, ext = os.path.splitext(name)
    return base, ext.lower()

def is_png(name: str) -> bool:
    return split_ext(name)[1] in PNG_EXT

def is_jpg(name: str) -> bool:
    return split_ext(name)[1] in JPG_EXT

def parse_sequence(name: str) -> Optional[int]:
    """
    Extrai NN_ do início do nome (ex: 01_banner.jpg -> 1).
    Retorna None se não houver NN_.
    """
    m = re.match(r"^(\d{2})_", name)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None

# ---- LOGOS ----
def parse_logo_color_variant(filename: str) -> Optional[str]:
    base, _ = split_ext(filename)
    key = base.lower()
    return key if key in LOGO_VARIANTS else None

def parse_logo_guideline(filename: str) -> Optional[dict]:
    base, ext = split_ext(filename)
    if ext not in JPG_EXT:
        return None
    m = GUIDELINE_RE.match(base)
    if not m:
        return None
    return {
        "sequence": int(m.group("nn")),
        "logo_type": m.group("type").lower(),
        "main_color": m.group("main").lower(),
        "secondary_color": m.group("secondary").lower(),
    }

# ---- AVATARS ----
def parse_avatar_variant(filename: str) -> Optional[str]:
    base, ext = split_ext(filename)
    if ext not in PNG_EXT:
        return None
    key = base.lower()
    return key if key in AVATAR_VARIANTS else None
