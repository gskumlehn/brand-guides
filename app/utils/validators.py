# app/utils/validators.py
import re
from typing import Tuple, Optional

# Categoria: "01-logos" -> (1, "logos")
_CATEGORY_RE = re.compile(r"^(?P<seq>\d{2})-(?P<label>.+)$", re.UNICODE)

# Subcategoria aceita:
#  - "01-principal-01" -> (1, "principal", 1)
#  - "02-02"           -> (2, "", 2)      (NN-NN: sem título)
#  - "02--02"          -> (2, "", 2)      (NN--NN: sem título)
_SUB_RE_FULL = re.compile(r"^(?P<seq>\d{2})-(?P<label>.*?)-(?P<cols>\d{1,2})$", re.UNICODE)
_SUB_RE_EMPTY_1 = re.compile(r"^(?P<seq>\d{2})-(?P<cols>\d{1,2})$", re.UNICODE)   # NN-NN
_SUB_RE_EMPTY_2 = re.compile(r"^(?P<seq>\d{2})--(?P<cols>\d{1,2})$", re.UNICODE)  # NN--NN

def parse_category_dir(dirname: str) -> Tuple[int, str]:
    d = dirname.strip()
    m = _CATEGORY_RE.match(d)
    if m:
        return int(m.group("seq")), m.group("label").strip()
    return 0, d

def parse_subcategory_dir(dirname: str) -> Tuple[int, Optional[str], Optional[int]]:
    """
    Retorna: (seq, label_exibicao_ou_vazio, colunas)
    - label "" significa "sem título" (NN-NN / NN--NN)
    - label None significa que NÃO é um padrão reconhecido (ex.: apenas "01")
    """
    d = dirname.strip()
    m = _SUB_RE_FULL.match(d)
    if m:
        return int(m.group("seq")), m.group("label").strip(), int(m.group("cols"))
    m = _SUB_RE_EMPTY_1.match(d)
    if m:
        return int(m.group("seq")), "", int(m.group("cols"))
    m = _SUB_RE_EMPTY_2.match(d)
    if m:
        return int(m.group("seq")), "", int(m.group("cols"))
    if d.isdigit() and len(d) == 2:
        return int(d), None, None
    return 0, None, None

def file_prefix_sequence(filename: str) -> int:
    m = re.match(r"^(\d{2})", filename.strip())
    return int(m.group(1)) if m else 0
