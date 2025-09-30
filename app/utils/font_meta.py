import os
import re

# Mapeia extensões para 'format()' do @font-face
def ext_to_format(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".woff2":
        return "woff2"
    if ext == ".woff":
        return "woff"
    if ext in (".otf",):
        return "opentype"
    return "truetype"  # .ttf e fallback


# Tenta extrair família a partir do nome do arquivo, removendo prefixo NN_
def family_from_filename(filename: str) -> str:
    base = os.path.splitext(os.path.basename(filename))[0]
    base = re.sub(r"^\d{2}_", "", base)  # remove 01_, 02_, ...
    base = base.replace("_", " ").replace("-", " ")
    # limpa palavras comuns de peso/estilo para um nome mais limpo
    base = re.sub(r"\b(regular|italic|oblique|bold|medium|light|black|thin|extrabold|semibold|demibold|variablefont|wght)\b", "", base, flags=re.I)
    base = re.sub(r"\s+", " ", base).strip()
    return base or "BrandFont"


def weight_from_filename(filename: str) -> int:
    f = filename.lower()
    if "variable" in f and "wght" in f:
        return 400
    if "thin" in f:
        return 100
    if "extralight" in f or "ultralight" in f:
        return 200
    if "light" in f:
        return 300
    if "regular" in f or "book" in f or "normal" in f:
        return 400
    if "medium" in f:
        return 500
    if "semibold" in f or "demibold" in f:
        return 600
    if "bold" in f:
        return 700
    if "extrabold" in f or "ultrabold" in f or "heavy" in f:
        return 800
    if "black" in f:
        return 900
    return 400


def style_from_filename(filename: str) -> str:
    f = filename.lower()
    if "italic" in f:
        return "italic"
    if "oblique" in f:
        return "oblique"
    return "normal"
