CATEGORY_SYNONYMS = {
    "logos": ["logos", "logo"],
    "graphics": ["graphics", "grafics", "grafismos", "patterns", "pattern"],
    "avatars": ["avatars", "avatar", "profiles"],
    "icons": ["icons", "icones", "ícones"],
    "fonts": ["fonts", "fontes"],
    "colors": ["colors", "cores", "paleta", "palette"],
    "applications": ["applications", "apps", "mockups", "aplicacoes", "aplicações"],
}

FLAT_CATEGORIES = ["icons", "fonts", "colors", "applications"]

LOGO_SUBTYPE_SYNONYMS = {
    "primary": ["prioritaria", "principal", "master", "primary"],
    "secondary_vertical": ["secondary-vertical", "vertical", "vert", "secondary_vertical"],
    "secondary_horizontal": ["secondary-horizontal", "horizontal", "horiz", "secondary_horizontal"],
    "guidelines": ["reguas", "grid", "safe-area", "safearea", "guidelines"],
}

def resolve_canonical_category(first_component: str) -> str | None:
    s = (first_component or "").strip().lower()
    for canon, aliases in CATEGORY_SYNONYMS.items():
        if s in aliases:
            return canon
    return None
