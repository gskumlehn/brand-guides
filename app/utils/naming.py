import os, re, unicodedata

def slugify_ascii(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9\-\_]+", "-", text).strip("-").lower()

    return text

def sanitize_filename(name: str) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = name.strip().replace(" ", "-")
    name = re.sub(r"[^a-zA-Z0-9\-\.]+", "", name)
    name = re.sub(r"-+", "-", name)

    return name.lower()

def with_seq_prefixes(names: list[str]) -> list[str]:
    cleaned = [sanitize_filename(n) for n in names]
    cleaned = sorted(cleaned)
    out = []
    for i, n in enumerate(cleaned, start=1):
        base, ext = os.path.splitext(n)
        seq = f"{i:02d}"
        out.append(f"{seq}_{base}{ext}")

    return out
