# app/utils/naming.py
import re
import unicodedata

def _to_ascii(s: str) -> str:
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
    )

def slug(s: str) -> str:
    s = _to_ascii(s or "")
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s

def safe_str(s: str | None) -> str:
    return slug(s or "")
