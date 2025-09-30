import re

_SEQ = re.compile(r"^(\d{2})_.+")

_SKIP_EXACT = {"readme.txt", "readme.md", ".ds_store", "ds_store", ".dsstore"}

def is_sequenced_asset(filename: str) -> bool:
    """
    Aceita somente arquivos com prefixo NN_ (ex.: 01_logo.svg).
    Também ignora artefatos como .DS_Store e forks '._arquivo'.
    """
    base = filename.split("/")[-1]
    if not base:
        return False
    if base.startswith("._"):
        return False
    if base.lower() in _SKIP_EXACT:
        return False
    return bool(_SEQ.match(base))

def extract_sequence(filename: str) -> int | None:
    base = filename.split("/")[-1]
    m = _SEQ.match(base)
    return int(m.group(1)) if m else None
