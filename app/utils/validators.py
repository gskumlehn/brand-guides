import re
def is_png(name: str) -> bool: return name.lower().endswith(".png")
def is_jpg(name: str) -> bool: return name.lower().endswith(".jpg") or name.lower().endswith(".jpeg")

NN_RE = re.compile(r"^(\d{2})_")
def has_nn_prefix(name: str) -> bool: return NN_RE.match(name) is not None
def parse_nn(name: str) -> int:
    m = NN_RE.match(name)
    return int(m.group(1)) if m else 0
