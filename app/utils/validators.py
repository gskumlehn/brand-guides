# app/utils/validators.py
import re

# Pastas com NN-... (hífen). Ex.: "01-titulos-02", "02--03", "02-02"
CAT_RE = re.compile(r"^(?P<nn>\d{2})-(?P<label>.+)$")
SUB_RE = re.compile(r"^(?P<nn>\d{2})(?:-(?P<label>.*?))?(?:-(?P<cols>\d{2}))?$")

def parse_category_dir(basename: str) -> tuple[int, str]:
    m = CAT_RE.match(basename)
    if not m:
        return (0, basename)
    return (int(m.group("nn")), (m.group("label") or "").strip())

def parse_subcategory_dir(basename: str) -> tuple[int, str | None, int | None]:
    """
    Suporta:
      NN-nome-NN  -> label e cols
      NN--NN      -> sem label e cols
      NN-NN       -> sem label e cols (título vazio)
    """
    m = SUB_RE.match(basename)
    if not m:
        return (0, basename, None)
    seq = int(m.group("nn"))
    label = (m.group("label") or "").strip() or None
    cols = int(m.group("cols")) if m.group("cols") else None
    return (seq, label, cols)

# Sequência de arquivo: aceita "01.png", "1.jpg", "001.svg" ou "01-arq.png"
FILE_SEQ_RE = re.compile(r"^(\d{1,3})(?:[-_.\s].*)?$")
def file_prefix_sequence(filename: str) -> int:
    base = filename.rsplit("/", 1)[-1]
    base = base.rsplit(".", 1)[0]
    m = FILE_SEQ_RE.match(base)
    return int(m.group(1)) if m else 0

def enforce_even_when_cols_2(n_files: int, cols: int | None, ctx: str = "") -> None:
    if cols == 2 and (n_files % 2 != 0):
        raise ValueError(f"Quantidade de arquivos deve ser PAR quando cols=2. {ctx} (len={n_files})")
