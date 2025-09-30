import zipfile
from typing import Iterable, Tuple, List, Dict

_NOISE = {"__macosx", ".ds_store"}

def _split(path: str) -> List[str]:
    return [p for p in path.replace("\\", "/").split("/") if p]

def _is_noise_first(part: str) -> bool:
    return part.lower() in _NOISE or part.startswith("._") or part.startswith(".")

def _detect_single_wrapper(zf: zipfile.ZipFile) -> str | None:
    """
    Se todos os arquivos (ignorando ruídos) compartilham o MESMO primeiro componente,
    retorna esse componente (wrapper). Caso contrário, None.
    """
    firsts = set()
    for info in zf.infolist():
        if info.is_dir():
            continue
        parts = _split(info.filename)
        if not parts:
            continue
        first = parts[0]
        if _is_noise_first(first):
            continue
        firsts.add(first)
        if len(firsts) > 1:
            return None
    return next(iter(firsts)) if firsts else None

def _strip_wrapper(path: str, wrapper: str | None) -> str:
    if not wrapper:
        return path
    parts = _split(path)
    if parts and parts[0].lower() == wrapper.lower():
        parts = parts[1:]
    return "/".join(parts)

def iter_all_files(zf: zipfile.ZipFile) -> Iterable[Tuple[str, bytes]]:
    """
    Itera por TODOS os arquivos do ZIP devolvendo caminhos relativos SEM o wrapper
    (quando existir apenas uma pasta-raiz), ignorando entradas de sistema.
    """
    wrapper = _detect_single_wrapper(zf)
    for info in zf.infolist():
        if info.is_dir():
            continue
        parts = _split(info.filename)
        if not parts:
            continue
        if _is_noise_first(parts[0]):
            continue
        rel = _strip_wrapper(info.filename, wrapper)
        if not rel:
            continue
        with zf.open(info) as f:
            yield rel, f.read()

def group_by_first_component(zf: zipfile.ZipFile) -> Dict[str, List[Tuple[str, bytes]]]:
    """
    Agrupa arquivos pelo primeiro componente (ex.: logos, graphics, ...), já
    com o wrapper removido quando presente.
    """
    out: Dict[str, List[Tuple[str, bytes]]] = {}
    for rel, data in iter_all_files(zf):
        parts = _split(rel)
        if not parts:
            continue
        first = parts[0].lower()
        tail = "/".join(parts[1:])
        out.setdefault(first, []).append((tail, data))
    return out
