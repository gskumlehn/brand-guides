import os
import re
from typing import Optional, Tuple

# Reconhece prefixo NN_ no início do arquivo (ex.: 01_nome.jpg)
_NN_RE = re.compile(r'^(?P<nn>\d{2})_')

def safe_str(value: Optional[str]) -> str:
    """
    Retorna string "segura" apenas removendo espaços nas pontas.
    Não normaliza, não troca acentos, não altera codificações.
    """
    return (value or "").strip()

def split_stem_ext(filename: str) -> Tuple[str, str]:
    """
    Separa nome e extensão. Extensão é retornada em minúsculas sem o ponto.
    Ex.: 'Foto.PNG' -> ('Foto', 'png')
    """
    stem, ext = os.path.splitext(filename)
    return stem, ext.lower().lstrip('.')

def extract_nn_prefix(filename: str) -> Optional[int]:
    """
    Extrai o prefixo NN_ do início do arquivo.
    Ex.: '01_cartaz.jpg' -> 1 ; 'cartaz.jpg' -> None
    """
    m = _NN_RE.match(filename)
    if not m:
        return None
    try:
        return int(m.group('nn'))
    except Exception:
        return None
