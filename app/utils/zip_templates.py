from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
from .zip_utils import build_template_zip_bytes

def build_template_zip() -> bytes:
    """
    Mantido por compatibilidade com chamadas existentes.
    """
    return build_template_zip_bytes()

def build_template_zip_fileobj() -> BytesIO:
    return BytesIO(build_template_zip_bytes())
