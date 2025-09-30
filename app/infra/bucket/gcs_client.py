# app/infra/bucket/gcs_client.py
import os
from functools import lru_cache
from typing import Iterable, Optional

from google.cloud import storage

# Dependências de credencial/projeto centralizadas
from ..auth.credentials import load_credentials, resolve_project_id


# ---------- Singletons básicos (seguem existindo para retrocompatibilidade) ----------
@lru_cache(maxsize=1)
def _creds():
    return load_credentials()

@lru_cache(maxsize=1)
def _project():
    return resolve_project_id(_creds())

@lru_cache(maxsize=1)
def client() -> storage.Client:
    return storage.Client(project=_project(), credentials=_creds())

def bucket_name() -> str:
    name = os.getenv("GCS_BUCKET")
    if not name:
        raise RuntimeError("GCS_BUCKET não definido nas variáveis de ambiente.")
    return name

def bucket() -> storage.Bucket:
    return client().bucket(bucket_name())


# ---------- Classe esperada pelos serviços ----------
class GCSClient:
    """
    Wrapper simples sobre google-cloud-storage para upload, stream e listagem.
    Mantém comportamento privado (sem tornar público por padrão).
    """
    def __init__(self, project_id: Optional[str] = None, bucket_name_override: Optional[str] = None):
        self._creds = _creds()
        self._project = project_id or _project()
        self._client = client()
        self._bucket = self._client.bucket(bucket_name_override or bucket_name())

    # -------- Uploads --------
    def upload_bytes(
        self,
        path: str,
        data: bytes,
        content_type: Optional[str] = None,
        cache_control: Optional[str] = "public, max-age=31536000",
        make_public: bool = False,  # mantenha False para não expor objetos por padrão
    ) -> None:
        blob = self._bucket.blob(path)
        if cache_control:
            blob.cache_control = cache_control
        if content_type:
            blob.content_type = content_type
        blob.upload_from_string(data, content_type=content_type)
        if make_public:
            blob.make_public()

    def upload_fileobj(
        self,
        path: str,
        fileobj,
        content_type: Optional[str] = None,
        cache_control: Optional[str] = "public, max-age=31536000",
        make_public: bool = False,
    ) -> None:
        blob = self._bucket.blob(path)
        if cache_control:
            blob.cache_control = cache_control
        if content_type:
            blob.content_type = content_type
        blob.upload_from_file(fileobj, content_type=content_type, rewind=True)
        if make_public:
            blob.make_public()

    # -------- Downloads / Stream --------
    def download_as_bytes(self, path: str) -> bytes:
        return self._bucket.blob(path).download_as_bytes()

    def open_stream(self, path: str, chunk_size: int = 256 * 1024) -> Iterable[bytes]:
        """
        Gera chunks do arquivo (útil para Response(stream_with_context(...))).
        """
        blob = self._bucket.blob(path)
        # stream via file-like para melhor uso de memória
        with blob.open("rb", chunk_size=chunk_size) as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    # -------- Utilidades --------
    def exists(self, path: str) -> bool:
        return self._bucket.blob(path).exists()

    def list_prefix(self, prefix: str) -> list[str]:
        """Retorna uma lista de nomes (paths) sob o prefixo."""
        return [b.name for b in self._client.list_blobs(self._bucket, prefix=prefix)]

    def blob(self, path: str) -> storage.Blob:
        return self._bucket.blob(path)
