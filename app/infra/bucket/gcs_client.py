import os
from datetime import timedelta
from google.cloud import storage
from ..auth.credentials import load_credentials

class GCSClient:
    def __init__(self):
        self._creds = load_credentials()
        self._project = os.getenv("GCP_PROJECT")
        self._bucket_name = os.getenv("GCS_BUCKET", "").strip()
        self._expiry = int(os.getenv("GCS_SIGNED_URL_EXPIRY_SECONDS", "3600"))
        self._client = None
        self._bucket = None

    def _client_ok(self):
        if self._client is None:
            self._client = storage.Client(project=self._project, credentials=self._creds)
        return self._client

    def _bucket_ok(self):
        if self._bucket is None:
            c = self._client_ok()
            b = c.lookup_bucket(self._bucket_name)
            if not b:
                raise RuntimeError(f"Bucket '{self._bucket_name}' não encontrado ou sem acesso.")
            self._bucket = b
        return self._bucket

    def upload_bytes(self, path: str, data: bytes, content_type: str | None):
        blob = self._bucket_ok().blob(path)
        blob.cache_control = "public, max-age=31536000"
        blob.upload_from_string(data, content_type=content_type)
        return f"gs://{self._bucket_name}/{path}"

    def signed_get_url(self, path: str, expires_s: int | None = None) -> str:
        blob = self._bucket_ok().blob(path)
        return blob.generate_signed_url(
            expiration=timedelta(seconds=expires_s or self._expiry),
            method="GET",
            version="v4",
        )

    @property
    def bucket(self):
        return self._bucket_ok()
