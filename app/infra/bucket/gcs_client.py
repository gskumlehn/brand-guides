# app/infra/bucket/gcs_client.py
import os
from datetime import timedelta
from typing import List, Optional
from google.cloud import storage


class GCSClient:
    def __init__(self):
        self._client: Optional[storage.Client] = None

    @property
    def client(self) -> storage.Client:
        if self._client is None:
            self._client = storage.Client()
        return self._client

    def write_object(self, bucket: str, path: str, data: bytes, content_type: str) -> str:
        bkt = self.client.bucket(bucket)
        blob = bkt.blob(path)
        blob.upload_from_string(data, content_type=content_type)
        # URL pública só para referência; quando bucket é privado use signed_url()
        return f"https://storage.googleapis.com/{bucket}/{path}"

    def signed_url(self, bucket: str, path: str, minutes: int = 15) -> str:
        bkt = self.client.bucket(bucket)
        blob = bkt.blob(path)
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=minutes),
            method="GET",
        )

    def list_paths(self, bucket: str, prefix: str) -> List[str]:
        """Lista nomes (paths) de objetos sob um prefixo."""
        bkt = self.client.bucket(bucket)
        blobs = self.client.list_blobs(bkt, prefix=prefix)
        return [b.name for b in blobs if not b.name.endswith("/")]

    def read_bytes(self, bucket: str, path: str) -> bytes:
        bkt = self.client.bucket(bucket)
        blob = bkt.blob(path)
        return blob.download_as_bytes()
