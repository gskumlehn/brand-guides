# app/infra/bucket/gcs_client.py
import os
from datetime import timedelta
from typing import Optional
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
        # URL pública *não usada* quando o bucket é privado; mantida para compat.
        return f"https://storage.googleapis.com/{bucket}/{path}"

    def signed_url(self, bucket: str, path: str, minutes: int = 15, as_attachment: bool = False) -> str:
        """URL assinada V4 para leitura."""
        bkt = self.client.bucket(bucket)
        blob = bkt.blob(path)
        response_disposition = None
        if as_attachment:
            filename = os.path.basename(path)
            response_disposition = f'attachment; filename="{filename}"'
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=minutes),
            method="GET",
            response_disposition=response_disposition,
        )
