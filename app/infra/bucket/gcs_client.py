# app/infra/bucket/gcs_client.py
import os
from typing import Optional
from google.cloud import storage
from ..auth.credentials import load_credentials, resolve_project_id

class GCSClient:
    def __init__(self) -> None:
        creds = load_credentials()
        self.client = storage.Client(project=resolve_project_id(creds), credentials=creds)

    def bucket(self, bucket_name: Optional[str] = None):
        name = bucket_name or os.getenv("GCS_BUCKET", "")
        return self.client.bucket(name)

    def write_object(self, bucket_name: str, path: str, content: bytes, content_type: str) -> str:
        b = self.bucket(bucket_name)
        blob = b.blob(path)
        blob.upload_from_string(content, content_type=content_type)
        blob.cache_control = "public, max-age=31536000"
        blob.patch()
        return f"https://storage.googleapis.com/{bucket_name}/{path}"
