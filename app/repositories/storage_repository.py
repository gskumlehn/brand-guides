# app/repositories/storage_repository.py
from typing import Dict, Iterable, Optional
from google.cloud import storage
from ..infra.auth.credentials import load_credentials, resolve_project_id
from ..utils.naming import safe_str
import os

_BUCKET = os.getenv("GCS_BUCKET", "your-bucket")

def _client() -> storage.Client:
    creds = load_credentials()
    return storage.Client(project=resolve_project_id(creds), credentials=creds)

def _public_url(path: str) -> str:
    return f"https://storage.googleapis.com/{_BUCKET}/{path}"

def build_prefix(brand: str, category_key: Optional[str] = None, subcategory_key: Optional[str] = None) -> str:
    parts = [safe_str(brand).lower()]
    if category_key:
        parts.append(safe_str(category_key).lower())
    if subcategory_key:
        parts.append(safe_str(subcategory_key).lower())
    prefix = "/".join(parts)
    return prefix + "/" if prefix and not prefix.endswith("/") else prefix

def stream(prefix: str) -> Iterable[Dict[str, str]]:
    for blob in _client().list_blobs(_BUCKET, prefix=prefix):
        if blob.name.endswith("/"):
            continue
        yield {
            "path": blob.name,
            "url": _public_url(blob.name),
            "size": blob.size,
            "updated": blob.updated.isoformat() if blob.updated else None,
            "content_type": blob.content_type,
        }
