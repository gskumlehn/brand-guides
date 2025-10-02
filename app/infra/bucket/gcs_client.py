import mimetypes
from google.cloud import storage
from ..auth.credentials import load_credentials, resolve_project_id

class GCSClient:
    def __init__(self):
        creds = load_credentials()
        self.client = storage.Client(project=resolve_project_id(creds), credentials=creds)

    def read_object(self, bucket_name: str, blob_path: str):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        data = blob.download_as_bytes()
        content_type = mimetypes.guess_type(blob_path)[0]
        filename = blob_path.split("/")[-1]
        return data, content_type, filename

    def write_object(self, bucket_name: str, blob_path: str, data: bytes, content_type: str | None = None) -> str:
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        if content_type:
            blob.upload_from_string(data, content_type=content_type)
        else:
            blob.upload_from_string(data)
        return f"gs://{bucket_name}/{blob_path}"
