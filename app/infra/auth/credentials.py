import os
from google.oauth2 import service_account

_SCOPES = ["https://www.googleapis.com/auth/cloud-platform",
           "https://www.googleapis.com/auth/devstorage.read_write",
           "https://www.googleapis.com/auth/bigquery"]

def load_credentials(path: str | None = None):
    sa_path = path or os.getenv("SERVICE_ACCOUNT_PATH", "/secrets/service-acocunt.json")

    return service_account.Credentials.from_service_account_file(sa_path, scopes=_SCOPES)
