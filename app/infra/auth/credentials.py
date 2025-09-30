import os
import google.auth
from google.oauth2 import service_account

_DEF = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "/secrets/service-account.json"

def load_credentials():
    if _DEF and os.path.isfile(_DEF):
        return service_account.Credentials.from_service_account_file(_DEF)
    creds, _ = google.auth.default()
    return creds

def resolve_project_id(creds):
    return (
        os.getenv("GCP_PROJECT")
        or getattr(creds, "project_id", None)
        or getattr(creds, "_project_id", None)
    )
