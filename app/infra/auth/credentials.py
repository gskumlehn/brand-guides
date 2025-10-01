import os
from google.oauth2 import service_account
import google.auth

def load_credentials():
    # Prefer service-account (container mount) -> /secrets/service-account.json
    svc_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/service-account.json")
    if os.path.exists(svc_path):
        return service_account.Credentials.from_service_account_file(svc_path)
    # Fallback para ADC se estiver configurado (ex.: Cloud Run/Build)
    creds, _ = google.auth.default()
    return creds

def resolve_project_id(creds):
    return getattr(creds, "project_id", None) or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
