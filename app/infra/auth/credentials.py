# app/infra/auth/credentials.py
import os
from typing import Optional, Tuple

import google.auth
from google.oauth2 import service_account

# Exige apenas conta de serviço (ADC via arquivo)
def load_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_path and os.path.exists(sa_path):
        return service_account.Credentials.from_service_account_file(sa_path)
    # fallback: ADC do ambiente (Cloud Run, etc.)
    creds, _ = google.auth.default()
    return creds

def resolve_project_id(creds=None) -> str:
    if not creds:
        creds = load_credentials()
    # 1) var explícita
    env_project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID")
    if env_project:
        return env_project
    # 2) do JSON de serviço
    if hasattr(creds, "project_id") and creds.project_id:
        return creds.project_id
    # 3) Cloud Run/ADC
    _, project_id = google.auth.default()
    if project_id:
        return project_id
    raise RuntimeError("Não foi possível resolver PROJECT_ID. Defina GCP_PROJECT/GOOGLE_CLOUD_PROJECT/PROJECT_ID.")
