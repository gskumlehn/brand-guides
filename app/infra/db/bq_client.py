import os
from google.cloud import bigquery
from ..auth.credentials import load_credentials, resolve_project_id

_DATASET = os.getenv("BQ_DATASET", "brand_guides")
_client = None

def client() -> bigquery.Client:
    global _client
    if _client is None:
        creds = load_credentials()
        project = resolve_project_id(creds)
        if not project:
            raise RuntimeError("GCP_PROJECT não resolvido. Defina GCP_PROJECT ou use SA com project_id.")
        _client = bigquery.Client(project=project, credentials=creds)
    return _client

def fq(table: str) -> str:
    c = client()
    return f"`{c.project}.{_DATASET}.{table}`"

def ensure_assets_table():
    c = client()
    c.query(f"CREATE SCHEMA IF NOT EXISTS `{c.project}.{_DATASET}`").result()
    c.query(f"""
    CREATE TABLE IF NOT EXISTS {fq('assets')} (
      brand_name    STRING NOT NULL,
      category      STRING NOT NULL,
      subcategory   STRING,
      sequence      INT64,
      original_name STRING,
      path          STRING,
      url           STRING,
      created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """).result()

def insert(table: str, rows: list[dict]):
    job = client().load_table_from_json(
        rows,
        f"{client().project}.{_DATASET}.{table}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
