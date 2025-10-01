import os
from typing import Any, Dict, List, Optional
from google.cloud import bigquery
from ..auth.credentials import load_credentials, resolve_project_id

_DATASET = os.getenv("BQ_DATASET", "brand_guides")
_bq_client: Optional[bigquery.Client] = None

def client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        creds = load_credentials()
        _bq_client = bigquery.Client(
            project=resolve_project_id(creds),
            credentials=creds
        )
    return _bq_client

def fq(table: str) -> str:
    return f"`{client().project}.{_DATASET}.{table}`"

def ensure_assets_table() -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS `{client().project}.{_DATASET}`;
    CREATE TABLE IF NOT EXISTS {fq('assets')} (
      brand_name    STRING,
      category      STRING,
      subcategory   STRING,
      sequence      INT64,
      original_name STRING,
      path          STRING,
      url           STRING,
      created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """
    client().query(ddl).result()

def ensure_colors_table() -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS `{client().project}.{_DATASET}`;
    CREATE TABLE IF NOT EXISTS {fq('colors')} (
      brand_name  STRING,
      color_name  STRING,
      hex         STRING,
      role        STRING,
      created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """
    client().query(ddl).result()

def ensure_all_tables() -> None:
    ensure_assets_table()
    ensure_colors_table()

def q(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    job_config = None
    if params:
        qp = []
        for k, v in params.items():
            typ = "INT64" if isinstance(v, int) else "STRING"
            qp.append(bigquery.ScalarQueryParameter(k, typ, v))
        job_config = bigquery.QueryJobConfig(query_parameters=qp)
    rows = client().query(sql, job_config=job_config).result()
    return [dict(r) for r in rows]

def load_json(table: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    job = client().load_table_from_json(
        rows,
        f"{client().project}.{_DATASET}.{table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True
        ),
    )
    job.result()

def ensure_assets_tables() -> None:
    ensure_all_tables()
