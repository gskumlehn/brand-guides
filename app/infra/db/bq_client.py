import logging, os
from typing import Any, Dict, List
from google.cloud import bigquery
from ..auth.credentials import load_credentials

log = logging.getLogger(__name__)
_bq=None
_dataset=None

def init_bq(dataset: str, project: str | None = None):
    global _bq,_dataset
    creds = load_credentials()
    _bq = bigquery.Client(project=project or os.getenv("BQ_PROJECT") or os.getenv("GCP_PROJECT"), credentials=creds)
    _dataset = dataset

def client() -> bigquery.Client:
    return _bq

def fq(table: str) -> str:
    return f"`{client().project}.{_dataset}.{table}`"

def ensure_schema():
    c = client()
    replace = os.getenv("BQ_FORCE_REPLACE_SCHEMA", "false").lower() == "true"
    c.query(f'CREATE SCHEMA IF NOT EXISTS `{c.project}.{_dataset}` OPTIONS(location="US")').result()
    ddl = f"""
    CREATE {"OR REPLACE " if replace else ""}TABLE `{c.project}.{_dataset}.assets` (
      brand_name    STRING NOT NULL,
      category      STRING NOT NULL,
      subcategory   STRING,
      sequence      INT64  NOT NULL,
      original_name STRING NOT NULL,
      path          STRING NOT NULL,
      url           STRING,
      created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    PARTITION BY DATE(created_at)
    CLUSTER BY brand_name, category
    """
    c.query(ddl).result()

def q(sql: str, params: Dict[str, Any] | None = None) -> List[dict]:
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter(k, _infer_type(v), v) for k,v in (params or {}).items()])
    rows = client().query(sql, job_config=job_config).result()

    return [dict(r.items()) for r in rows]

def insert(table: str, rows: list[dict]):
    table_ref = client().dataset(_dataset).table(table)
    job = client().load_table_from_json(
        rows,
        table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()  # garante conclusão do load

def _infer_type(v: Any) -> str:
    if isinstance(v, bool): return "BOOL"
    if isinstance(v, int):  return "INT64"
    return "STRING"
