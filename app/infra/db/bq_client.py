# app/infra/db/bq_client.py
import os
from functools import lru_cache
from typing import Any, Iterable, List, Mapping, Optional

from google.cloud import bigquery
from ..auth.credentials import load_credentials, resolve_project_id

_DATASET = os.getenv("BQ_DATASET", "brand_guides")

ASSETS_SCHEMA = [
    bigquery.SchemaField("brand_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("subcategory", "STRING"),
    bigquery.SchemaField("sequence", "INTEGER"),
    bigquery.SchemaField("original_name", "STRING"),
    bigquery.SchemaField("path", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("file_url", "STRING"),
    bigquery.SchemaField("stream_url", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"),
]

@lru_cache(maxsize=1)
def _creds():
    return load_credentials()

@lru_cache(maxsize=1)
def _project():
    return resolve_project_id(_creds())

@lru_cache(maxsize=1)
def client() -> bigquery.Client:
    return bigquery.Client(project=_project(), credentials=_creds())

def fq(table: str) -> str:
    return f"`{_project()}.{_DATASET}.{table}`"

def ensure_dataset():
    c = client()
    ds_ref = bigquery.Dataset(f"{_project()}.{_DATASET}")
    try:
        c.get_dataset(ds_ref)
    except Exception:
        c.create_dataset(ds_ref, exists_ok=True)

def ensure_assets_table():
    ensure_dataset()
    c = client()
    table_id = f"{_project()}.{_DATASET}.assets"
    try:
        c.get_table(table_id)
    except Exception:
        c.create_table(bigquery.Table(table_id, schema=ASSETS_SCHEMA), exists_ok=True)

def q(sql: str, params: Optional[Mapping[str, Any]] = None) -> List[Mapping[str, Any]]:
    job_config = None
    if params:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(k, _infer_bq_type(v), v) for k, v in params.items()]
        )
    rows = client().query(sql, job_config=job_config).result()
    return [dict(r.items()) for r in rows]

# --------- NOVO: INSERT sem streaming -> LOAD JOB ----------
def insert(table: str, rows: Iterable[Mapping[str, Any]]):
    """
    Faz LOAD JOB de JSON (não usa streaming).
    """
    ensure_dataset()
    c = client()
    table_id = f"{_project()}.{_DATASET}.{table}"

    # Se a tabela não existir, cria com schema (apenas para 'assets' aqui)
    try:
        c.get_table(table_id)
    except Exception:
        schema = ASSETS_SCHEMA if table == "assets" else None
        c.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        schema=None,  # usa o schema existente
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        ignore_unknown_values=True,
    )

    load_job = c.load_table_from_json(list(rows), table_id, job_config=job_config)  # <-- LOAD JOB
    load_job.result()  # espera finalizar
    if load_job.errors:
        raise RuntimeError(load_job.errors)
# -----------------------------------------------------------

def truncate(table: str):
    client().query(f"TRUNCATE TABLE {fq(table)}").result()

def merge_assets_from_stage(stage_table: str):
    """
    Faz MERGE de stage -> assets pelo par (brand_name, category, subcategory, path).
    """
    sql = f"""
    MERGE {fq('assets')} T
    USING {fq(stage_table)} S
    ON  T.brand_name  = S.brand_name
    AND T.category    = S.category
    AND IFNULL(T.subcategory,'') = IFNULL(S.subcategory,'')
    AND T.path       = S.path
    WHEN MATCHED THEN UPDATE SET
      sequence     = S.sequence,
      original_name= S.original_name,
      url          = S.url,
      file_url     = S.file_url,
      stream_url   = S.stream_url
    WHEN NOT MATCHED THEN INSERT
      (brand_name, category, subcategory, sequence, original_name, path, url, file_url, stream_url)
    VALUES
      (S.brand_name, S.category, S.subcategory, S.sequence, S.original_name, S.path, S.url, S.file_url, S.stream_url)
    """
    client().query(sql).result()

def _infer_bq_type(v: Any) -> str:
    if isinstance(v, bool): return "BOOL"
    if isinstance(v, int): return "INT64"
    if isinstance(v, float): return "FLOAT64"
    return "STRING"
