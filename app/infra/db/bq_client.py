import os
from functools import lru_cache
from typing import Any, Iterable, List, Mapping, Optional

from google.cloud import bigquery
from ..auth.credentials import load_credentials, resolve_project_id

_DATASET = os.getenv("BQ_DATASET", "brand_guides")

# Schema “esperado” da tabela assets
ASSETS_FIELDS = [
    ("brand_name",   "STRING",   "REQUIRED"),
    ("category",     "STRING",   "REQUIRED"),
    ("subcategory",  "STRING",   "NULLABLE"),
    ("sequence",     "INT64",    "NULLABLE"),
    ("original_name","STRING",   "NULLABLE"),
    ("path",         "STRING",   "NULLABLE"),
    ("url",          "STRING",   "NULLABLE"),
    ("file_url",     "STRING",   "NULLABLE"),
    ("stream_url",   "STRING",   "NULLABLE"),
    ("created_at",   "TIMESTAMP","NULLABLE"),  # preenchido por app/ingestão
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
    ds_id = f"{_project()}.{_DATASET}"
    try:
        c.get_dataset(ds_id)
    except Exception:
        c.create_dataset(bigquery.Dataset(ds_id), exists_ok=True)

def _create_table_if_not_exists(table: str, fields: list[tuple[str,str,str]]):
    c = client()
    table_id = f"{_project()}.{_DATASET}.{table}"
    try:
        c.get_table(table_id)
        created = False
    except Exception:
        schema = [bigquery.SchemaField(n, t, mode=m) for (n,t,m) in fields]
        c.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)
        created = True

    # Migrações idempotentes de colunas (ADD COLUMN IF NOT EXISTS)
    # Garante que rodamos mesmo se a tabela já existia com schema antigo.
    alter_fragments = []
    for (name, typ, _mode) in fields:
        if name == "created_at":
            # TIP: se quiser default no BQ, use uma view; aqui garantimos só a coluna.
            alter_fragments.append(f"ADD COLUMN IF NOT EXISTS {name} {typ}")
        else:
            alter_fragments.append(f"ADD COLUMN IF NOT EXISTS {name} {typ}")
    sql = f"ALTER TABLE {fq(table)} " + ", ".join(alter_fragments)
    # Rodamos o ALTER mesmo que nada precise ser adicionado (é idempotente)
    client().query(sql).result()
    return created

def ensure_assets_tables():
    ensure_dataset()
    _create_table_if_not_exists("assets", ASSETS_FIELDS)
    _create_table_if_not_exists("assets_stage", ASSETS_FIELDS)

def q(sql: str, params: Optional[Mapping[str, Any]] = None) -> List[Mapping[str, Any]]:
    job_config = None
    if params:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(k, _infer_bq_type(v), v) for k, v in params.items()]
        )
    rows = client().query(sql, job_config=job_config).result()
    return [dict(r.items()) for r in rows]

# ----------------- LOAD JOB (sem streaming) -----------------
def insert(table: str, rows: Iterable[Mapping[str, Any]]):
    """
    Faz LOAD JOB de JSON para {dataset}.{table}.
    """
    ensure_dataset()
    c = client()
    table_id = f"{_project()}.{_DATASET}.{table}"

    # garante existência (e migra)
    if table in ("assets", "assets_stage"):
        _create_table_if_not_exists(table, ASSETS_FIELDS)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        ignore_unknown_values=True,
    )
    load_job = c.load_table_from_json(list(rows), table_id, job_config=job_config)
    load_job.result()
    if load_job.errors:
        raise RuntimeError(load_job.errors)
# ------------------------------------------------------------

def truncate(table: str):
    client().query(f"TRUNCATE TABLE {fq(table)}").result()

def merge_assets_from_stage(stage_table: str):
    sql = f"""
    MERGE {fq('assets')} T
    USING {fq(stage_table)} S
    ON  T.brand_name  = S.brand_name
    AND T.category    = S.category
    AND IFNULL(T.subcategory,'') = IFNULL(S.subcategory,'')
    AND T.path        = S.path
    WHEN MATCHED THEN UPDATE SET
      sequence      = S.sequence,
      original_name = S.original_name,
      url           = S.url,
      file_url      = S.file_url,
      stream_url    = S.stream_url,
      created_at    = S.created_at
    WHEN NOT MATCHED THEN INSERT
      (brand_name, category, subcategory, sequence, original_name, path, url, file_url, stream_url, created_at)
    VALUES
      (S.brand_name, S.category, S.subcategory, S.sequence, S.original_name, S.path, S.url, S.file_url, S.stream_url, S.created_at)
    """
    client().query(sql).result()

def _infer_bq_type(v: Any) -> str:
    if isinstance(v, bool): return "BOOL"
    if isinstance(v, int): return "INT64"
    if isinstance(v, float): return "FLOAT64"
    return "STRING"
