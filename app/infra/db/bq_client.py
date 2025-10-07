# app/infra/db/bq_client.py

import os
from typing import Any, Dict, List, Optional, Iterable
from google.cloud import bigquery
from ..auth.credentials import load_credentials, resolve_project_id

__all__ = [
    "client",
    "fq",
    "ensure_dataset",
    "ensure_assets_table",
    "ensure_colors_table",
    "ensure_all_tables",
    "ensure_assets_tables",  # alias solicitado
    "q",
    "q_stream",
    "load_json",
]

_DATASET = os.getenv("BQ_DATASET", "brand_guides")
_bq_client: Optional[bigquery.Client] = None


# ----------------------------
# Client / helpers
# ----------------------------
def client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        creds = load_credentials()
        _bq_client = bigquery.Client(project=resolve_project_id(creds), credentials=creds)
    return _bq_client


def fq(table: str) -> str:
    return f"`{client().project}.{_DATASET}.{table}`"


def _exec(sql: str) -> None:
    client().query(sql).result()


def _exec_many(ddls: List[str]) -> None:
    for ddl in ddls:
        if ddl and ddl.strip():
            _exec(ddl)


# ----------------------------
# Ensure (Dataset / Tables)
# ----------------------------
def ensure_dataset() -> None:
    _exec(f"CREATE SCHEMA IF NOT EXISTS `{client().project}.{_DATASET}`;")


def ensure_assets_table() -> None:
    """
    Tabela 'assets' compatível com legado, com colunas novas para a nova ingestão.
    Mantém 'category' e 'subcategory' legadas; adiciona chaves/seq novas.
    """
    ensure_dataset()

    # Criação base (legado)
    _exec(f"""
    CREATE TABLE IF NOT EXISTS {fq('assets')} (
      brand_name        STRING,
      category          STRING,
      subcategory       STRING,
      sequence          INT64,
      original_name     STRING,
      path              STRING,
      url               STRING,
      created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """)

    # Evoluções (idempotentes)
    _exec_many([
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS category_key STRING;",
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS category_label STRING;",
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS category_seq INT64;",
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS subcategory_key STRING;",
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS subcategory_label STRING;",
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS subcategory_seq INT64;",
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS columns INT64;",        # 1..4
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS is_original BOOL;",
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS asset_type STRING;",    # 'image' | 'text'
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS text_content STRING;",  # textos de categoria/sub
    ])

    # View de compatibilidade opcional
    _exec(f"""
    CREATE OR REPLACE VIEW {fq('assets_legacy')} AS
    SELECT
      brand_name,
      COALESCE(category_label, category)              AS category,
      COALESCE(subcategory_label, subcategory)        AS subcategory,
      sequence, original_name, path, url, created_at
    FROM {fq('assets')};
    """)


def ensure_colors_table() -> None:
    ensure_dataset()
    _exec(f"""
    CREATE TABLE IF NOT EXISTS {fq('colors')} (
      brand_name  STRING,
      color_name  STRING,
      hex         STRING,
      role        STRING,
      created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """)
    _exec_many([
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS palette_key STRING;",
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS color_key STRING;",
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS color_label STRING;",
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS rgb_txt STRING;",
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS cmic_txt STRING;",   # legado
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS cmyk_txt STRING;",   # novo
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS pantone_txt STRING;",# novo
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS category STRING;",   # novo: main|secondary
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS subcategory STRING;",# novo: primary|secondary|others|null
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS sequence INT64;",
        f"ALTER TABLE {fq('colors')} ADD COLUMN IF NOT EXISTS raw_json JSON;",
    ])


def ensure_all_tables() -> None:
    ensure_assets_table()
    ensure_colors_table()


def ensure_assets_tables() -> None:
    """
    Alias solicitado por chamadas antigas. Mantém assinatura.
    """
    ensure_all_tables()


# ----------------------------
# Query / Load helpers
# ----------------------------
def _infer_type(v: Any) -> str:
    if isinstance(v, bool):  return "BOOL"
    if isinstance(v, int):   return "INT64"
    if isinstance(v, float): return "FLOAT64"
    return "STRING"


def q(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    job_config = None
    if params:
        qp = [bigquery.ScalarQueryParameter(k, _infer_type(v), v) for k, v in params.items()]
        job_config = bigquery.QueryJobConfig(query_parameters=qp)
    rows = client().query(sql, job_config=job_config).result()
    return [dict(r) for r in rows]


def q_stream(sql: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
    job_config = None
    if params:
        qp = [bigquery.ScalarQueryParameter(k, _infer_type(v), v) for k, v in params.items()]
        job_config = bigquery.QueryJobConfig(query_parameters=qp)
    for row in client().query(sql, job_config=job_config).result(page_size=1000):
        yield dict(row)


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
