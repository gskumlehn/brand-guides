import os
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from ..auth.credentials import load_credentials, resolve_project_id

_DATASET = os.getenv("BQ_DATASET", "brand_guides")
_TABLE = "assets"

_client: Optional[bigquery.Client] = None


def client() -> bigquery.Client:
    global _client
    if _client is None:
        creds = load_credentials()
        project = resolve_project_id()
        _client = bigquery.Client(credentials=creds, project=project)
    return _client


def fq(table: str) -> str:
    """Fully-qualified table."""
    c = client()
    return f"`{c.project}.{_DATASET}.{table}`"


def ensure_assets_tables() -> None:
    """
    Garante que o dataset e a tabela existam e que as colunas novas estejam presentes.
    """
    c = client()
    # 1) Dataset
    c.query(f"CREATE SCHEMA IF NOT EXISTS `{c.project}.{_DATASET}`").result()

    # 2) Tabela base (caso ainda não exista)
    c.query(
        f"""
        CREATE TABLE IF NOT EXISTS {fq(_TABLE)} (
          brand_name   STRING NOT NULL,
          category     STRING NOT NULL,
          subcategory  STRING,
          original_name STRING NOT NULL,
          path         STRING NOT NULL,
          sequence     INT64,
          url          STRING,
          mime_type    STRING,
          file_ext     STRING,
          -- Novos metadados de logos/guidelines
          logo_variant STRING,          -- 'primary' | 'secondary_horizontal' | 'secondary_vertical'
          logo_color   STRING,          -- PNG: 'primary' | 'secondary' | 'black' | 'white'
          color_primary STRING,         -- JPG guideline: 'primary' | 'secondary' | 'black' | 'white'
          color_secondary STRING,       -- JPG guideline: idem
          created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """
    ).result()

    # 3) ADD COLUMN IF NOT EXISTS (idempotente para ambientes que já tinham a tabela)
    alters = [
        ("mime_type", "STRING"),
        ("file_ext", "STRING"),
        ("logo_variant", "STRING"),
        ("logo_color", "STRING"),
        ("color_primary", "STRING"),
        ("color_secondary", "STRING"),
        ("created_at", "TIMESTAMP"),
    ]
    for col, typ in alters:
        c.query(f"ALTER TABLE {fq(_TABLE)} ADD COLUMN IF NOT EXISTS {col} {typ}").result()


def _bq_param(name: str, value: Any) -> ScalarQueryParameter:
    # Inferência simples de tipo
    if isinstance(value, bool):
        return ScalarQueryParameter(name, "BOOL", value)
    if isinstance(value, int):
        return ScalarQueryParameter(name, "INT64", value)
    if isinstance(value, float):
        return ScalarQueryParameter(name, "FLOAT64", value)
    # None ou string/qualquer outro => STRING
    return ScalarQueryParameter(name, "STRING", value)


def q(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Executa uma query como job (sem streaming). Retorna linhas como dict.
    """
    job_config = None
    if params:
        job_config = QueryJobConfig(
            query_parameters=[_bq_param(k, v) for k, v in params.items()]
        )
    rows = client().query(sql, job_config=job_config).result()
    out = []
    for r in rows:
        out.append(dict(r.items()))
    return out
