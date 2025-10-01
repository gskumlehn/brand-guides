# app/infra/db/bq_client.py
import os
from typing import Any, Dict, List, Optional
from google.cloud import bigquery

from ..auth.credentials import load_credentials, resolve_project_id

_DATASET = os.getenv("BQ_DATASET", "brand_guides")
_bq_client: Optional[bigquery.Client] = None


def client() -> bigquery.Client:
    """Singleton do BigQuery Client autenticado com Service Account."""
    global _bq_client
    if _bq_client is None:
        creds = load_credentials()
        _bq_client = bigquery.Client(
            project=resolve_project_id(creds),
            credentials=creds
        )
    return _bq_client


def fq(table: str) -> str:
    """Fully qualified table (quoted)."""
    return f"`{client().project}.{_DATASET}.{table}`"


def ensure_assets_table() -> None:
    """
    Cria o schema e a tabela 'assets' se não existirem
    e garante as colunas necessárias (sem priority/source_*).
    """
    proj = client().project
    # CREATE SCHEMA + CREATE TABLE (idempotentes)
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS `{proj}.{_DATASET}`;

    CREATE TABLE IF NOT EXISTS {fq('assets')} (
      brand_name    STRING,
      category      STRING,
      subcategory   STRING,
      sequence      INT64,
      original_name STRING,
      path          STRING,
      url           STRING,
      applied_color STRING,
      created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """
    client().query(ddl).result()

    # Garante colunas (para casos de tabela antiga sem esses campos)
    alters = [
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS sequence INT64",
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS applied_color STRING",
        f"ALTER TABLE {fq('assets')} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP",
    ]
    for stmt in alters:
        client().query(stmt).result()


def ensure_colors_table() -> None:
    """
    Tabela 'colors' para armazenar paleta e papéis (role):
    - role ∈ {'primary','secondary','others'}
    """
    proj = client().project
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS `{proj}.{_DATASET}`;

    CREATE TABLE IF NOT EXISTS {fq('colors')} (
      brand_name  STRING,
      color_name  STRING,
      hex         STRING,
      role        STRING,     -- primary | secondary | others
      created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """
    client().query(ddl).result()


def ensure_all_tables() -> None:
    """Convenience: garante todas as tabelas necessárias."""
    ensure_assets_table()
    ensure_colors_table()


def q(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Executa uma QUERY JOB (com parâmetros) e retorna lista de dicts.
    Usa jobs (não streaming).
    """
    job_config = None
    if params:
        qp = []
        for k, v in params.items():
            if isinstance(v, int):
                qp.append(bigquery.ScalarQueryParameter(k, "INT64", v))
            else:
                qp.append(bigquery.ScalarQueryParameter(k, "STRING", v))
        job_config = bigquery.QueryJobConfig(query_parameters=qp)

    rows = client().query(sql, job_config=job_config).result()
    return [dict(r) for r in rows]


def load_json(table: str, rows: List[Dict[str, Any]]) -> None:
    """
    Append via LOAD JOB (evita streaming buffer).
    Ideal para inserir lotes (ex.: ingestão).
    """
    if not rows:
        return
    job = client().load_table_from_json(
        rows,
        f"{client().project}.{_DATASET}.{table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
        ),
    )
    job.result()


# Alias mantido p/ compatibilidade retro (algum código pode chamá-lo)
def ensure_assets_tables() -> None:
    ensure_all_tables()
