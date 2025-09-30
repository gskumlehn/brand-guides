# app/services/ingestion_service.py
from typing import Iterable, Mapping
from ..repositories.asset_repository import AssetRepository

class IngestionService:
    def __init__(self, repo: AssetRepository | None = None):
        self.repo = repo or AssetRepository()

    def ingest_assets(self, rows: Iterable[Mapping]):
        """
        rows: iterável de dicts exatamente com os campos de assets.
        """
        self.repo.upsert_batch(rows)
