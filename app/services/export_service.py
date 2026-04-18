from __future__ import annotations

from pymongo.database import Database

from app.models.log import ExportQueryParams
from app.repositories.logs import LogsRepository
from app.services.serialization import serialize_log_document


class ExportService:
    def __init__(self, db: Database) -> None:
        self.repository = LogsRepository(db)

    def export_logs(self, params: ExportQueryParams) -> list[dict]:
        documents = self.repository.find_for_export(params)
        return [serialize_log_document(document, omit_empty_normalized=True) for document in documents]
