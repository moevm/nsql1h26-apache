from __future__ import annotations

from datetime import datetime, timezone

from bson import ObjectId
from pymongo.database import Database

from app.repositories.logs import LogsRepository
from app.services.import_service import ImportService
from app.services.log_generator import generate_lines


class SeedService:
    def __init__(self, db: Database) -> None:
        self.repository = LogsRepository(db)
        self.import_service = ImportService(db)

    def seed_if_empty(self) -> int:
        if not self.repository.is_empty():
            return 0

        access_lines, error_lines = generate_lines(
            access_count=300,
            error_count=80,
            seed=42,
            days=10,
        )
        uploaded_at = datetime.now(timezone.utc)
        import_batch_id = ObjectId()
        documents = []

        for raw_line in access_lines + error_lines:
            document = self.import_service._build_document(
                raw_line=raw_line,
                file_name="demo_seed.log",
                uploaded_at=uploaded_at,
                import_batch_id=import_batch_id,
            )
            if document is not None:
                documents.append(document)

        return self.repository.insert_many(documents)
