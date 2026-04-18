from __future__ import annotations

from datetime import datetime, timezone
from io import TextIOWrapper
from typing import Any, Callable

from bson import ObjectId
from fastapi import UploadFile
from pymongo.database import Database

from app.config import settings
from app.models.common import ImportMode, LogType
from app.parsers import AccessLogParser, ErrorLogParser
from app.repositories.logs import LogsRepository

ParserFunc = Callable[[str], Any]


class ImportService:
    def __init__(self, db: Database) -> None:
        self.repository = LogsRepository(db)

    def import_logs(self, upload_file: UploadFile, log_type: LogType, mode: ImportMode) -> dict[str, int | bool]:
        if mode == ImportMode.replace:
            self.repository.delete_by_type(log_type.value)

        parser = self._get_parser(log_type)
        import_batch_id = ObjectId()
        uploaded_at = datetime.now(timezone.utc)

        total = 0
        inserted = 0
        errors = 0
        batch: list[dict[str, Any]] = []

        upload_file.file.seek(0)
        text_stream = TextIOWrapper(upload_file.file, encoding="utf-8", errors="replace")
        try:
            for line in text_stream:
                raw_line = line.rstrip("\r\n")
                if not raw_line.strip():
                    continue

                total += 1
                document = self._build_document(
                    raw_line=raw_line,
                    file_name=upload_file.filename or "uploaded.log",
                    uploaded_at=uploaded_at,
                    import_batch_id=import_batch_id,
                    log_type=log_type,
                    parser=parser,
                )
                if document["parse_error"]:
                    errors += 1
                batch.append(document)

                if len(batch) >= settings.import_chunk_size:
                    inserted += self.repository.insert_many(batch)
                    batch.clear()

            if batch:
                inserted += self.repository.insert_many(batch)
        finally:
            text_stream.detach()

        return {
            "success": True,
            "total": total,
            "inserted": inserted,
            "errors": errors,
        }

    def _build_document(
        self,
        *,
        raw_line: str,
        file_name: str,
        uploaded_at: datetime,
        import_batch_id: ObjectId,
        log_type: LogType,
        parser: ParserFunc,
    ) -> dict[str, Any]:
        try:
            parsed = parser(raw_line)
            parsed_payload = parsed.model_dump(exclude_none=True)
            timestamp = parsed_payload.get("time_local") or parsed_payload.get("timestamp")
            parse_error = False
        except ValueError:
            parsed_payload = {}
            timestamp = None
            parse_error = True

        normalized = {
            "uri_template": None,
            "message_template": None,
            "signature": None,
            "ip_prefix": None,
        }

        return {
            "import_batch_id": import_batch_id,
            "log_type": log_type.value,
            "source": {
                "file_name": file_name,
                "uploaded_at": uploaded_at,
            },
            "raw": raw_line,
            "timestamp": timestamp,
            "parsed": parsed_payload,
            "normalized": normalized,
            "parse_error": parse_error,
        }

    @staticmethod
    def _get_parser(log_type: LogType) -> ParserFunc:
        if log_type == LogType.access:
            return AccessLogParser.parse
        return ErrorLogParser.parse
