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

    def import_logs(self, upload_file: UploadFile, log_type: LogType | None, mode: ImportMode) -> dict[str, int | bool]:
        import_batch_id = ObjectId()
        uploaded_at = datetime.now(timezone.utc)

        total = 0
        errors = 0
        access_count = 0
        error_count = 0
        documents: list[dict[str, Any]] = []

        upload_file.file.seek(0)
        text_stream = TextIOWrapper(upload_file.file, encoding="utf-8-sig", errors="replace")
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
                    preferred_log_type=log_type,
                )
                if document is None:
                    errors += 1
                    continue

                if document["log_type"] == LogType.access.value:
                    access_count += 1
                elif document["log_type"] == LogType.error.value:
                    error_count += 1
                documents.append(document)
        finally:
            text_stream.detach()

        if not documents:
            raise ValueError("File does not contain valid Apache access/error log lines")

        if mode == ImportMode.replace:
            self.repository.delete_all()

        inserted = 0
        for start in range(0, len(documents), settings.import_chunk_size):
            inserted += self.repository.insert_many(documents[start : start + settings.import_chunk_size])

        return {
            "success": True,
            "total": total,
            "inserted": inserted,
            "errors": errors,
            "access": access_count,
            "error": error_count,
        }

    def _build_document(
        self,
        *,
        raw_line: str,
        file_name: str,
        uploaded_at: datetime,
        import_batch_id: ObjectId,
        preferred_log_type: LogType | None = None,
    ) -> dict[str, Any] | None:
        detected = self._parse_with_detected_type(raw_line, preferred_log_type)
        if detected is None:
            return None

        log_type, parsed_payload = detected
        timestamp = parsed_payload.get("time_local") or parsed_payload.get("timestamp")

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
            "parse_error": False,
        }

    @classmethod
    def _parse_with_detected_type(
        cls,
        raw_line: str,
        preferred_log_type: LogType | None,
    ) -> tuple[LogType, dict[str, Any]] | None:
        log_types = [LogType.access, LogType.error]
        if preferred_log_type in log_types:
            log_types.remove(preferred_log_type)
            log_types.insert(0, preferred_log_type)

        for log_type in log_types:
            try:
                parsed = cls._get_parser(log_type)(raw_line)
                return log_type, parsed.model_dump(exclude_none=True)
            except ValueError:
                continue
        return None

    @staticmethod
    def _get_parser(log_type: LogType) -> ParserFunc:
        if log_type == LogType.access:
            return AccessLogParser.parse
        return ErrorLogParser.parse
