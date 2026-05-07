from __future__ import annotations

from datetime import datetime, timezone
from io import TextIOWrapper
import json
from typing import Any, Callable

from bson import ObjectId
from fastapi import UploadFile
from pymongo.database import Database

from app.config import settings
from app.models.cluster import Cluster
from app.models.cluster_run import ClusterRun
from app.models.common import ImportMode, LogType
from app.models.log import LogDocument
from app.parsers import AccessLogParser, ErrorLogParser
from app.repositories.cluster_runs import ClusterRunsRepository
from app.repositories.clusters import ClustersRepository
from app.repositories.logs import LogsRepository

ParserFunc = Callable[[str], Any]


class ImportService:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.repository = LogsRepository(db)
        self.runs_repository = ClusterRunsRepository(db)
        self.clusters_repository = ClustersRepository(db)

    def import_logs(self, upload_file: UploadFile, log_type: LogType | None, mode: ImportMode) -> dict[str, int | bool]:
        if self._looks_like_json(upload_file):
            return self._import_application_dump(upload_file, mode)
        return self._import_apache_logs(upload_file, log_type, mode)

    def _import_apache_logs(
        self,
        upload_file: UploadFile,
        log_type: LogType | None,
        mode: ImportMode,
    ) -> dict[str, int | bool]:
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
            "logs": inserted,
            "cluster_runs": 0,
            "clusters": 0,
        }

    def _import_application_dump(self, upload_file: UploadFile, mode: ImportMode) -> dict[str, int | bool]:
        payload = self._read_json_payload(upload_file)
        logs_payload = payload.get("logs") or []
        runs_payload = payload.get("cluster_runs") or []
        clusters_payload = payload.get("clusters") or []

        if not isinstance(logs_payload, list) or not isinstance(runs_payload, list) or not isinstance(clusters_payload, list):
            raise ValueError("Application JSON dump must contain list fields: logs, cluster_runs, clusters")

        preserve_ids = mode == ImportMode.replace
        log_id_map: dict[str, str] = {}
        run_id_map: dict[str, str] = {}
        logs = [self._restore_log_document(item, log_id_map, preserve_id=preserve_ids) for item in logs_payload]
        runs = [self._restore_run_document(item, run_id_map, preserve_id=preserve_ids) for item in runs_payload]
        clusters = [
            self._restore_cluster_document(item, run_id_map, log_id_map, preserve_id=preserve_ids)
            for item in clusters_payload
        ]

        if mode == ImportMode.replace:
            self.clusters_repository.delete_all()
            self.runs_repository.delete_all()
            self.repository.delete_all()

        logs_inserted = self._insert_in_chunks(self.repository.insert_many, logs)
        runs_inserted = self.runs_repository.insert_many(runs)
        clusters_inserted = self.clusters_repository.insert_many(clusters)

        return {
            "success": True,
            "total": len(logs) + len(runs) + len(clusters),
            "inserted": logs_inserted + runs_inserted + clusters_inserted,
            "errors": 0,
            "access": sum(1 for item in logs if item.get("log_type") == LogType.access.value),
            "error": sum(1 for item in logs if item.get("log_type") == LogType.error.value),
            "logs": logs_inserted,
            "cluster_runs": runs_inserted,
            "clusters": clusters_inserted,
        }

    @staticmethod
    def _looks_like_json(upload_file: UploadFile) -> bool:
        upload_file.file.seek(0)
        prefix = upload_file.file.read(512)
        upload_file.file.seek(0)
        if isinstance(prefix, bytes):
            prefix_text = prefix.decode("utf-8-sig", errors="ignore")
        else:
            prefix_text = str(prefix)
        return prefix_text.lstrip().startswith("{")

    @staticmethod
    def _read_json_payload(upload_file: UploadFile) -> dict[str, Any]:
        upload_file.file.seek(0)
        raw = upload_file.file.read()
        if isinstance(raw, bytes):
            text = raw.decode("utf-8-sig")
        else:
            text = str(raw)
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("Invalid JSON application dump") from exc
        if not isinstance(payload, dict) or not {"logs", "cluster_runs", "clusters"}.issubset(payload.keys()):
            raise ValueError("JSON file is not an application dump with logs, cluster_runs and clusters")
        return payload

    def _restore_log_document(self, payload: Any, log_id_map: dict[str, str], *, preserve_id: bool) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Each logs item must be an object")
        document = LogDocument.model_validate(payload).model_dump(by_alias=True, exclude_none=True)
        old_id = str(document.get("_id") or "")
        self._restore_object_id(document, preserve_id=preserve_id)
        if old_id:
            log_id_map[old_id] = str(document["_id"])
        if isinstance(document.get("log_type"), LogType):
            document["log_type"] = document["log_type"].value
        import_batch_id = document.get("import_batch_id")
        if isinstance(import_batch_id, str) and ObjectId.is_valid(import_batch_id):
            document["import_batch_id"] = ObjectId(import_batch_id)
        return document

    def _restore_run_document(self, payload: Any, run_id_map: dict[str, str], *, preserve_id: bool) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Each cluster_runs item must be an object")
        document = ClusterRun.model_validate(payload).model_dump(by_alias=True, exclude_none=True)
        old_id = str(document.get("_id") or "")
        self._restore_object_id(document, preserve_id=preserve_id)
        new_id = str(document["_id"])
        if old_id:
            run_id_map[old_id] = new_id
        return document

    def _restore_cluster_document(
        self,
        payload: Any,
        run_id_map: dict[str, str],
        log_id_map: dict[str, str],
        *,
        preserve_id: bool,
    ) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Each clusters item must be an object")
        document = Cluster.model_validate(payload).model_dump(by_alias=True, exclude_none=True)
        self._restore_object_id(document, preserve_id=preserve_id)
        run_id = str(document.get("run_id") or "")
        document["run_id"] = run_id_map.get(run_id, run_id)
        for sample in document.get("samples") or []:
            log_id = str(sample.get("log_id") or "")
            sample["log_id"] = log_id_map.get(log_id, log_id)
        return document

    @staticmethod
    def _restore_object_id(document: dict[str, Any], *, preserve_id: bool) -> None:
        raw_id = document.get("_id")
        if preserve_id and isinstance(raw_id, str) and ObjectId.is_valid(raw_id):
            document["_id"] = ObjectId(raw_id)
        else:
            document["_id"] = ObjectId()

    def _insert_in_chunks(self, inserter: Callable[[list[dict[str, Any]]], int], documents: list[dict[str, Any]]) -> int:
        inserted = 0
        for start in range(0, len(documents), settings.import_chunk_size):
            inserted += inserter(documents[start : start + settings.import_chunk_size])
        return inserted

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
