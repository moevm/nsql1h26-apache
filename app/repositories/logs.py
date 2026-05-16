from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Iterable, Optional

from bson import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database

from app.models.log import ExportQueryParams, LogsFilterParams


class LogsRepository:
    def __init__(self, db: Database) -> None:
        self.collection: Collection = db["logs"]

    def delete_by_type(self, log_type: str) -> int:
        result = self.collection.delete_many({"log_type": log_type})
        return result.deleted_count

    def delete_all(self) -> int:
        result = self.collection.delete_many({})
        return result.deleted_count

    def is_empty(self) -> bool:
        return self.collection.estimated_document_count() == 0

    def insert_many(self, documents: Iterable[dict[str, Any]]) -> int:
        docs = list(documents)
        if not docs:
            return 0
        result = self.collection.insert_many(docs, ordered=False)
        return len(result.inserted_ids)

    def insert_one(self, document: dict[str, Any]) -> str:
        result = self.collection.insert_one(document)
        return str(result.inserted_id)

    def count_by_filters(self, params: LogsFilterParams) -> int:
        return self.collection.count_documents(self._build_logs_query(params))

    def find_logs(self, params: LogsFilterParams) -> list[dict[str, Any]]:
        query = self._build_logs_query(params)
        cursor = (
            self.collection.find(query)
            .sort("timestamp", -1)
            .skip(params.offset)
            .limit(params.limit)
        )
        return list(cursor)

    def find_for_export(self, params: ExportQueryParams) -> list[dict[str, Any]]:
        query: dict[str, Any] = {}
        if params.type is not None:
            query["log_type"] = params.type.value
        cursor = (
            self.collection.find(query)
            .sort("timestamp", -1)
            .skip(params.offset)
            .limit(params.limit)
        )
        return list(cursor)

    def find_all_for_export(self) -> list[dict[str, Any]]:
        return list(self.collection.find({}).sort("timestamp", -1))

    def find_for_clustering(self, params: LogsFilterParams) -> list[dict[str, Any]]:
        query = self._build_logs_query(params)
        return list(self.collection.find(query).sort("timestamp", 1))

    def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return list(self.collection.aggregate(pipeline, allowDiskUse=True))

    def build_query(self, params: LogsFilterParams) -> dict[str, Any]:
        return self._build_logs_query(params)

    def get_by_id(self, log_id: str) -> Optional[dict[str, Any]]:
        return self.collection.find_one({"_id": ObjectId(log_id)})

    @staticmethod
    def _build_logs_query(params: LogsFilterParams) -> dict[str, Any]:
        query: dict[str, Any] = {}
        and_clauses: list[dict[str, Any]] = []

        if params.type is not None:
            query["log_type"] = params.type.value

        timestamp_query: dict[str, Any] = {}
        if isinstance(params.from_date, datetime):
            timestamp_query["$gte"] = params.from_date
        if isinstance(params.to_date, datetime):
            timestamp_query["$lte"] = params.to_date
        if timestamp_query:
            query["timestamp"] = timestamp_query

        if params.status is not None:
            query["parsed.status"] = params.status

        if params.status_group and params.status_group != "all":
            range_query = LogsRepository._status_group_query(params.status_group)
            if range_query:
                and_clauses.append({"log_type": "access"})
                and_clauses.append({"parsed.status": range_query})

        if params.result and params.result != "all":
            result_query = LogsRepository._result_query(params.result)
            if result_query:
                and_clauses.append(result_query)

        if params.method:
            query["parsed.method"] = params.method.upper()

        if params.ip:
            ip_regex = LogsRepository._prefix_regex(params.ip)
            and_clauses.append(
                {
                    "$or": [
                        {"parsed.ip": ip_regex},
                        {"parsed.remote_addr": ip_regex},
                        {"parsed.client": ip_regex},
                    ]
                }
            )

        if params.cluster:
            cluster_query = LogsRepository._cluster_query(params.cluster)
            if cluster_query:
                and_clauses.append(cluster_query)

        if params.search:
            escaped = re.escape(params.search)
            regex = {"$regex": escaped, "$options": "i"}
            query["$or"] = [
                {"raw": regex},
                {"parsed.uri": regex},
                {"parsed.message": regex},
            ]

        if and_clauses:
            existing_and = query.get("$and")
            if isinstance(existing_and, list):
                existing_and.extend(and_clauses)
            else:
                query["$and"] = and_clauses

        return query

    @staticmethod
    def _status_group_query(status_group: str) -> dict[str, int] | None:
        ranges = {
            "2xx": {"$gte": 200, "$lt": 300},
            "3xx": {"$gte": 300, "$lt": 400},
            "4xx": {"$gte": 400, "$lt": 500},
            "5xx": {"$gte": 500, "$lt": 600},
            "4xx5xx": {"$gte": 400, "$lt": 600},
        }
        return ranges.get(status_group)

    @staticmethod
    def _result_query(result: str) -> dict[str, Any] | None:
        if result == "failed":
            return {
                "$or": [
                    {"log_type": "error"},
                    {"log_type": "access", "parsed.status": {"$gte": 400}},
                ]
            }
        if result == "success":
            return {"log_type": "access", "parsed.status": {"$lt": 400}}
        return None

    @staticmethod
    def _prefix_regex(value: str) -> dict[str, str]:
        prefix = value.strip()
        if "*" in prefix:
            prefix = prefix.split("*", 1)[0]
        return {"$regex": f"^{re.escape(prefix)}", "$options": "i"}

    @staticmethod
    def _cluster_query(cluster_key: str) -> dict[str, Any] | None:
        key = cluster_key.strip()
        access_match = re.match(r"^(?P<method>[A-Za-z]+)\s+(?P<uri>.+)#(?P<status>\d+|-)$", key)
        if access_match:
            method = access_match.group("method").upper()
            status_raw = access_match.group("status")
            uri_regex = LogsRepository._template_to_regex(access_match.group("uri"), uri=True)
            query: dict[str, Any] = {
                "log_type": "access",
                "parsed.method": method,
                "$or": [
                    {"parsed.uri": uri_regex},
                    {"parsed.request_path": uri_regex},
                ],
            }
            if status_raw != "-":
                query["parsed.status"] = int(status_raw)
            return query

        if ":" in key:
            level, message_template = key.split(":", 1)
            return {
                "log_type": "error",
                "parsed.level": level.strip(),
                "parsed.message": LogsRepository._template_to_regex(message_template.strip(), uri=False),
            }

        return None

    @staticmethod
    def _template_to_regex(template: str, *, uri: bool) -> dict[str, str]:
        escaped = re.escape(template)
        replacements = {
            re.escape("<ID>"): r"\d+",
            re.escape("<id>"): r"\d+",
            re.escape("<IP>"): r"\d{1,3}(?:\.\d{1,3}){3}",
            re.escape("<ip>"): r"\d{1,3}(?:\.\d{1,3}){3}",
            re.escape("<PATH_ID>"): r"/[A-Za-z0-9._/-]*\d+[A-Za-z0-9._/-]*",
            re.escape("<path_id>"): r"/[A-Za-z0-9._/-]*\d+[A-Za-z0-9._/-]*",
        }
        for token, pattern in replacements.items():
            escaped = escaped.replace(token, pattern)
        suffix = r"(?:\?.*)?" if uri else ""
        return {"$regex": f"^{escaped}{suffix}$", "$options": "i"}
