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

    def insert_many(self, documents: Iterable[dict[str, Any]]) -> int:
        docs = list(documents)
        if not docs:
            return 0
        result = self.collection.insert_many(docs, ordered=False)
        return len(result.inserted_ids)

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

    def get_by_id(self, log_id: str) -> Optional[dict[str, Any]]:
        return self.collection.find_one({"_id": ObjectId(log_id)})

    @staticmethod
    def _build_logs_query(params: LogsFilterParams) -> dict[str, Any]:
        query: dict[str, Any] = {}

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

        if params.method:
            query["parsed.method"] = params.method.upper()

        if params.search:
            escaped = re.escape(params.search)
            regex = {"$regex": escaped, "$options": "i"}
            query["$or"] = [
                {"raw": regex},
                {"parsed.uri": regex},
                {"parsed.message": regex},
            ]

        return query
