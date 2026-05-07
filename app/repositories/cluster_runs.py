from __future__ import annotations

from typing import Any, Iterable

from bson import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database


class ClusterRunsRepository:
    def __init__(self, db: Database) -> None:
        self.collection: Collection = db["cluster_runs"]

    def create(self, document: dict[str, Any]) -> str:
        result = self.collection.insert_one(document)
        return str(result.inserted_id)

    def insert_many(self, documents: Iterable[dict[str, Any]]) -> int:
        docs = list(documents)
        if not docs:
            return 0
        result = self.collection.insert_many(docs, ordered=False)
        return len(result.inserted_ids)

    def count(self, *, method: str | None = None, status: str | None = None) -> int:
        return self.collection.count_documents(self._build_query(method=method, status=status))

    def list(
        self,
        *,
        method: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        cursor = (
            self.collection.find(self._build_query(method=method, status=status))
            .sort("created_at", -1)
            .skip(offset)
            .limit(limit)
        )
        return list(cursor)

    def find_all_for_export(self) -> list[dict[str, Any]]:
        return list(self.collection.find({}).sort("created_at", -1))

    def get_by_id(self, run_id: str) -> dict[str, Any] | None:
        if not ObjectId.is_valid(run_id):
            return None
        return self.collection.find_one({"_id": ObjectId(run_id)})

    def delete_all(self) -> int:
        result = self.collection.delete_many({})
        return result.deleted_count

    @staticmethod
    def _build_query(*, method: str | None, status: str | None) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if method:
            query["method"] = {"$regex": method, "$options": "i"}
        if status and status != "all":
            query["status"] = status
        return query
