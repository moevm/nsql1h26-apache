from __future__ import annotations

from typing import Any, Iterable

from bson import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database


class ClustersRepository:
    def __init__(self, db: Database) -> None:
        self.collection: Collection = db["clusters"]

    def insert_many(self, documents: Iterable[dict[str, Any]]) -> int:
        docs = list(documents)
        if not docs:
            return 0
        result = self.collection.insert_many(docs, ordered=False)
        return len(result.inserted_ids)

    def count_by_run(self, run_id: str, *, search: str | None = None) -> int:
        return self.collection.count_documents(self._build_query(run_id, search=search))

    def list_by_run(
        self,
        run_id: str,
        *,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        cursor = (
            self.collection.find(self._build_query(run_id, search=search))
            .sort("size", -1)
            .skip(offset)
            .limit(limit)
        )
        return list(cursor)

    def get_by_id(self, cluster_id: str) -> dict[str, Any] | None:
        if not ObjectId.is_valid(cluster_id):
            return None
        return self.collection.find_one({"_id": ObjectId(cluster_id)})

    def top_by_run(self, run_id: str, *, limit: int = 10) -> list[dict[str, Any]]:
        return list(self.collection.find({"run_id": run_id}).sort("size", -1).limit(limit))

    def find_all_for_export(self) -> list[dict[str, Any]]:
        return list(self.collection.find({}).sort("size", -1))

    def delete_by_run(self, run_id: str) -> int:
        result = self.collection.delete_many({"run_id": run_id})
        return result.deleted_count

    def delete_all(self) -> int:
        result = self.collection.delete_many({})
        return result.deleted_count

    @staticmethod
    def _build_query(run_id: str, *, search: str | None = None) -> dict[str, Any]:
        query: dict[str, Any] = {"run_id": run_id}
        if search:
            query["cluster_key"] = {"$regex": search, "$options": "i"}
        return query
