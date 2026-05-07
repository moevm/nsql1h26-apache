from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any

from bson import ObjectId
from pymongo.database import Database

from app.models.cluster_run import ClusterRunCreateRequest, build_logs_filter_params
from app.repositories.cluster_runs import ClusterRunsRepository
from app.repositories.clusters import ClustersRepository
from app.repositories.logs import LogsRepository


@dataclass
class ClusterBucket:
    key: str
    description: str
    samples: list[dict[str, str]] = field(default_factory=list)
    size: int = 0
    statuses: Counter[str] = field(default_factory=Counter)
    methods: Counter[str] = field(default_factory=Counter)
    log_types: Counter[str] = field(default_factory=Counter)
    first_seen: datetime | None = None
    last_seen: datetime | None = None


class ClusteringService:
    def __init__(self, db: Database) -> None:
        self.logs_repository = LogsRepository(db)
        self.runs_repository = ClusterRunsRepository(db)
        self.clusters_repository = ClustersRepository(db)

    def run(self, payload: ClusterRunCreateRequest) -> dict[str, Any]:
        params = build_logs_filter_params(payload.filters)
        logs = self.logs_repository.find_for_clustering(params)
        buckets: dict[str, ClusterBucket] = {}

        for document in logs:
            cluster_key, description = self._build_cluster_key(document)
            if not cluster_key:
                continue

            bucket = buckets.setdefault(cluster_key, ClusterBucket(key=cluster_key, description=description))
            bucket.size += 1
            self._add_document_to_bucket(bucket, document)

        run_id = ObjectId()
        clusters = [self._bucket_to_document(run_id, bucket) for bucket in buckets.values()]
        clusters.sort(key=lambda item: item["size"], reverse=True)

        now = datetime.now(timezone.utc)
        run_document = {
            "_id": run_id,
            "created_at": now,
            "method": payload.method,
            "filters": payload.filters,
            "summary": {
                "logs_total": len(logs),
                "clusters_total": len(clusters),
                "clustered_logs_total": sum(cluster["size"] for cluster in clusters),
                "unclustered_logs_total": max(0, len(logs) - sum(cluster["size"] for cluster in clusters)),
            },
            "preset_name": payload.preset_name,
            "status": "finished",
        }

        self.runs_repository.create(run_document)
        self.clusters_repository.insert_many(clusters)
        return run_document

    def stats(self, run_id: str) -> dict[str, Any]:
        run = self.runs_repository.get_by_id(run_id)
        if run is None:
            raise ValueError("Cluster run not found")

        top_clusters = self.clusters_repository.top_by_run(run_id, limit=10)
        status_counts: Counter[str] = Counter()
        method_counts: Counter[str] = Counter()
        log_type_counts: Counter[str] = Counter()

        for cluster in top_clusters:
            stats = cluster.get("stats") or {}
            status_counts.update(stats.get("status_counts") or {})
            method_counts.update(stats.get("method_counts") or {})
            log_type_counts.update(stats.get("log_type_counts") or {})

        return {
            "run": run,
            "top_clusters": top_clusters,
            "status_counts": dict(status_counts),
            "method_counts": dict(method_counts),
            "log_type_counts": dict(log_type_counts),
        }

    def _build_cluster_key(self, document: dict[str, Any]) -> tuple[str | None, str]:
        parsed = document.get("parsed") or {}
        log_type = document.get("log_type")

        if log_type == "access":
            method = (parsed.get("method") or parsed.get("request_method") or "UNKNOWN").upper()
            uri = parsed.get("uri") or parsed.get("request_path") or "-"
            status = parsed.get("status") or "-"
            uri_template = self._template_uri(uri)
            normalized = document.setdefault("normalized", {})
            normalized["uri_template"] = uri_template
            normalized["signature"] = f"{method} {uri_template} {status}"
            return f"{method} {uri_template}#{status}", "access: method + uri template + status"

        if log_type == "error":
            level = parsed.get("level") or "unknown"
            message_template = self._template_message(parsed.get("message") or document.get("raw") or "")
            normalized = document.setdefault("normalized", {})
            normalized["message_template"] = message_template
            normalized["signature"] = f"{level} {message_template}"
            return f"{level}: {message_template}", "error: level + message template"

        return None, "unsupported log type"

    def _add_document_to_bucket(self, bucket: ClusterBucket, document: dict[str, Any]) -> None:
        parsed = document.get("parsed") or {}
        log_id = str(document.get("_id"))
        if len(bucket.samples) < 5:
            bucket.samples.append({"log_id": log_id, "raw": document.get("raw") or ""})

        log_type = str(document.get("log_type") or "unknown")
        bucket.log_types[log_type] += 1

        method = parsed.get("method") or parsed.get("request_method")
        if method:
            bucket.methods[str(method).upper()] += 1

        status = parsed.get("status")
        if status is not None:
            bucket.statuses[str(status)] += 1

        timestamp = document.get("timestamp")
        if isinstance(timestamp, datetime):
            if bucket.first_seen is None or timestamp < bucket.first_seen:
                bucket.first_seen = timestamp
            if bucket.last_seen is None or timestamp > bucket.last_seen:
                bucket.last_seen = timestamp

    @staticmethod
    def _bucket_to_document(run_id: ObjectId, bucket: ClusterBucket) -> dict[str, Any]:
        return {
            "run_id": str(run_id),
            "cluster_key": bucket.key,
            "size": bucket.size,
            "stats": {
                "status_counts": dict(bucket.statuses),
                "method_counts": dict(bucket.methods),
                "log_type_counts": dict(bucket.log_types),
                "first_seen": bucket.first_seen,
                "last_seen": bucket.last_seen,
            },
            "samples": bucket.samples,
            "description": bucket.description,
        }

    @staticmethod
    def _template_uri(uri: str) -> str:
        parts = uri.split("?")[0].split("/")
        templated = ["<ID>" if re.fullmatch(r"\d+", part) else part for part in parts]
        return "/".join(templated) or "/"

    @staticmethod
    def _template_message(message: str) -> str:
        value = re.sub(r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "<IP>", message)
        value = re.sub(r"\b\d+\b", "<ID>", value)
        value = re.sub(r"/[A-Za-z0-9._/-]*<ID>[A-Za-z0-9._/-]*", "/<PATH_ID>", value)
        return value.strip() or "empty message"
