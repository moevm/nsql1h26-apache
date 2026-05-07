from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from pymongo.database import Database

from app.models.common import LogType
from app.models.cluster_run import ClusterRunCreateRequest, build_logs_filter_params
from app.repositories.cluster_runs import ClusterRunsRepository
from app.repositories.clusters import ClustersRepository
from app.repositories.logs import LogsRepository


class ClusteringService:
    def __init__(self, db: Database) -> None:
        self.logs_repository = LogsRepository(db)
        self.runs_repository = ClusterRunsRepository(db)
        self.clusters_repository = ClustersRepository(db)

    def run(self, payload: ClusterRunCreateRequest) -> dict[str, Any]:
        run_id = ObjectId()
        method = self._normalize_method(payload.method)
        filters = self._normalize_filters_for_method(payload.filters, method)
        params = build_logs_filter_params(filters)
        logs_total = self.logs_repository.count_by_filters(params)
        clusters = self.logs_repository.aggregate(self._build_cluster_pipeline(params, method, run_id))

        now = datetime.now(timezone.utc)
        run_document = {
            "_id": run_id,
            "created_at": now,
            "method": method,
            "filters": filters,
            "summary": {
                "logs_total": logs_total,
                "clusters_total": len(clusters),
                "clustered_logs_total": sum(cluster["size"] for cluster in clusters),
                "unclustered_logs_total": max(0, logs_total - sum(cluster["size"] for cluster in clusters)),
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
        status_counts: dict[str, int] = {}
        method_counts: dict[str, int] = {}
        log_type_counts: dict[str, int] = {}

        for cluster in top_clusters:
            stats = cluster.get("stats") or {}
            self._merge_counts(status_counts, stats.get("status_counts") or {})
            self._merge_counts(method_counts, stats.get("method_counts") or {})
            self._merge_counts(log_type_counts, stats.get("log_type_counts") or {})

        return {
            "run": run,
            "top_clusters": top_clusters,
            "status_counts": status_counts,
            "method_counts": method_counts,
            "log_type_counts": log_type_counts,
        }

    @staticmethod
    def _normalize_method(method: str) -> str:
        supported = {"rule_based", "access_endpoint_status", "error_level_message", "access_endpoint_status_ip"}
        return method if method in supported else "rule_based"

    @staticmethod
    def _normalize_filters_for_method(filters: dict[str, Any], method: str) -> dict[str, Any]:
        normalized = dict(filters)
        if method in {"access_endpoint_status", "access_endpoint_status_ip"}:
            normalized["type"] = LogType.access.value
        elif method == "error_level_message":
            normalized["type"] = LogType.error.value
        return normalized

    def _build_cluster_pipeline(self, params, method: str, run_id: ObjectId) -> list[dict[str, Any]]:
        access_key = {"$concat": ["$__method", " ", "$__uri_template", "#", "$__status"]}
        access_ip_key = {"$concat": ["$__method", " ", "$__uri_template", "#", "$__status", "#", "$__ip_prefix"]}
        error_key = {"$concat": ["$__level", ": ", "$__message_template"]}

        if method == "access_endpoint_status":
            cluster_key = access_key
            description = "access: method + uri template + status"
        elif method == "access_endpoint_status_ip":
            cluster_key = access_ip_key
            description = "access: method + uri template + status + ip prefix"
        elif method == "error_level_message":
            cluster_key = error_key
            description = "error: level + message template"
        else:
            cluster_key = {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$log_type", LogType.access.value]}, "then": access_key},
                        {"case": {"$eq": ["$log_type", LogType.error.value]}, "then": error_key},
                    ],
                    "default": None,
                }
            }
            description = "rule-based: access by endpoint/status, error by message template"

        return [
            {"$match": self.logs_repository.build_query(params)},
            {
                "$set": {
                    "__method": {
                        "$toUpper": {
                            "$ifNull": ["$parsed.method", {"$ifNull": ["$parsed.request_method", "UNKNOWN"]}]
                        }
                    },
                    "__uri": {"$ifNull": ["$parsed.uri", {"$ifNull": ["$parsed.request_path", "-"]}]},
                    "__status": {"$toString": {"$ifNull": ["$parsed.status", "-"]}},
                    "__level": {"$toString": {"$ifNull": ["$parsed.level", "unknown"]}},
                    "__message": {"$ifNull": ["$parsed.message", "$raw"]},
                    "__ip": {"$ifNull": ["$parsed.ip", {"$ifNull": ["$parsed.remote_addr", "$parsed.client"]}]},
                }
            },
            {
                "$set": {
                    "__uri_template": {
                        "$function": {
                            "body": "function(uri) { return String(uri || '-').split('?')[0].replace(/\\/\\d+(?=\\/|$)/g, '/<ID>'); }",
                            "args": ["$__uri"],
                            "lang": "js",
                        }
                    },
                    "__message_template": {
                        "$function": {
                            "body": "function(message) { return String(message || '').replace(/\\b\\d{1,3}(?:\\.\\d{1,3}){3}\\b/g, '<IP>').replace(/\\b\\d+\\b/g, '<ID>').trim() || 'empty message'; }",
                            "args": ["$__message"],
                            "lang": "js",
                        }
                    },
                    "__ip_prefix": {
                        "$function": {
                            "body": "function(ip) { return ip ? String(ip).replace(/\\.\\d+$/, '.0/24') : 'unknown'; }",
                            "args": ["$__ip"],
                            "lang": "js",
                        }
                    },
                }
            },
            {
                "$set": {
                    "__cluster_key": cluster_key,
                    "__description": description,
                }
            },
            {"$match": {"__cluster_key": {"$ne": None}}},
            {
                "$group": {
                    "_id": "$__cluster_key",
                    "description": {"$first": "$__description"},
                    "size": {"$sum": 1},
                    "samples": {"$push": {"log_id": {"$toString": "$_id"}, "raw": "$raw"}},
                    "statuses": {"$push": "$__status"},
                    "methods": {"$push": "$__method"},
                    "log_types": {"$push": {"$toString": "$log_type"}},
                    "first_seen": {"$min": "$timestamp"},
                    "last_seen": {"$max": "$timestamp"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "run_id": str(run_id),
                    "cluster_key": "$_id",
                    "size": 1,
                    "description": 1,
                    "samples": {"$slice": ["$samples", 5]},
                    "stats": {
                        "status_counts": self._count_values("$statuses"),
                        "method_counts": self._count_values("$methods"),
                        "log_type_counts": self._count_values("$log_types"),
                        "first_seen": "$first_seen",
                        "last_seen": "$last_seen",
                    },
                }
            },
            {"$sort": {"size": -1, "cluster_key": 1}},
        ]

    @staticmethod
    def _count_values(array_expression: str) -> dict[str, Any]:
        return {
            "$arrayToObject": {
                "$map": {
                    "input": {"$setUnion": [array_expression, []]},
                    "as": "item",
                    "in": {
                        "k": "$$item",
                        "v": {
                            "$size": {
                                "$filter": {
                                    "input": array_expression,
                                    "as": "value",
                                    "cond": {"$eq": ["$$value", "$$item"]},
                                }
                            }
                        },
                    },
                }
            }
        }

    @staticmethod
    def _merge_counts(target: dict[str, int], source: dict[str, int]) -> None:
        for key, value in source.items():
            target[str(key)] = target.get(str(key), 0) + int(value)
