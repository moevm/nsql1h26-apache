from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import Field, field_validator

from app.models.common import MongoBaseModel, MongoIdMixin
from app.models.log import LogsFilterParams


class ClusterRunSummary(MongoBaseModel):
    logs_total: int = 0
    clusters_total: int = 0
    clustered_logs_total: int = 0
    unclustered_logs_total: int = 0


class ClusterRun(MongoIdMixin):
    created_at: datetime
    method: str
    filters: dict[str, Any] = Field(default_factory=dict)
    summary: ClusterRunSummary = Field(default_factory=ClusterRunSummary)
    preset_name: str | None = None
    status: str | None = None


class ClusterRunCreateRequest(MongoBaseModel):
    method: str = "rule_based"
    filters: dict[str, Any] = Field(default_factory=dict)
    preset_name: str | None = None

    @field_validator("method")
    @classmethod
    def normalize_method(cls, value: str) -> str:
        candidate = value.strip()
        return candidate or "rule_based"


class ClusterRunListResponse(MongoBaseModel):
    total: int
    items: list[ClusterRun]


class ClusterRunStatsResponse(MongoBaseModel):
    run: ClusterRun
    top_clusters: list[dict[str, Any]] = Field(default_factory=list)
    status_counts: dict[str, int] = Field(default_factory=dict)
    method_counts: dict[str, int] = Field(default_factory=dict)
    log_type_counts: dict[str, int] = Field(default_factory=dict)


def build_logs_filter_params(payload: dict[str, Any]) -> LogsFilterParams:
    allowed = {"type", "from_date", "to_date", "status", "method", "search"}
    filtered = {key: value for key, value in payload.items() if key in allowed and value not in (None, "", "all")}
    filtered["limit"] = 5000
    filtered["offset"] = 0
    return LogsFilterParams(**filtered)
