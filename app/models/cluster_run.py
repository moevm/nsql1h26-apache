from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import Field

from app.models.common import MongoBaseModel, MongoIdMixin


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
