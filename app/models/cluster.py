from __future__ import annotations

from typing import Any

from pydantic import Field

from app.models.common import MongoBaseModel, MongoIdMixin


class ClusterSample(MongoBaseModel):
    log_id: str
    raw: str


class Cluster(MongoIdMixin):
    run_id: str
    cluster_key: str
    size: int
    stats: dict[str, Any] = Field(default_factory=dict)
    samples: list[ClusterSample] = Field(default_factory=list)
    description: str | None = None
