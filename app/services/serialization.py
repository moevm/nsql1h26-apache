from __future__ import annotations

from typing import Any

from app.models.cluster import Cluster
from app.models.cluster_run import ClusterRun
from fastapi.encoders import jsonable_encoder

from app.models.log import LogDocument


def serialize_log_document(document: dict[str, Any], *, omit_empty_normalized: bool = False) -> dict[str, Any]:
    payload = LogDocument.model_validate(document).model_dump(by_alias=True, exclude_none=True)

    if omit_empty_normalized:
        normalized = payload.get("normalized")
        if not normalized or not any(value is not None for value in normalized.values()):
            payload.pop("normalized", None)

    return jsonable_encoder(payload)


def serialize_cluster_run(document: dict[str, Any]) -> dict[str, Any]:
    return jsonable_encoder(ClusterRun.model_validate(document).model_dump(by_alias=True, exclude_none=True))


def serialize_cluster(document: dict[str, Any]) -> dict[str, Any]:
    return jsonable_encoder(Cluster.model_validate(document).model_dump(by_alias=True, exclude_none=True))
