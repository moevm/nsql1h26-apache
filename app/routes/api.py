from __future__ import annotations

from typing import Annotated

from bson import ObjectId
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from pydantic import ValidationError
from pymongo.database import Database

from app.db.mongo import get_database
from app.models.cluster import Cluster, ClusterListResponse
from app.models.cluster_run import ClusterRun, ClusterRunCreateRequest, ClusterRunListResponse
from app.models.common import ImportMode, LogType, RawLogResponse, SuccessResponse
from app.models.log import ImportResponse, LogDocument, LogsFilterParams, LogsListResponse
from app.repositories.cluster_runs import ClusterRunsRepository
from app.repositories.clusters import ClustersRepository
from app.repositories.logs import LogsRepository
from app.services.clustering_service import ClusteringService
from app.services.export_service import ExportService
from app.services.import_service import ImportService
from app.services.serialization import serialize_cluster, serialize_cluster_run, serialize_log_document

router = APIRouter(tags=["logs"])


def get_db() -> Database:
    return get_database()


def get_logs_filters(
    type: LogType | None = Query(default=None),
    from_date: str | None = Query(default=None),
    to_date: str | None = Query(default=None),
    status: int | None = Query(default=None),
    method: str | None = Query(default=None),
    search: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> LogsFilterParams:
    return _build_model(
        LogsFilterParams,
        type=type,
        from_date=from_date,
        to_date=to_date,
        status=status,
        method=method,
        search=search,
        limit=limit,
        offset=offset,
    )


@router.post("/import", response_model=ImportResponse)
def import_logs(
    file: UploadFile = File(...),
    type: LogType | None = Form(default=None),
    mode: ImportMode = Form(...),
    db: Database = Depends(get_db),
):
    service = ImportService(db)
    try:
        return service.import_logs(file, type, mode)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/export")
def export_application(db: Database = Depends(get_db)):
    service = ExportService(db)
    return service.export_application()


@router.post("/cluster-runs", response_model=ClusterRun)
def create_cluster_run(payload: ClusterRunCreateRequest | None = None, db: Database = Depends(get_db)):
    service = ClusteringService(db)
    try:
        document = service.run(payload or ClusterRunCreateRequest())
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return serialize_cluster_run(document)


@router.get("/cluster-runs", response_model=ClusterRunListResponse)
def list_cluster_runs(
    method: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Database = Depends(get_db),
):
    repository = ClusterRunsRepository(db)
    total = repository.count(method=method, status=status_filter)
    items = [
        serialize_cluster_run(document)
        for document in repository.list(method=method, status=status_filter, limit=limit, offset=offset)
    ]
    return {"total": total, "items": items}


@router.get("/cluster-runs/{run_id}", response_model=ClusterRun)
def get_cluster_run(run_id: str, db: Database = Depends(get_db)):
    _validate_object_id(run_id, "run_id")
    repository = ClusterRunsRepository(db)
    document = repository.get_by_id(run_id)
    if document is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cluster run not found")
    return serialize_cluster_run(document)


@router.get("/cluster-runs/{run_id}/clusters", response_model=ClusterListResponse)
def list_clusters_by_run(
    run_id: str,
    search: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Database = Depends(get_db),
):
    _validate_object_id(run_id, "run_id")
    runs_repository = ClusterRunsRepository(db)
    if runs_repository.get_by_id(run_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cluster run not found")

    repository = ClustersRepository(db)
    total = repository.count_by_run(run_id, search=search)
    items = [
        serialize_cluster(document)
        for document in repository.list_by_run(run_id, search=search, limit=limit, offset=offset)
    ]
    return {"total": total, "items": items}


@router.get("/cluster-runs/{run_id}/stats")
def get_cluster_run_stats(run_id: str, db: Database = Depends(get_db)):
    _validate_object_id(run_id, "run_id")
    service = ClusteringService(db)
    try:
        stats = service.stats(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return {
        "run": serialize_cluster_run(stats["run"]),
        "top_clusters": [serialize_cluster(document) for document in stats["top_clusters"]],
        "status_counts": stats["status_counts"],
        "method_counts": stats["method_counts"],
        "log_type_counts": stats["log_type_counts"],
    }


@router.get("/clusters/{cluster_id}", response_model=Cluster)
def get_cluster(cluster_id: str, db: Database = Depends(get_db)):
    _validate_object_id(cluster_id, "cluster_id")
    repository = ClustersRepository(db)
    document = repository.get_by_id(cluster_id)
    if document is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cluster not found")
    return serialize_cluster(document)


@router.get("/logs", response_model=LogsListResponse)
def list_logs(
    params: Annotated[LogsFilterParams, Depends(get_logs_filters)],
    db: Database = Depends(get_db),
):
    repository = LogsRepository(db)
    total = repository.count_by_filters(params)
    items = [serialize_log_document(document) for document in repository.find_logs(params)]
    return {"total": total, "items": items}


@router.get("/logs/{log_id}", response_model=LogDocument)
def get_log(log_id: str, db: Database = Depends(get_db)):
    _validate_object_id(log_id, "log_id")
    repository = LogsRepository(db)
    document = repository.get_by_id(log_id)
    if document is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")
    return serialize_log_document(document)


@router.get("/logs/{log_id}/raw", response_model=RawLogResponse)
def get_raw_log(log_id: str, db: Database = Depends(get_db)) -> RawLogResponse:
    _validate_object_id(log_id, "log_id")
    repository = LogsRepository(db)
    document = repository.get_by_id(log_id)
    if document is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")
    return RawLogResponse(raw=document["raw"])


@router.get("/health", response_model=SuccessResponse)
def healthcheck() -> SuccessResponse:
    return SuccessResponse(success=True)


def _validate_object_id(value: str, field_name: str) -> None:
    if not ObjectId.is_valid(value):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid {field_name}")


def _build_model(model_cls, **payload):
    try:
        return model_cls(**payload)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=exc.errors()) from exc
