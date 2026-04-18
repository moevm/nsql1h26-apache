from __future__ import annotations

from typing import Annotated

from bson import ObjectId
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from pydantic import ValidationError
from pymongo.database import Database

from app.db.mongo import get_database
from app.models.common import ImportMode, LogType, RawLogResponse, SuccessResponse
from app.models.log import ExportQueryParams, ImportResponse, LogDocument, LogsFilterParams, LogsListResponse
from app.repositories.logs import LogsRepository
from app.services.export_service import ExportService
from app.services.import_service import ImportService
from app.services.serialization import serialize_log_document

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


def get_export_params(
    type: LogType | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
) -> ExportQueryParams:
    return _build_model(ExportQueryParams, type=type, limit=limit, offset=offset)


@router.post("/import", response_model=ImportResponse)
def import_logs(
    file: UploadFile = File(...),
    type: LogType = Form(...),
    mode: ImportMode = Form(...),
    db: Database = Depends(get_db),
):
    service = ImportService(db)
    return service.import_logs(file, type, mode)


@router.get("/export", response_model=list[LogDocument], response_model_exclude_none=True)
def export_logs(
    params: Annotated[ExportQueryParams, Depends(get_export_params)],
    db: Database = Depends(get_db),
):
    service = ExportService(db)
    return service.export_logs(params)


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
    _validate_log_id(log_id)
    repository = LogsRepository(db)
    document = repository.get_by_id(log_id)
    if document is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")
    return serialize_log_document(document)


@router.get("/logs/{log_id}/raw", response_model=RawLogResponse)
def get_raw_log(log_id: str, db: Database = Depends(get_db)) -> RawLogResponse:
    _validate_log_id(log_id)
    repository = LogsRepository(db)
    document = repository.get_by_id(log_id)
    if document is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")
    return RawLogResponse(raw=document["raw"])


@router.get("/health", response_model=SuccessResponse)
def healthcheck() -> SuccessResponse:
    return SuccessResponse(success=True)


def _validate_log_id(log_id: str) -> None:
    if not ObjectId.is_valid(log_id):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid log_id")


def _build_model(model_cls, **payload):
    try:
        return model_cls(**payload)
    except ValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=exc.errors()) from exc
