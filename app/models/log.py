from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Any

from pydantic import Field, field_validator

from app.models.common import LogType, MongoBaseModel, MongoIdMixin, PaginationParams


class LogSource(MongoBaseModel):
    file_name: str
    uploaded_at: datetime


class LogNormalized(MongoBaseModel):
    uri_template: str | None = None
    message_template: str | None = None
    signature: str | None = None
    ip_prefix: str | None = None


class AccessParsed(MongoBaseModel):
    remote_addr: str | None = None
    remote_user: str | None = None
    time_local: datetime | None = None
    request_method: str | None = None
    request_path: str | None = None
    request_protocol: str | None = None
    status: int | None = None
    body_bytes_sent: int | None = None
    http_referer: str | None = None
    http_user_agent: str | None = None

    ip: str | None = None
    method: str | None = None
    uri: str | None = None
    user_agent: str | None = None
    referer: str | None = None


class ErrorParsed(MongoBaseModel):
    timestamp: datetime | None = None
    level: str | None = None
    message: str | None = None
    client: str | None = None
    pid: int | None = None
    tid: int | None = None


class LogDocument(MongoIdMixin):
    import_batch_id: str | None = None
    log_type: LogType
    source: LogSource
    raw: str
    timestamp: datetime | None = None
    parsed: dict[str, Any] = Field(default_factory=dict)
    normalized: LogNormalized | None = None
    parse_error: bool = False

    @field_validator("import_batch_id", mode="before")
    @classmethod
    def normalize_import_batch_id(cls, value: Any) -> str | None:
        if value is None:
            return None
        return str(value)


class LogsListItem(LogDocument):
    pass


class LogsListResponse(MongoBaseModel):
    total: int
    items: list[LogsListItem]


class ImportResponse(MongoBaseModel):
    success: bool = True
    total: int
    inserted: int
    errors: int
    access: int = 0
    error: int = 0


class LogsFilterParams(PaginationParams):
    type: LogType | None = None
    from_date: datetime | None = None
    to_date: datetime | None = None
    status: int | None = None
    method: str | None = None
    search: str | None = None

    @field_validator("from_date", mode="before")
    @classmethod
    def validate_from_date(cls, value: Any) -> Any:
        return _parse_date_like(value, end_of_day=False)

    @field_validator("to_date", mode="before")
    @classmethod
    def validate_to_date(cls, value: Any) -> Any:
        return _parse_date_like(value, end_of_day=True)


class ExportQueryParams(PaginationParams):
    type: LogType | None = None
    limit: int = Field(default=1000, ge=1, le=10000)


def _parse_date_like(value: Any, *, end_of_day: bool) -> Any:
    if value is None:
        return value
    if isinstance(value, str):
        candidate = value.strip()
        if len(candidate) == 10:
            parsed_date = date.fromisoformat(candidate)
            moment = time.max if end_of_day else time.min
            return datetime.combine(parsed_date, moment, tzinfo=timezone.utc)
        value = candidate
    if isinstance(value, date) and not isinstance(value, datetime):
        moment = time.max if end_of_day else time.min
        return datetime.combine(value, moment, tzinfo=timezone.utc)
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value
