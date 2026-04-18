from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from bson import ObjectId
from pydantic import BaseModel, ConfigDict, Field, field_validator


class MongoBaseModel(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        extra="ignore",
        json_encoders={ObjectId: str, datetime: lambda value: value.isoformat()},
    )


class LogType(str, Enum):
    access = "access"
    error = "error"


class ImportMode(str, Enum):
    append = "append"
    replace = "replace"


class RawLogResponse(MongoBaseModel):
    raw: str


class SuccessResponse(MongoBaseModel):
    success: bool = True


class PaginationParams(MongoBaseModel):
    limit: int = Field(default=50, ge=1, le=5000)
    offset: int = Field(default=0, ge=0)


class MongoIdMixin(MongoBaseModel):
    id: str | None = Field(default=None, alias="_id")

    @field_validator("id", mode="before")
    @classmethod
    def validate_id(cls, value: Any) -> str | None:
        if value is None:
            return None
        return str(value)


class ObjectIdString(MongoBaseModel):
    value: str

    @field_validator("value")
    @classmethod
    def validate_object_id(cls, value: str) -> str:
        if not ObjectId.is_valid(value):
            raise ValueError("Invalid ObjectId")
        return value
