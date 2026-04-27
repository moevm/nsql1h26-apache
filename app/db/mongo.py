from __future__ import annotations

from functools import lru_cache

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database

from app.config import settings
from app.services.seed_service import SeedService


@lru_cache(maxsize=1)
def get_client() -> MongoClient:
    return MongoClient(settings.mongodb_url, serverSelectionTimeoutMS=5000, tz_aware=True)


@lru_cache(maxsize=1)
def get_database() -> Database:
    return get_client()[settings.db_name]


def init_database() -> None:
    db = get_database()
    logs = db["logs"]
    logs.create_index([("log_type", ASCENDING), ("timestamp", DESCENDING)], name="idx_logs_type_timestamp")
    logs.create_index([("parsed.status", ASCENDING)], name="idx_logs_status")
    logs.create_index([("parsed.method", ASCENDING)], name="idx_logs_method")
    logs.create_index([("import_batch_id", ASCENDING)], name="idx_logs_import_batch")
    SeedService(db).seed_if_empty()


def ping() -> bool:
    get_client().admin.command("ping")
    return True


def close_mongo_connection() -> None:
    get_client().close()
    get_client.cache_clear()
    get_database.cache_clear()
