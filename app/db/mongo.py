from __future__ import annotations

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from app.config import settings


def get_client() -> MongoClient:
    return MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=2000)


def get_database():
    return get_client()[settings.mongo_db_name]


def ping() -> bool:
    try:
        client = get_client()
        client.admin.command("ping")
        return True
    except PyMongoError:
        return False

