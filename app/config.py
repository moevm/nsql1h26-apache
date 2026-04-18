from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "Apache Logs Prototype")
    app_host: str = os.getenv("APP_HOST", "0.0.0.0")
    app_port: int = int(os.getenv("APP_PORT", "8000"))
    mongodb_url: str = os.getenv("MONGODB_URL", os.getenv("MONGO_URI", "mongodb://db:27017"))
    db_name: str = os.getenv("DB_NAME", os.getenv("MONGO_DB_NAME", "log_clustering"))
    api_prefix: str = os.getenv("API_PREFIX", "/api")
    import_chunk_size: int = int(os.getenv("IMPORT_CHUNK_SIZE", "1000"))


settings = Settings()
