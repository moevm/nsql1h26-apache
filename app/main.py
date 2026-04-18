from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db.mongo import close_mongo_connection, init_database
from app.routes.api import router as api_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_database()
    yield
    close_mongo_connection()


app = FastAPI(
    title=settings.app_name,
    version="0.5.0",
    lifespan=lifespan,
)

app.include_router(api_router, prefix=settings.api_prefix)
app.mount("/", StaticFiles(directory="app/static", html=True), name="static")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": exc.errors()},
    )


@app.get("/")
def root():
    return {
        "app": "apache-logs-prototype",
        "status": "ok",
        "docs": "/docs",
        "api": settings.api_prefix,
    }
