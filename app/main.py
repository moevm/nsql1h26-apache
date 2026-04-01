from __future__ import annotations

from fastapi import FastAPI


app = FastAPI(
    title="Apache Logs Prototype",
    version="0.5.0-dev",
)


@app.get("/")
def root():
    return {
        "app": "apache-logs-prototype",
        "status": "bootstrapped",
    }
