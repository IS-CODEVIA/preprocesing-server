from __future__ import annotations

import time
from typing import Any

import structlog
from fastapi import APIRouter, Response

from src.infrastructure.config.settings import settings

logger = structlog.get_logger()

router = APIRouter()

_start_time: float = time.time()
_ready: bool = False


def set_ready(ready: bool) -> None:
    global _ready
    _ready = ready


@router.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": settings.SERVICE_NAME,
        "version": "1.0.0",
        "uptime_seconds": round(time.time() - _start_time, 2),
        "timestamp": time.time(),
    }


@router.get("/ready")
async def readiness() -> Response:
    if not _ready:
        return Response(
            status_code=503,
            content='{"status":"not_ready","service":"' + settings.SERVICE_NAME + '"}',
            media_type="application/json",
        )

    return Response(
        status_code=200,
        content='{"status":"ready","service":"' + settings.SERVICE_NAME + '"}',
        media_type="application/json",
    )


@router.get("/live")
async def liveness() -> dict[str, str]:
    return {"status": "alive"}
