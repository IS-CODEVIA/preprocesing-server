from __future__ import annotations

import pytest
from httpx import AsyncClient, ASGITransport
from fastapi import FastAPI

from src.presentation.health.health_routes import router, set_ready


@pytest.fixture
def app() -> FastAPI:
    application = FastAPI()
    application.include_router(router)
    return application


class TestHealthRoutes:
    async def test_health_endpoint(self, app: FastAPI) -> None:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/health")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "ok"
            assert data["service"] == "speech-service"
            assert "uptime_seconds" in data

    async def test_readiness_not_ready(self, app: FastAPI) -> None:
        set_ready(False)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/ready")
            assert response.status_code == 503
            assert response.json()["status"] == "not_ready"

    async def test_readiness_ready(self, app: FastAPI) -> None:
        set_ready(True)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/ready")
            assert response.status_code == 200
            assert response.json()["status"] == "ready"

    async def test_liveness(self, app: FastAPI) -> None:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/live")
            assert response.status_code == 200
            assert response.json()["status"] == "alive"
