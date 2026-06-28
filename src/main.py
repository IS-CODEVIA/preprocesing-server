from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse



from src.infrastructure.config.settings import settings
from src.infrastructure.di.container import Container
from src.infrastructure.logging.setup import configure_logging
from src.presentation.health.health_routes import router as health_router, set_ready
from src.presentation.websocket.transcription_ws import TranscriptionWebSocketHandler

logger = structlog.get_logger()


class Application:
    def __init__(self) -> None:
        configure_logging()

        self.container = Container()
        self.app = FastAPI(
            title="Speech Service",
            description="Production-ready speech-to-text microservice using Faster-Whisper",
            version="1.0.0",
            docs_url="/docs" if settings.LOG_LEVEL == "DEBUG" else None,
            redoc_url=None,
            lifespan=self._lifespan,
        )

        self._register_routes()
        self._register_exception_handlers()

    def _register_routes(self) -> None:
        self.app.include_router(health_router)

        @self.app.websocket("/ws/transcribe")
        async def transcribe_websocket(websocket):
            handler = TranscriptionWebSocketHandler(
                start_session_uc=self.container.create_start_session_use_case(),
                process_chunk_uc=self.container.create_process_audio_chunk_use_case(),
                finalize_uc=self.container.create_finalize_transcription_use_case(),
                connection_manager=self.container.connection_manager,
                session_repository=self.container.session_repository,
            )
            await handler.handle(websocket)

    def _register_exception_handlers(self) -> None:
        @self.app.exception_handler(Exception)
        async def global_exception_handler(request, exc: Exception) -> JSONResponse:
            await logger.aerror("unhandled_exception", error=str(exc), exc_info=True)
            return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI) -> AsyncIterator[None]:
        await logger.ainfo(
            "service_starting",
            service=settings.SERVICE_NAME,
            whisper_model=settings.WHISPER_MODEL,
            whisper_device=settings.WHISPER_DEVICE,
        )

        await self.container.initialize()

        stale_cleaner = asyncio.create_task(self._stale_connection_cleaner())

        set_ready(True)
        await logger.ainfo("service_ready")

        yield

        set_ready(False)
        stale_cleaner.cancel()
        try:
            await stale_cleaner
        except asyncio.CancelledError:
            pass

        await self.container.shutdown()
        await logger.ainfo("service_stopped")

    async def _stale_connection_cleaner(self) -> None:
        while True:
            await asyncio.sleep(60)
            try:
                cleaned = await self.container.connection_manager.cleanup_stale(
                    timeout_seconds=settings.SESSION_TIMEOUT_SECONDS,
                )
                if cleaned > 0:
                    await logger.ainfo("stale_connections_cleaned", count=cleaned)
            except Exception as exc:
                await logger.aerror("stale_cleaner_error", error=str(exc))


def create_app() -> FastAPI:
    application = Application()
    return application.app
