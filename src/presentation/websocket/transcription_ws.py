from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import structlog
from fastapi import WebSocket, status

from src.application.dto.transcription_dto import AudioChunkDTO, StartSessionDTO
from src.application.use_cases.finalize_transcription import (
    FinalizeTranscriptionUseCase,
    SessionNotFoundError as FinalizeNotFound,
    SessionAlreadyFinalizedError,
)
from src.application.use_cases.process_audio_chunk import (
    ChunkTooLargeError,
    ProcessAudioChunkUseCase,
    SessionNotFoundError as ProcessNotFound,
    SessionNotActiveError,
    BufferLimitError,
)
from src.application.use_cases.start_session import (
    SessionLimitError,
    SessionAlreadyExistsError,
    StartSessionUseCase,
)
from src.domain.repositories.session_repository import SessionRepository
from src.infrastructure.config.settings import settings
from src.infrastructure.websocket.connection_manager import ConnectionManager

logger = structlog.get_logger()


class TranscriptionWebSocketHandler:
    def __init__(
        self,
        start_session_uc: StartSessionUseCase,
        process_chunk_uc: ProcessAudioChunkUseCase,
        finalize_uc: FinalizeTranscriptionUseCase,
        connection_manager: ConnectionManager,
        session_repository: SessionRepository,
    ) -> None:
        self._start_session_uc = start_session_uc
        self._process_chunk_uc = process_chunk_uc
        self._finalize_uc = finalize_uc
        self._connection_manager = connection_manager
        self._session_repository = session_repository

    async def handle(self, websocket: WebSocket) -> None:
        await websocket.accept()

        session_id: str | None = None
        user_id: str | None = None
        heartbeat_task: asyncio.Task | None = None

        try:
            message = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            start_data = json.loads(message)

            if not isinstance(start_data, dict) or start_data.get("type") != "start":
                raise ValueError("First message must be a JSON object with type='start'")

            session_id = start_data.get("session_id", "").strip()
            user_id = str(start_data.get("user_id", "")).strip()
            language = start_data.get("language", "es")

            if not session_id:
                raise ValueError("session_id is required")
            if not user_id:
                raise ValueError("user_id is required")
            if language not in ("es", "en", "fr", "de", "pt", "it"):
                raise ValueError(f"Unsupported language: {language}")

            await self._start_session_uc.execute(
                StartSessionDTO(session_id=session_id, user_id=user_id, language=language)
            )

            await self._connection_manager.add(websocket, session_id, user_id)

            heartbeat_task = asyncio.create_task(self._heartbeat_loop(websocket, session_id))

            await logger.ainfo("session_established", session_id=session_id, user_id=user_id)

            await self._process_audio_stream(websocket, session_id)

        except asyncio.TimeoutError:
            await logger.awarning("websocket_timeout_no_start_message")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="No start message within 30s")
        except (json.JSONDecodeError, ValueError) as exc:
            await logger.awarning("invalid_start_message", error=str(exc))
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=str(exc))
        except SessionLimitError:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Server at capacity")
        except SessionAlreadyExistsError:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Session already exists")
        except Exception as exc:
            await logger.aerror("websocket_handler_error", session_id=session_id, error=str(exc), exc_info=True)
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
            if session_id is not None:
                await self._connection_manager.remove(session_id)
                await self._session_repository.remove_session(session_id)

    async def _process_audio_stream(self, websocket: WebSocket, session_id: str) -> None:
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive(), timeout=settings.SESSION_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                await logger.awarning("session_timeout", session_id=session_id)
                await self._connection_manager.send_to_session(session_id, {
                    "type": "error", "code": "timeout", "message": "Session timed out",
                })
                break

            if raw.get("type") == "websocket.disconnect":
                break

            content = raw.get("text") or raw.get("bytes")

            if isinstance(content, str):
                try:
                    msg = json.loads(content)
                except json.JSONDecodeError:
                    continue

                if msg.get("type") == "stop":
                    await self._handle_stop(session_id)
                    break
                elif msg.get("type") == "ping":
                    await self._connection_manager.send_to_session(session_id, {"type": "pong"})
                    continue
                else:
                    continue

            elif isinstance(content, bytes):
                dto = AudioChunkDTO(session_id=session_id, chunk=content, timestamp=time.time())

                try:
                    result = await self._process_chunk_uc.execute(dto)
                    if result is not None:
                        await self._connection_manager.send_to_session(session_id, {
                            "type": "partial_transcription",
                            "session_id": session_id,
                            "text": result.text,
                            "latency_ms": round(result.latency_ms, 2),
                        })
                except (ChunkTooLargeError, BufferLimitError) as exc:
                    await logger.awarning("chunk_error", session_id=session_id, error=str(exc))
                    await self._connection_manager.send_to_session(session_id, {
                        "type": "error", "code": type(exc).__name__, "message": str(exc),
                    })
                except (ProcessNotFound, SessionNotActiveError):
                    break
            else:
                continue

    async def _handle_stop(self, session_id: str) -> None:
        try:
            result = await self._finalize_uc.execute(session_id)
            await self._connection_manager.send_to_session(session_id, {
                "type": "final_transcription",
                "session_id": session_id,
                "text": result.text,
                "latency_ms": round(result.latency_ms, 2),
                "quality": {
                    "score": round(result.quality_score, 2),
                    "warnings": result.quality_warnings,
                    "passed": result.quality_passed,
                },
            })
            await logger.ainfo("transcription_completed", session_id=session_id, latency_ms=round(result.latency_ms, 2))
        except (FinalizeNotFound, SessionAlreadyFinalizedError):
            pass

    async def _heartbeat_loop(self, websocket: WebSocket, session_id: str) -> None:
        try:
            while True:
                await asyncio.sleep(settings.HEARTBEAT_INTERVAL_SECONDS)
                try:
                    await asyncio.wait_for(websocket.send_json({"type": "heartbeat"}), timeout=5.0)
                except Exception:
                    break
        except asyncio.CancelledError:
            pass
