from __future__ import annotations

import time
import structlog
from typing import Optional

from src.domain.entities.transcription import Transcription, TranscriptionStatus
from src.domain.repositories.session_repository import SessionRepository
from src.domain.services.transcriber import Transcriber
from src.domain.services.event_publisher import TranscriptionEventPublisher
from src.application.dto.transcription_dto import AudioChunkDTO, PartialTranscriptionResult

logger = structlog.get_logger()


class ProcessAudioChunkUseCase:
    def __init__(
        self,
        session_repository: SessionRepository,
        transcriber: Transcriber,
        event_publisher: TranscriptionEventPublisher,
        chunk_interval_ms: int = 500,
        max_buffer_bytes: int = 50 * 1024 * 1024,
        max_chunk_size_bytes: int = 131072,
    ) -> None:
        self._session_repository = session_repository
        self._transcriber = transcriber
        self._event_publisher = event_publisher
        self._chunk_interval_ms = chunk_interval_ms
        self._max_buffer_bytes = max_buffer_bytes
        self._max_chunk_size_bytes = max_chunk_size_bytes

    async def execute(self, dto: AudioChunkDTO) -> Optional[PartialTranscriptionResult]:
        session = await self._session_repository.get_session(dto.session_id)
        if session is None:
            raise SessionNotFoundError(f"Session not found: {dto.session_id}")

        if session.status != TranscriptionStatus.STREAMING:
            raise SessionNotActiveError(
                f"Session is not active: {dto.session_id}, status: {session.status.value}"
            )

        if len(dto.chunk) > self._max_chunk_size_bytes:
            raise ChunkTooLargeError(
                f"Chunk size {len(dto.chunk)} exceeds maximum {self._max_chunk_size_bytes}"
            )

        if session.total_bytes() + len(dto.chunk) > self._max_buffer_bytes:
            raise BufferLimitError(
                f"Buffer limit exceeded for session: {dto.session_id}"
            )

        await self._session_repository.append_audio_chunk(dto.session_id, dto.chunk)
        session.add_chunk(dto.chunk)

        await logger.ainfo(
            "audio_chunk_received",
            session_id=dto.session_id,
            size_bytes=len(dto.chunk),
            total_bytes=session.total_bytes(),
            chunk_count=session.chunk_count(),
        )

        should_transcribe = self._should_run_partial(session)
        if not should_transcribe:
            return None

        start_time = time.monotonic()
        audio_buffer = await self._session_repository.get_audio_buffer(dto.session_id)
        text = await self._transcriber.transcribe_partial(audio_buffer, session.language)

        latency_ms = (time.monotonic() - start_time) * 1000
        session.add_partial(text)
        await self._session_repository.update_session(session)

        await self._event_publisher.publish_partial(
            session_id=session.session_id,
            user_id=session.user_id,
            language=session.language,
            text=text,
        )

        await logger.ainfo(
            "transcription_partial",
            session_id=dto.session_id,
            latency_ms=round(latency_ms, 2),
            text_length=len(text),
            text=text,
        )

        return PartialTranscriptionResult(
            session_id=session.session_id,
            text=text,
            is_partial=True,
            latency_ms=latency_ms,
        )

    def _should_run_partial(self, session: Transcription) -> bool:
        if session.last_partial_at == 0.0:
            return True
        elapsed_ms = (time.time() - session.last_partial_at) * 1000
        return elapsed_ms >= self._chunk_interval_ms


class SessionNotFoundError(Exception):
    pass


class SessionNotActiveError(Exception):
    pass


class ChunkTooLargeError(Exception):
    pass


class BufferLimitError(Exception):
    pass
