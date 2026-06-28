from __future__ import annotations

import time
from typing import Optional

import structlog

from src.domain.entities.transcription import TranscriptionStatus
from src.domain.repositories.session_repository import SessionRepository
from src.domain.services.transcriber import Transcriber
from src.domain.services.event_publisher import TranscriptionEventPublisher
from src.domain.services.quality_validator import TranscriptQualityValidator
from src.application.dto.transcription_dto import FinalTranscriptionResult

logger = structlog.get_logger()


class FinalizeTranscriptionUseCase:
    def __init__(
        self,
        session_repository: SessionRepository,
        transcriber: Transcriber,
        event_publisher: TranscriptionEventPublisher,
        kafka_publisher: Optional[TranscriptionEventPublisher] = None,
    ) -> None:
        self._session_repository = session_repository
        self._transcriber = transcriber
        self._event_publisher = event_publisher
        self._kafka_publisher = kafka_publisher

    async def execute(self, session_id: str) -> FinalTranscriptionResult:
        session = await self._session_repository.get_session(session_id)
        if session is None:
            raise SessionNotFoundError(f"Session not found: {session_id}")

        if session.status == TranscriptionStatus.FINALIZED:
            raise SessionAlreadyFinalizedError(f"Session already finalized: {session_id}")

        start_time = time.monotonic()
        audio_buffer = await self._session_repository.get_audio_buffer(session_id)

        text = await self._transcriber.transcribe_final(audio_buffer, session.language)

        latency_ms = (time.monotonic() - start_time) * 1000
        session.mark_finalized(text)

        quality = TranscriptQualityValidator.validate(
            text=text,
            detected_language=session.language,
            expected_language=session.language,
        )

        event_id = await self._event_publisher.publish_final(
            session_id=session.session_id,
            user_id=session.user_id,
            language=session.language,
            text=text,
        )
        if self._kafka_publisher:
            await self._kafka_publisher.publish_final(
                session_id=session.session_id,
                user_id=session.user_id,
                language=session.language,
                text=text,
            )

        await logger.ainfo(
            "transcription_finalized",
            session_id=session_id,
            latency_ms=round(latency_ms, 2),
            text_length=len(text),
            text=text,
            quality_score=round(quality.overall_score, 2),
            quality_passed=quality.passed,
            event_id=event_id,
        )

        await self._session_repository.remove_session(session_id)

        return FinalTranscriptionResult(
            session_id=session.session_id,
            text=text,
            language=session.language,
            latency_ms=latency_ms,
            quality_score=quality.overall_score,
            quality_warnings=quality.warnings,
            quality_passed=quality.passed,
        )


class SessionNotFoundError(Exception):
    pass


class SessionAlreadyFinalizedError(Exception):
    pass
