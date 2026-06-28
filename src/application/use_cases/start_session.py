from __future__ import annotations

import structlog
from typing import Optional

from src.domain.entities.transcription import Transcription, TranscriptionStatus
from src.domain.repositories.session_repository import SessionRepository
from src.domain.services.event_publisher import TranscriptionEventPublisher
from src.application.dto.transcription_dto import StartSessionDTO

logger = structlog.get_logger()


class StartSessionUseCase:
    def __init__(
        self,
        session_repository: SessionRepository,
        event_publisher: TranscriptionEventPublisher,
        max_sessions: int = 100,
    ) -> None:
        self._session_repository = session_repository
        self._event_publisher = event_publisher
        self._max_sessions = max_sessions

    async def execute(self, dto: StartSessionDTO) -> Transcription:
        active_count = await self._session_repository.get_active_count()
        if active_count >= self._max_sessions:
            raise SessionLimitError(
                f"Maximum active sessions reached: {self._max_sessions}"
            )

        if await self._session_repository.get_session(dto.session_id):
            raise SessionAlreadyExistsError(
                f"Session already exists: {dto.session_id}"
            )

        transcription = Transcription(
            session_id=dto.session_id,
            user_id=dto.user_id,
            language=dto.language,
            status=TranscriptionStatus.STREAMING,
        )

        await self._session_repository.create_session(transcription)

        await logger.ainfo(
            "session_started",
            session_id=dto.session_id,
            user_id=dto.user_id,
            language=dto.language,
        )

        return transcription


class SessionLimitError(Exception):
    pass


class SessionAlreadyExistsError(Exception):
    pass
