from __future__ import annotations

from typing import Optional

import structlog

from src.domain.entities.transcription import Transcription, TranscriptionStatus
from src.domain.repositories.session_repository import SessionRepository

logger = structlog.get_logger()


class InMemorySessionRepository(SessionRepository):
    def __init__(self) -> None:
        self._sessions: dict[str, Transcription] = {}

    async def create_session(self, transcription: Transcription) -> None:
        self._sessions[transcription.session_id] = transcription

    async def get_session(self, session_id: str) -> Optional[Transcription]:
        return self._sessions.get(session_id)

    async def update_session(self, session: Transcription) -> None:
        self._sessions[session.session_id] = session

    async def append_audio_chunk(self, session_id: str, chunk: bytes) -> None:
        session = self._sessions.get(session_id)
        if session:
            session.add_chunk(chunk)

    async def get_audio_buffer(self, session_id: str) -> bytes:
        session = self._sessions.get(session_id)
        if session:
            return session.get_audio_buffer()
        return b""

    async def remove_session(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)

    async def get_active_count(self) -> int:
        return len(self._sessions)

    async def get_all_active_sessions(self) -> list[Transcription]:
        return list(self._sessions.values())
