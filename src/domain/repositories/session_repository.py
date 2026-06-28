from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from src.domain.entities.transcription import Transcription


class SessionRepository(ABC):
    @abstractmethod
    async def create_session(self, transcription: Transcription) -> None: ...

    @abstractmethod
    async def get_session(self, session_id: str) -> Optional[Transcription]: ...

    @abstractmethod
    async def update_session(self, session: Transcription) -> None: ...

    @abstractmethod
    async def append_audio_chunk(self, session_id: str, chunk: bytes) -> None: ...

    @abstractmethod
    async def get_audio_buffer(self, session_id: str) -> bytes: ...

    @abstractmethod
    async def remove_session(self, session_id: str) -> None: ...

    @abstractmethod
    async def get_active_count(self) -> int: ...

    @abstractmethod
    async def get_all_active_sessions(self) -> list[Transcription]: ...
