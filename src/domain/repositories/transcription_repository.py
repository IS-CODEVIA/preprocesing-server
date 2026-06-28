from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from src.domain.entities.transcription import Transcription, TranscriptionStatus


class TranscriptionRepository(ABC):
    @abstractmethod
    async def save(self, transcription: Transcription) -> None: ...



    @abstractmethod
    async def find_by_session_id(self, session_id: str) -> Optional[Transcription]: ...

    @abstractmethod
    async def delete(self, session_id: str) -> None: ...

    @abstractmethod
    async def exists(self, session_id: str) -> bool: ...

    @abstractmethod
    async def count_active(self) -> int: ...

    @abstractmethod
    async def update_status(self, session_id: str, status: TranscriptionStatus) -> None: ...

    @abstractmethod
    async def find_expired(self, timeout_seconds: int) -> list[Transcription]: ...

    @abstractmethod
    async def cleanup_session(self, session_id: str) -> None: ...
