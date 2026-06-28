from __future__ import annotations

from abc import ABC, abstractmethod

from src.domain.entities.transcription import TranscriptionEvent


class TranscriptionEventPublisher(ABC):
    @abstractmethod
    async def publish(self, event: TranscriptionEvent) -> str: ...

    @abstractmethod
    async def publish_partial(
        self,
        session_id: str,
        user_id: str,
        language: str,
        text: str,
    ) -> str: ...

    @abstractmethod
    async def publish_final(
        self,
        session_id: str,
        user_id: str,
        language: str,
        text: str,
    ) -> str: ...
