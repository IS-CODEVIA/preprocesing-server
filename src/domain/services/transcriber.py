from __future__ import annotations

from abc import ABC, abstractmethod


class Transcriber(ABC):
    @abstractmethod
    async def transcribe_partial(self, audio_bytes: bytes, language: str) -> str: ...

    @abstractmethod
    async def transcribe_final(self, audio_bytes: bytes, language: str) -> str: ...

    @abstractmethod
    async def load_model(self) -> None: ...

    @abstractmethod
    async def unload_model(self) -> None: ...
