from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class TranscriptionStatus(Enum):
    PENDING = "pending"
    STREAMING = "streaming"
    FINALIZED = "finalized"
    FAILED = "failed"
    TIMEOUT = "timeout"


class EventType(Enum):
    PARTIAL = "partial"
    FINAL = "final"


@dataclass
class Transcription:
    session_id: str
    user_id: str
    status: TranscriptionStatus = TranscriptionStatus.PENDING
    language: str = "es"
    chunks: list[bytes] = field(default_factory=list)
    partial_texts: list[str] = field(default_factory=list)
    final_text: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_partial_at: float = 0.0
    _total_bytes: int = 0

    def add_chunk(self, chunk: bytes) -> None:
        self.chunks.append(chunk)
        self._total_bytes += len(chunk)
        self.updated_at = datetime.now(timezone.utc)

    def get_audio_buffer(self) -> bytes:
        return b"".join(self.chunks)

    def total_bytes(self) -> int:
        return self._total_bytes

    def chunk_count(self) -> int:
        return len(self.chunks)

    def mark_streaming(self) -> None:
        self.status = TranscriptionStatus.STREAMING
        self.updated_at = datetime.now(timezone.utc)

    def mark_finalized(self, text: str) -> None:
        self.status = TranscriptionStatus.FINALIZED
        self.final_text = text
        self.updated_at = datetime.now(timezone.utc)

    def mark_failed(self) -> None:
        self.status = TranscriptionStatus.FAILED
        self.updated_at = datetime.now(timezone.utc)

    def add_partial(self, text: str) -> None:
        self.partial_texts.append(text)
        self.last_partial_at = datetime.now(timezone.utc).timestamp()
        self.updated_at = datetime.now(timezone.utc)


@dataclass
class TranscriptionEvent:
    event_id: str
    session_id: str
    user_id: str
    timestamp: str
    language: str
    event_type: EventType
    text: str

    @classmethod
    def create(
        cls,
        session_id: str,
        user_id: str,
        language: str,
        event_type: EventType,
        text: str,
    ) -> TranscriptionEvent:
        return cls(
            event_id=str(uuid.uuid4()),
            session_id=session_id,
            user_id=user_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            language=language,
            event_type=event_type,
            text=text,
        )
