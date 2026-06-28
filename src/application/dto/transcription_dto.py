from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class StartSessionDTO:
    session_id: str
    user_id: str
    language: str = "es"


@dataclass
class AudioChunkDTO:
    session_id: str
    chunk: bytes
    timestamp: float = 0.0


@dataclass
class PartialTranscriptionResult:
    session_id: str
    text: str
    is_partial: bool = True
    latency_ms: float = 0.0


@dataclass
class FinalTranscriptionResult:
    session_id: str
    text: str
    language: str = "es"
    latency_ms: float = 0.0
    quality_score: float = 0.0
    quality_warnings: list[str] = field(default_factory=list)
    quality_passed: bool = True


@dataclass
class TranscriptionResponse:
    type: str
    session_id: str
    text: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
