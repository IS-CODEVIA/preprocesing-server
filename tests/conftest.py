from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from src.domain.entities.transcription import Transcription, TranscriptionStatus
from src.domain.repositories.session_repository import SessionRepository
from src.domain.services.event_publisher import TranscriptionEventPublisher
from src.domain.services.transcriber import Transcriber


@pytest.fixture
def mock_transcriber() -> Transcriber:
    mock = MagicMock(spec=Transcriber)
    mock.transcribe_partial = AsyncMock(return_value="test partial text")
    mock.transcribe_final = AsyncMock(return_value="test final text")
    mock.load_model = AsyncMock()
    mock.unload_model = AsyncMock()
    return mock


@pytest.fixture
def mock_event_publisher() -> TranscriptionEventPublisher:
    mock = MagicMock(spec=TranscriptionEventPublisher)
    mock.publish = AsyncMock(return_value="mock-message-id")
    mock.publish_partial = AsyncMock(return_value="mock-message-id")
    mock.publish_final = AsyncMock(return_value="mock-message-id")
    return mock


@pytest.fixture
def mock_session_repository() -> SessionRepository:
    mock = MagicMock(spec=SessionRepository)
    mock.create_session = AsyncMock()
    mock.get_session = AsyncMock(return_value=None)
    mock.remove_session = AsyncMock()
    mock.get_active_count = AsyncMock(return_value=0)
    mock.get_all_active_sessions = AsyncMock(return_value=[])
    return mock


@pytest.fixture
def sample_transcription() -> Transcription:
    return Transcription(
        session_id="test-session-123",
        user_id="test-user-456",
        status=TranscriptionStatus.STREAMING,
    )


@pytest.fixture
def sample_audio_chunk() -> bytes:
    return b"\x00\x01\x02\x03" * 4096
