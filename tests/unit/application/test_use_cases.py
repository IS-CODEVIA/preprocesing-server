from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.application.dto.transcription_dto import AudioChunkDTO, StartSessionDTO
from src.application.use_cases.finalize_transcription import (
    FinalizeTranscriptionUseCase,
    SessionNotFoundError as FinalizeNotFound,
    SessionAlreadyFinalizedError,
)
from src.application.use_cases.process_audio_chunk import (
    BufferLimitError,
    ChunkTooLargeError,
    ProcessAudioChunkUseCase,
    SessionNotFoundError as ProcessNotFound,
    SessionNotActiveError,
)
from src.application.use_cases.start_session import (
    SessionAlreadyExistsError,
    SessionLimitError,
    StartSessionUseCase,
)
from src.domain.entities.transcription import Transcription, TranscriptionStatus


class TestStartSessionUseCase:
    async def test_start_session_success(self, mock_session_repository, mock_event_publisher) -> None:
        mock_session_repository.get_session = AsyncMock(return_value=None)
        mock_session_repository.get_active_count = AsyncMock(return_value=0)

        uc = StartSessionUseCase(
            session_repository=mock_session_repository,
            event_publisher=mock_event_publisher,
            max_sessions=100,
        )

        dto = StartSessionDTO(session_id="s1", user_id="u1")
        result = await uc.execute(dto)

        assert result.session_id == "s1"
        assert result.user_id == "u1"
        assert result.status == TranscriptionStatus.STREAMING
        mock_session_repository.create_session.assert_awaited_once()

    async def test_start_session_limit_reached(self, mock_session_repository, mock_event_publisher) -> None:
        mock_session_repository.get_active_count = AsyncMock(return_value=100)

        uc = StartSessionUseCase(
            session_repository=mock_session_repository,
            event_publisher=mock_event_publisher,
            max_sessions=100,
        )

        dto = StartSessionDTO(session_id="s1", user_id="u1")
        with pytest.raises(SessionLimitError):
            await uc.execute(dto)

    async def test_start_session_already_exists(self, mock_session_repository, mock_event_publisher) -> None:
        mock_session_repository.get_active_count = AsyncMock(return_value=0)
        mock_session_repository.get_session = AsyncMock(
            return_value=Transcription(session_id="s1", user_id="u1")
        )

        uc = StartSessionUseCase(
            session_repository=mock_session_repository,
            event_publisher=mock_event_publisher,
            max_sessions=100,
        )

        dto = StartSessionDTO(session_id="s1", user_id="u1")
        with pytest.raises(SessionAlreadyExistsError):
            await uc.execute(dto)


class TestProcessAudioChunkUseCase:
    async def test_process_chunk_success(self, mock_session_repository, mock_transcriber, mock_event_publisher) -> None:
        session = Transcription(session_id="s1", user_id="u1", status=TranscriptionStatus.STREAMING)
        mock_session_repository.get_session = AsyncMock(return_value=session)

        uc = ProcessAudioChunkUseCase(
            session_repository=mock_session_repository,
            transcriber=mock_transcriber,
            event_publisher=mock_event_publisher,
            chunk_interval_ms=1,
        )

        dto = AudioChunkDTO(session_id="s1", chunk=b"test audio data")
        result = await uc.execute(dto)

        assert result is not None
        assert result.session_id == "s1"
        assert result.text == "test partial text"
        assert result.is_partial is True
        mock_transcriber.transcribe_partial.assert_awaited_once()
        mock_event_publisher.publish_partial.assert_awaited_once()

    async def test_process_chunk_session_not_found(self, mock_session_repository, mock_transcriber, mock_event_publisher) -> None:
        mock_session_repository.get_session = AsyncMock(return_value=None)

        uc = ProcessAudioChunkUseCase(
            session_repository=mock_session_repository,
            transcriber=mock_transcriber,
            event_publisher=mock_event_publisher,
        )

        dto = AudioChunkDTO(session_id="nonexistent", chunk=b"data")
        with pytest.raises(ProcessNotFound):
            await uc.execute(dto)

    async def test_process_chunk_session_not_active(self, mock_session_repository, mock_transcriber, mock_event_publisher) -> None:
        session = Transcription(session_id="s1", user_id="u1", status=TranscriptionStatus.FINALIZED)
        mock_session_repository.get_session = AsyncMock(return_value=session)

        uc = ProcessAudioChunkUseCase(
            session_repository=mock_session_repository,
            transcriber=mock_transcriber,
            event_publisher=mock_event_publisher,
        )

        dto = AudioChunkDTO(session_id="s1", chunk=b"data")
        with pytest.raises(SessionNotActiveError):
            await uc.execute(dto)

    async def test_process_chunk_too_large(self, mock_session_repository, mock_transcriber, mock_event_publisher) -> None:
        session = Transcription(session_id="s1", user_id="u1", status=TranscriptionStatus.STREAMING)
        mock_session_repository.get_session = AsyncMock(return_value=session)

        uc = ProcessAudioChunkUseCase(
            session_repository=mock_session_repository,
            transcriber=mock_transcriber,
            event_publisher=mock_event_publisher,
            max_chunk_size_bytes=10,
        )

        dto = AudioChunkDTO(session_id="s1", chunk=b"x" * 100)
        with pytest.raises(ChunkTooLargeError):
            await uc.execute(dto)

    async def test_process_chunk_buffer_limit(self, mock_session_repository, mock_transcriber, mock_event_publisher) -> None:
        session = Transcription(session_id="s1", user_id="u1", status=TranscriptionStatus.STREAMING)
        session.add_chunk(b"x" * 100)
        mock_session_repository.get_session = AsyncMock(return_value=session)

        uc = ProcessAudioChunkUseCase(
            session_repository=mock_session_repository,
            transcriber=mock_transcriber,
            event_publisher=mock_event_publisher,
            max_buffer_bytes=50,
        )

        dto = AudioChunkDTO(session_id="s1", chunk=b"x" * 100)
        with pytest.raises(BufferLimitError):
            await uc.execute(dto)

    async def test_process_chunk_skip_partial(self, mock_session_repository, mock_transcriber, mock_event_publisher) -> None:
        session = Transcription(session_id="s1", user_id="u1", status=TranscriptionStatus.STREAMING)
        session.last_partial_at = 9999999999.0
        mock_session_repository.get_session = AsyncMock(return_value=session)

        uc = ProcessAudioChunkUseCase(
            session_repository=mock_session_repository,
            transcriber=mock_transcriber,
            event_publisher=mock_event_publisher,
            chunk_interval_ms=50000,
        )

        dto = AudioChunkDTO(session_id="s1", chunk=b"test")
        result = await uc.execute(dto)

        assert result is None


class TestFinalizeTranscriptionUseCase:
    async def test_finalize_success(self, mock_session_repository, mock_transcriber, mock_event_publisher) -> None:
        session = Transcription(session_id="s1", user_id="u1", status=TranscriptionStatus.STREAMING)
        session.add_chunk(b"test audio data for final")
        mock_session_repository.get_session = AsyncMock(return_value=session)

        uc = FinalizeTranscriptionUseCase(
            session_repository=mock_session_repository,
            transcriber=mock_transcriber,
            event_publisher=mock_event_publisher,
        )

        result = await uc.execute("s1")

        assert result.session_id == "s1"
        assert result.text == "test final text"
        assert result.language == "es"
        assert result.latency_ms > 0
        mock_transcriber.transcribe_final.assert_awaited_once()
        mock_event_publisher.publish_final.assert_awaited_once()
        mock_session_repository.remove_session.assert_awaited_once_with("s1")

    async def test_finalize_session_not_found(self, mock_session_repository, mock_transcriber, mock_event_publisher) -> None:
        mock_session_repository.get_session = AsyncMock(return_value=None)

        uc = FinalizeTranscriptionUseCase(
            session_repository=mock_session_repository,
            transcriber=mock_transcriber,
            event_publisher=mock_event_publisher,
        )

        with pytest.raises(FinalizeNotFound):
            await uc.execute("nonexistent")

    async def test_finalize_already_finalized(self, mock_session_repository, mock_transcriber, mock_event_publisher) -> None:
        session = Transcription(session_id="s1", user_id="u1", status=TranscriptionStatus.FINALIZED)
        mock_session_repository.get_session = AsyncMock(return_value=session)

        uc = FinalizeTranscriptionUseCase(
            session_repository=mock_session_repository,
            transcriber=mock_transcriber,
            event_publisher=mock_event_publisher,
        )

        with pytest.raises(SessionAlreadyFinalizedError):
            await uc.execute("s1")
