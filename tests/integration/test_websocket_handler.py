from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.application.use_cases.start_session import StartSessionUseCase
from src.presentation.websocket.transcription_ws import TranscriptionWebSocketHandler


class TestTranscriptionWebSocketHandler:
    @pytest.fixture
    def handler(self, mock_session_repository, mock_transcriber, mock_event_publisher):
        connection_manager = MagicMock()
        connection_manager.add = AsyncMock()
        connection_manager.remove = AsyncMock()
        connection_manager.send_to_session = AsyncMock(return_value=True)

        start_uc = StartSessionUseCase(
            session_repository=mock_session_repository,
            event_publisher=mock_event_publisher,
            max_sessions=100,
        )

        process_uc = MagicMock()
        process_uc.execute = AsyncMock(return_value=None)

        finalize_uc = MagicMock()
        finalize_uc.execute = AsyncMock()

        return TranscriptionWebSocketHandler(
            start_session_uc=start_uc,
            process_chunk_uc=process_uc,
            finalize_uc=finalize_uc,
            connection_manager=connection_manager,
            session_repository=mock_session_repository,
        )

    async def test_parse_valid_start_message(self, handler):
        ws = MagicMock()
        ws.receive_text = AsyncMock(
            return_value=json.dumps({
                "type": "start",
                "session_id": "test-session",
                "user_id": "test-user",
                "language": "es",
            })
        )
        ws.accept = AsyncMock()
        ws.close = AsyncMock()
        ws.receive = AsyncMock(side_effect=[{"type": "websocket.disconnect"}])

        await handler.handle(ws)

        ws.accept.assert_awaited_once()

    async def test_invalid_start_message_no_type(self, handler):
        ws = MagicMock()
        ws.receive_text = AsyncMock(
            return_value=json.dumps({
                "session_id": "test-session",
                "user_id": "test-user",
            })
        )
        ws.accept = AsyncMock()
        ws.close = AsyncMock()
        ws.receive = AsyncMock(side_effect=[{"type": "websocket.disconnect"}])

        await handler.handle(ws)

        ws.close.assert_awaited_once()

    async def test_invalid_start_message_missing_session_id(self, handler):
        ws = MagicMock()
        ws.receive_text = AsyncMock(
            return_value=json.dumps({
                "type": "start",
                "user_id": "test-user",
            })
        )
        ws.accept = AsyncMock()
        ws.close = AsyncMock()
        ws.receive = AsyncMock(side_effect=[{"type": "websocket.disconnect"}])

        await handler.handle(ws)

        ws.close.assert_awaited_once()
