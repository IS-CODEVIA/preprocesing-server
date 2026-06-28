from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from src.domain.entities.transcription import EventType, TranscriptionEvent
from src.infrastructure.redis.redis_stream_publisher import RedisStreamPublisher


@pytest_asyncio.fixture
async def mock_redis():
    redis = MagicMock()
    redis.xadd = AsyncMock(return_value="1680000000000-0")
    return redis


class TestRedisStreamPublisher:
    async def test_publish_event(self, mock_redis) -> None:
        publisher = RedisStreamPublisher(
            redis_client=mock_redis,
            stream_name="test-transcriptions",
        )

        event = TranscriptionEvent.create(
            session_id="s1",
            user_id="u1",
            language="es",
            event_type=EventType.PARTIAL,
            text="hola",
        )

        message_id = await publisher.publish(event)

        assert message_id == "1680000000000-0"
        mock_redis.xadd.assert_awaited_once()
        call_args = mock_redis.xadd.await_args
        assert call_args is not None
        assert call_args[0][0] == "test-transcriptions"
        assert call_args[0][1]["event_id"] == event.event_id
        assert call_args[0][1]["session_id"] == "s1"
        assert call_args[0][1]["event_type"] == "partial"
        assert call_args[0][1]["text"] == "hola"

    async def test_publish_partial(self, mock_redis) -> None:
        publisher = RedisStreamPublisher(
            redis_client=mock_redis,
            stream_name="test-transcriptions",
        )

        message_id = await publisher.publish_partial(
            session_id="s1",
            user_id="u1",
            language="es",
            text="hola mundo",
        )

        assert message_id == "1680000000000-0"
        mock_redis.xadd.assert_awaited_once()
        call_args = mock_redis.xadd.await_args
        assert call_args is not None
        assert call_args[0][1]["event_type"] == "partial"
        assert call_args[0][1]["text"] == "hola mundo"

    async def test_publish_final(self, mock_redis) -> None:
        publisher = RedisStreamPublisher(
            redis_client=mock_redis,
            stream_name="test-transcriptions",
        )

        message_id = await publisher.publish_final(
            session_id="s1",
            user_id="u1",
            language="es",
            text="hola mundo final",
        )

        assert message_id == "1680000000000-0"
        mock_redis.xadd.assert_awaited_once()
        call_args = mock_redis.xadd.await_args
        assert call_args is not None
        assert call_args[0][1]["event_type"] == "final"
        assert call_args[0][1]["text"] == "hola mundo final"

    async def test_publish_with_maxlen(self, mock_redis) -> None:
        publisher = RedisStreamPublisher(
            redis_client=mock_redis,
            stream_name="test-transcriptions",
            max_stream_length=50000,
        )

        event = TranscriptionEvent.create(
            session_id="s1",
            user_id="u1",
            language="es",
            event_type=EventType.PARTIAL,
            text="test",
        )

        await publisher.publish(event)

        call_args = mock_redis.xadd.await_args
        assert call_args is not None
        kwargs = call_args[1]
        assert kwargs.get("maxlen") == 50000
        assert kwargs.get("approximate") is True
