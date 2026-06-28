from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as aioredis
import structlog

from src.domain.entities.transcription import EventType, TranscriptionEvent
from src.domain.services.event_publisher import TranscriptionEventPublisher

logger = structlog.get_logger()


class RedisStreamPublisher(TranscriptionEventPublisher):
    def __init__(
        self,
        redis_client: aioredis.Redis,
        stream_name: str = "transcriptions",
        max_stream_length: int = 100_000,
    ) -> None:
        self._redis = redis_client
        self._stream_name = stream_name
        self._max_stream_length = max_stream_length

    async def publish(self, event: TranscriptionEvent) -> str:
        message_id = await self._redis.xadd(
            self._stream_name,
            fields={
                "event_id": event.event_id,
                "session_id": event.session_id,
                "user_id": event.user_id,
                "timestamp": event.timestamp,
                "language": event.language,
                "event_type": event.event_type.value,
                "text": event.text,
            },
            maxlen=self._max_stream_length,
            approximate=True,
        )

        await logger.ainfo(
            "redis_event_published",
            stream=self._stream_name,
            event_id=event.event_id,
            message_id=message_id,
            event_type=event.event_type.value,
        )

        return message_id

    async def publish_partial(
        self,
        session_id: str,
        user_id: str,
        language: str,
        text: str,
    ) -> str:
        event = TranscriptionEvent.create(
            session_id=session_id,
            user_id=user_id,
            language=language,
            event_type=EventType.PARTIAL,
            text=text,
        )
        return await self.publish(event)

    async def publish_final(
        self,
        session_id: str,
        user_id: str,
        language: str,
        text: str,
    ) -> str:
        event = TranscriptionEvent.create(
            session_id=session_id,
            user_id=user_id,
            language=language,
            event_type=EventType.FINAL,
            text=text,
        )
        return await self.publish(event)
