from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Optional

import structlog
from aiokafka import AIOKafkaProducer

from src.domain.entities.transcription import EventType, TranscriptionEvent
from src.domain.services.event_publisher import TranscriptionEventPublisher

logger = structlog.get_logger()


class KafkaEventPublisher(TranscriptionEventPublisher):
    def __init__(
        self,
        bootstrap_servers: str = "127.0.0.1:9092",
        client_id: str = "speech-service",
        topic: str = "transcriptions.raw",
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._topic = topic
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retry_backoff_ms=500,
            request_timeout_ms=30000,
            max_request_size=10485760,
        )
        await self._producer.start()
        await logger.ainfo("kafka_producer_started", servers=self._bootstrap_servers)

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            await logger.ainfo("kafka_producer_stopped")

    async def publish(self, event: TranscriptionEvent) -> str:
        message_id = str(uuid.uuid4())
        payload = {
            "event_id": message_id,
            "event_type": f"transcription.{event.event_type.value}",
            "session_id": event.session_id,
            "user_id": event.user_id,
            "language": event.language,
            "text": event.text,
            "confidence": 0.0,
            "language_confidence": 0.0,
            "source_service": "speech-service",
            "timestamp": event.timestamp,
        }
        if self._producer is None:
            raise RuntimeError("Kafka producer not started")

        await self._producer.send_and_wait(self._topic, payload)
        await logger.ainfo(
            "kafka_event_published",
            topic=self._topic,
            event_id=message_id,
            event_type=payload["event_type"],
            session_id=event.session_id,
        )
        return message_id

    async def publish_partial(
        self,
        session_id: str,
        user_id: str,
        language: str,
        text: str,
    ) -> str:
        return ""

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
