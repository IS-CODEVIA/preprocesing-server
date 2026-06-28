from __future__ import annotations

from typing import Optional

import redis.asyncio as aioredis
import structlog

from src.domain.repositories.session_repository import SessionRepository
from src.domain.services.event_publisher import TranscriptionEventPublisher
from src.domain.services.transcriber import Transcriber

from src.application.use_cases.start_session import StartSessionUseCase
from src.application.use_cases.process_audio_chunk import ProcessAudioChunkUseCase
from src.application.use_cases.finalize_transcription import FinalizeTranscriptionUseCase

from src.infrastructure.config.settings import settings
from src.infrastructure.redis.redis_stream_publisher import RedisStreamPublisher
from src.infrastructure.redis.in_memory_session_repository import InMemorySessionRepository
from src.infrastructure.redis.redis_session_repository import RedisSessionRepository
from src.infrastructure.kafka.kafka_event_publisher import KafkaEventPublisher
from src.infrastructure.speech.faster_whisper_adapter import FasterWhisperAdapter
from src.infrastructure.websocket.connection_manager import ConnectionManager

logger = structlog.get_logger()


class Container:
    def __init__(self) -> None:
        self._redis: Optional[aioredis.Redis] = None
        self._kafka_publisher: Optional[KafkaEventPublisher] = None

        self.connection_manager = ConnectionManager()

        self.session_repository: SessionRepository = InMemorySessionRepository()

        self.transcriber: Transcriber = FasterWhisperAdapter(
            model_name=settings.WHISPER_MODEL,
            device=settings.WHISPER_DEVICE,
            compute_type=settings.WHISPER_COMPUTE_TYPE,
        )

        self.event_publisher: TranscriptionEventPublisher = None  # type: ignore

    async def initialize_redis(self) -> aioredis.Redis:
        self._redis = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            health_check_interval=30,
            socket_connect_timeout=5,
            socket_timeout=10,
            retry_on_timeout=True,
            max_connections=50,
        )

        await self._redis.ping()

        self.event_publisher = RedisStreamPublisher(
            redis_client=self._redis,
            stream_name=settings.TRANSCRIPTION_STREAM,
        )

        if settings.REDIS_SESSION_ENABLED:
            self.session_repository = RedisSessionRepository(
                redis_client=self._redis,
                session_ttl=settings.SESSION_TIMEOUT_SECONDS,
            )
            await logger.ainfo("using_redis_session_repository")

        await logger.ainfo(
            "redis_connected",
            url=settings.REDIS_URL,
        )

        return self._redis

    async def initialize_kafka(self) -> None:
        if not settings.KAFKA_BOOTSTRAP_SERVERS:
            await logger.ainfo("kafka_not_configured_skipping")
            return
        self._kafka_publisher = KafkaEventPublisher(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            client_id=settings.KAFKA_CLIENT_ID,
            topic=settings.KAFKA_TOPIC,
        )
        await self._kafka_publisher.start()
        await logger.ainfo(
            "kafka_connected",
            servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        )

    async def close_redis(self) -> None:
        if self._redis is not None:
            await self._redis.close()
            await self._redis.connection_pool.disconnect()
            await logger.ainfo("redis_disconnected")

    def create_start_session_use_case(self) -> StartSessionUseCase:
        return StartSessionUseCase(
            session_repository=self.session_repository,
            event_publisher=self.event_publisher,
            max_sessions=settings.MAX_SESSIONS,
        )

    def create_process_audio_chunk_use_case(self) -> ProcessAudioChunkUseCase:
        return ProcessAudioChunkUseCase(
            session_repository=self.session_repository,
            transcriber=self.transcriber,
            event_publisher=self.event_publisher,
            chunk_interval_ms=settings.CHUNK_INTERVAL_MS,
            max_buffer_bytes=settings.max_buffer_bytes,
            max_chunk_size_bytes=settings.MAX_CHUNK_SIZE_BYTES,
        )

    def create_finalize_transcription_use_case(self) -> FinalizeTranscriptionUseCase:
        return FinalizeTranscriptionUseCase(
            session_repository=self.session_repository,
            transcriber=self.transcriber,
            event_publisher=self.event_publisher,
            kafka_publisher=self._kafka_publisher,
        )

    async def initialize(self) -> None:
        await self.initialize_redis()
        await self.initialize_kafka()
        await self.transcriber.load_model()
        await logger.ainfo(
            "container_initialized",
            whisper_model=settings.WHISPER_MODEL,
            whisper_device=settings.WHISPER_DEVICE,
        )

    async def shutdown(self) -> None:
        await self.transcriber.unload_model()
        await self.connection_manager.close_all()
        if self._kafka_publisher:
            await self._kafka_publisher.stop()
        await self.close_redis()
        await logger.ainfo("container_shutdown_complete")
