from __future__ import annotations

import json
import pickle
from typing import Optional

import redis.asyncio as aioredis
import structlog

from src.domain.entities.transcription import Transcription, TranscriptionStatus
from src.domain.repositories.session_repository import SessionRepository

logger = structlog.get_logger()


class RedisSessionRepository(SessionRepository):
    def __init__(self, redis_client: aioredis.Redis, session_ttl: int = 900) -> None:
        self._redis = redis_client
        self._session_ttl = session_ttl
        self._prefix = "session:"

    def _key(self, session_id: str) -> str:
        return f"{self._prefix}{session_id}"

    async def create_session(self, transcription: Transcription) -> None:
        key = self._key(transcription.session_id)
        data = {
            "session_id": transcription.session_id,
            "user_id": transcription.user_id,
            "status": transcription.status.value,
            "language": transcription.language,
            "partial_texts": json.dumps(transcription.partial_texts),
            "final_text": transcription.final_text or "",
            "created_at": transcription.created_at.isoformat(),
            "updated_at": transcription.updated_at.isoformat(),
            "last_partial_at": str(transcription.last_partial_at),
            "_total_bytes": str(transcription.total_bytes()),
        }
        audio_key = f"{key}:audio"
        await self._redis.set(key, json.dumps(data), ex=self._session_ttl)
        await self._redis.delete(audio_key)
        await logger.ainfo("session_created_redis", session_id=transcription.session_id)

    async def get_session(self, session_id: str) -> Optional[Transcription]:
        key = self._key(session_id)
        raw = await self._redis.get(key)
        if raw is None:
            return None

        data = json.loads(raw)
        audio_key = f"{key}:audio"
        audio_data = await self._redis.get(audio_key)
        chunks = pickle.loads(audio_data) if audio_data else []

        session = Transcription(
            session_id=data["session_id"],
            user_id=data["user_id"],
            status=TranscriptionStatus(data["status"]),
            language=data["language"],
            chunks=chunks,
            partial_texts=json.loads(data.get("partial_texts", "[]")),
            final_text=data.get("final_text") or None,
        )
        return session

    async def remove_session(self, session_id: str) -> None:
        key = self._key(session_id)
        audio_key = f"{key}:audio"
        await self._redis.delete(key, audio_key)
        await logger.ainfo("session_removed_redis", session_id=session_id)

    async def get_active_count(self) -> int:
        keys = await self._redis.keys(f"{self._prefix}*")
        return len(keys)

    async def get_all_active_sessions(self) -> list[Transcription]:
        keys = await self._redis.keys(f"{self._prefix}*")
        sessions = []
        for key in keys:
            session_id = key.replace(self._prefix, "", 1)
            session = await self.get_session(session_id)
            if session:
                sessions.append(session)
        return sessions

    async def append_audio_chunk(self, session_id: str, chunk: bytes) -> None:
        audio_key = f"{self._key(session_id)}:audio"
        await self._redis.append(audio_key, chunk)
        await self._redis.expire(audio_key, self._session_ttl)

    async def get_audio_buffer(self, session_id: str) -> bytes:
        audio_key = f"{self._key(session_id)}:audio"
        data = await self._redis.get(audio_key)
        return data or b""
