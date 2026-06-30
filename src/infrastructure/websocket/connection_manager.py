from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Optional

import structlog
from fastapi import WebSocket
from starlette.websockets import WebSocketState

from src.infrastructure.config.settings import settings

logger = structlog.get_logger()




class WebSocketConnection:
    def __init__(
        self,
        websocket: WebSocket,
        session_id: str,
        user_id: str,
    ) -> None:
        self.websocket = websocket
        self.session_id = session_id
        self.user_id = user_id
        self.connected_at = time.time()
        self.last_activity_at = time.time()
        self._closed = False

    async def send_json(self, data: dict[str, Any]) -> None:
        if self._closed:
            return
        try:
            await self.websocket.send_json(data)
            self.last_activity_at = time.time()
        except Exception as exc:
            await logger.aerror(
                "websocket_send_failed",
                session_id=self.session_id,
                error=str(exc),
            )
            self._closed = True

    async def send_bytes(self, data: bytes) -> None:
        if self._closed:
            return
        try:
            await self.websocket.send_bytes(data)
            self.last_activity_at = time.time()
        except Exception:
            self._closed = True

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self.websocket.client_state != WebSocketState.DISCONNECTED:
                await self.websocket.close(code=code, reason=reason)
        except Exception as exc:
            await logger.awarning(
                "websocket_close_error",
                session_id=self.session_id,
                error=str(exc),
            )

    @property
    def is_closed(self) -> bool:
        return self._closed or self.websocket.client_state == WebSocketState.DISCONNECTED


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: dict[str, WebSocketConnection] = {}
        self._lock = asyncio.Lock()

    async def add(
        self,
        websocket: WebSocket,
        session_id: str,
        user_id: str,
    ) -> WebSocketConnection:
        async with self._lock:
            connection = WebSocketConnection(
                websocket=websocket,
                session_id=session_id,
                user_id=user_id,
            )
            self._connections[session_id] = connection

            await logger.ainfo(
                "websocket_connected",
                session_id=session_id,
                user_id=user_id,
                total_connections=len(self._connections),
            )

            return connection

    async def remove(self, session_id: str) -> None:
        async with self._lock:
            connection = self._connections.pop(session_id, None)
            if connection is not None:
                await connection.close()
                await logger.ainfo(
                    "websocket_disconnected",
                    session_id=session_id,
                    total_connections=len(self._connections),
                )

    async def get(self, session_id: str) -> Optional[WebSocketConnection]:
        async with self._lock:
            return self._connections.get(session_id)

    async def broadcast_json(self, data: dict[str, Any]) -> None:
        async with self._lock:
            for session_id, conn in list(self._connections.items()):
                await conn.send_json(data)

    async def send_to_session(self, session_id: str, data: dict[str, Any]) -> bool:
        connection = await self.get(session_id)
        if connection is None or connection.is_closed:
            return False
        await connection.send_json(data)
        return True

    async def cleanup_stale(self, timeout_seconds: int = 300) -> int:
        now = time.time()
        stale_ids: list[str] = []

        async with self._lock:
            for session_id, conn in self._connections.items():
                if now - conn.last_activity_at > timeout_seconds:
                    stale_ids.append(session_id)
                elif conn.is_closed:
                    stale_ids.append(session_id)

            for session_id in stale_ids:
                conn = self._connections.pop(session_id, None)
                if conn is not None:
                    await conn.close(code=1001, reason="Session timeout")

        if stale_ids:
            await logger.ainfo(
                "cleaned_stale_connections",
                count=len(stale_ids),
            )

        return len(stale_ids)

    @property
    def active_count(self) -> int:
        return len(self._connections)

    async def close_all(self) -> None:
        async with self._lock:
            for session_id, conn in list(self._connections.items()):
                await conn.close(code=1001, reason="Service shutting down")
            self._connections.clear()

        await logger.ainfo("all_websocket_connections_closed")
