from __future__ import annotations

from datetime import datetime, timezone

from src.domain.entities.transcription import (
    EventType,
    Transcription,
    TranscriptionEvent,
    TranscriptionStatus,
)


class TestTranscription:
    def test_create_transcription(self) -> None:
        t = Transcription(
            session_id="session-1",
            user_id="user-1",
        )
        assert t.session_id == "session-1"
        assert t.user_id == "user-1"
        assert t.status == TranscriptionStatus.PENDING
        assert t.language == "es"
        assert t.chunks == []
        assert t.partial_texts == []
        assert t.final_text is None
        assert t.total_bytes() == 0
        assert t.chunk_count() == 0

    def test_add_chunk(self) -> None:
        t = Transcription(session_id="s1", user_id="u1")
        t.add_chunk(b"hello")
        assert t.chunk_count() == 1
        assert t.total_bytes() == 5
        t.add_chunk(b" world")
        assert t.chunk_count() == 2
        assert t.total_bytes() == 11
        assert t.get_audio_buffer() == b"hello world"

    def test_mark_streaming(self) -> None:
        t = Transcription(session_id="s1", user_id="u1")
        assert t.status == TranscriptionStatus.PENDING
        t.mark_streaming()
        assert t.status == TranscriptionStatus.STREAMING

    def test_mark_finalized(self) -> None:
        t = Transcription(session_id="s1", user_id="u1")
        t.mark_finalized("hello world")
        assert t.status == TranscriptionStatus.FINALIZED
        assert t.final_text == "hello world"

    def test_mark_failed(self) -> None:
        t = Transcription(session_id="s1", user_id="u1")
        t.mark_failed()
        assert t.status == TranscriptionStatus.FAILED

    def test_add_partial(self) -> None:
        t = Transcription(session_id="s1", user_id="u1")
        t.add_partial("hello")
        assert t.partial_texts == ["hello"]
        assert t.last_partial_at > 0
        t.add_partial("hello world")
        assert t.partial_texts == ["hello", "hello world"]

    def test_large_buffer(self) -> None:
        t = Transcription(session_id="s1", user_id="u1")
        chunk = b"x" * 1024 * 1024
        t.add_chunk(chunk)
        assert t.total_bytes() == 1024 * 1024
        assert t.chunk_count() == 1


class TestTranscriptionEvent:
    def test_create_partial_event(self) -> None:
        event = TranscriptionEvent.create(
            session_id="s1",
            user_id="u1",
            language="es",
            event_type=EventType.PARTIAL,
            text="hola",
        )
        assert event.session_id == "s1"
        assert event.user_id == "u1"
        assert event.language == "es"
        assert event.event_type == EventType.PARTIAL
        assert event.text == "hola"
        assert event.event_id is not None
        assert event.timestamp is not None

    def test_create_final_event(self) -> None:
        event = TranscriptionEvent.create(
            session_id="s1",
            user_id="u1",
            language="es",
            event_type=EventType.FINAL,
            text="hola mundo",
        )
        assert event.event_type == EventType.FINAL
        assert event.text == "hola mundo"
