from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    WHISPER_MODEL: str = "large-v3"
    WHISPER_DEVICE: str = "cuda"
    WHISPER_COMPUTE_TYPE: str = "float16"
    REDIS_URL: str = "redis://127.0.0.1:6379"
    TRANSCRIPTION_STREAM: str = "transcriptions"
    CHUNK_INTERVAL_MS: int = 500
    MAX_SESSIONS: int = 100
    MAX_BUFFER_MB: int = 50
    LOG_LEVEL: str = "INFO"
    MAX_CHUNK_SIZE_BYTES: int = 131072
    HEARTBEAT_INTERVAL_SECONDS: int = 15
    HEARTBEAT_TIMEOUT_SECONDS: int = 30
    SESSION_TIMEOUT_SECONDS: int = 600
    REDIS_SESSION_ENABLED: bool = False
    KAFKA_BOOTSTRAP_SERVERS: str = ""
    KAFKA_TOPIC: str = "transcriptions.raw"
    KAFKA_CLIENT_ID: str = "speech-service"
    LOG_FILE: str = ""
    SERVICE_NAME: str = "speech-service"

    @property
    def max_buffer_bytes(self) -> int:
        return self.MAX_BUFFER_MB * 1024 * 1024

    @property
    def use_gpu(self) -> bool:
        return self.WHISPER_DEVICE == "cuda"


settings = Settings()
