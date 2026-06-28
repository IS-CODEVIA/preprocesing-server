from __future__ import annotations

import asyncio
import io
import os
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import numpy as np
import soundfile as sf
import structlog




from src.domain.services.transcriber import Transcriber
from src.infrastructure.config.settings import settings

logger = structlog.get_logger()


class FasterWhisperAdapter(Transcriber):
    def __init__(
        self,
        model_name: str = settings.WHISPER_MODEL,
        device: str = settings.WHISPER_DEVICE,
        compute_type: str = settings.WHISPER_COMPUTE_TYPE,
        num_workers: int = 1,
    ) -> None:
        self._model_name = model_name
        self._device = device
        self._compute_type = compute_type
        self._num_workers = num_workers
        self._model: Optional[object] = None
        self._executor = ThreadPoolExecutor(
            max_workers=num_workers,
            thread_name_prefix="whisper",
        )

    async def load_model(self) -> None:
        from faster_whisper import WhisperModel

        await logger.ainfo(
            "loading_whisper_model",
            model=self._model_name,
            device=self._device,
            compute_type=self._compute_type,
        )

        loop = asyncio.get_running_loop()
        self._model = await loop.run_in_executor(
            self._executor,
            lambda: WhisperModel(
                model_size_or_path=self._model_name,
                device=self._device,
                compute_type=self._compute_type,
                num_workers=self._num_workers,
                cpu_threads=4 if self._device == "cpu" else 1,
            ),
        )

        await logger.ainfo(
            "whisper_model_loaded",
            model=self._model_name,
            device=self._device,
        )

    async def unload_model(self) -> None:
        if self._model is not None:
            self._model = None
            self._executor.shutdown(wait=False)
            await logger.ainfo("whisper_model_unloaded")

    async def transcribe_partial(self, audio_bytes: bytes, language: str) -> str:
        if self._model is None:
            raise RuntimeError("Whisper model not loaded. Call load_model() first.")

        audio_array = await self._decode_audio(audio_bytes)
        loop = asyncio.get_running_loop()

        segments, info = await loop.run_in_executor(
            self._executor,
            lambda: self._model.transcribe(
                audio=audio_array,
                language=language,
                task="transcribe",
                beam_size=3,
                best_of=3,
                temperature=0.0,
                vad_filter=True,
                vad_parameters=dict(
                    min_speech_duration_ms=100,
                    min_silence_duration_ms=30,
                ),
            ),
        )

        text_parts = []
        for segment in segments:
            text_parts.append(segment.text.strip())

        return " ".join(text_parts) if text_parts else ""

    async def transcribe_final(self, audio_bytes: bytes, language: str) -> str:
        if self._model is None:
            raise RuntimeError("Whisper model not loaded. Call load_model() first.")

        audio_array = await self._decode_audio(audio_bytes)
        loop = asyncio.get_running_loop()

        segments, info = await loop.run_in_executor(
            self._executor,
            lambda: self._model.transcribe(
                audio=audio_array,
                language=language,
                task="transcribe",
                beam_size=5,
                best_of=5,
                temperature=0.0,
                vad_filter=True,
                vad_parameters=dict(
                    min_speech_duration_ms=200,
                    min_silence_duration_ms=100,
                ),
                word_timestamps=True,
            ),
        )

        text_parts = []
        for segment in segments:
            text_parts.append(segment.text.strip())

        return " ".join(text_parts) if text_parts else ""

    async def _decode_audio(self, audio_bytes: bytes) -> np.ndarray:
        try:
            audio_array, sample_rate = sf.read(io.BytesIO(audio_bytes), dtype="float32")
        except Exception:
            audio_array = await self._decode_with_ffmpeg(audio_bytes)
            sample_rate = 16000

        if sample_rate != 16000:
            audio_array = self._resample(audio_array, sample_rate, 16000)

        return audio_array.astype(np.float32)

    async def _decode_with_ffmpeg(self, audio_bytes: bytes) -> np.ndarray:
        loop = asyncio.get_running_loop()

        def _run_ffmpeg() -> np.ndarray:
            with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as tmp_in:
                tmp_in.write(audio_bytes)
                tmp_in_path = tmp_in.name

            tmp_out_path = tmp_in_path + ".wav"
            try:
                subprocess.run(
                    [
                        "ffmpeg",
                        "-i", tmp_in_path,
                        "-ar", "16000",
                        "-ac", "1",
                        "-f", "wav",
                        "-y",
                        tmp_out_path,
                    ],
                    capture_output=True,
                    check=True,
                )
                data, sr = sf.read(tmp_out_path, dtype="float32")
                return data
            finally:
                try:
                    os.unlink(tmp_in_path)
                    if os.path.exists(tmp_out_path):
                        os.unlink(tmp_out_path)
                except OSError:
                    pass

        return await loop.run_in_executor(self._executor, _run_ffmpeg)

    def _resample(self, audio: np.ndarray, orig_sr: int, target_sr: int) -> np.ndarray:
        if orig_sr == target_sr:
            return audio
        ratio = target_sr / orig_sr
        n_samples = int(len(audio) * ratio)
        return np.interp(
            np.linspace(0, len(audio) - 1, n_samples),
            np.arange(len(audio)),
            audio,
        ).astype(np.float32)
