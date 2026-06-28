"""
Whisper Transcription Quality Validator
Add this to the existing wisper/ service for quality validation.
Place in: wisper/src/domain/services/
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class QualityReport:
    overall_score: float = 0.0
    language_confidence: float = 0.0
    word_error_rate_estimate: Optional[float] = None
    has_repetitions: bool = False
    has_artifacts: bool = False
    is_complete_sentence: bool = False
    min_segment_confidence: float = 1.0
    avg_segment_confidence: float = 0.0
    warnings: list[str] = field(default_factory=list)
    passed: bool = True


class TranscriptQualityValidator:
    MIN_OVERALL_SCORE = 0.6
    MIN_SEGMENT_CONFIDENCE = 0.3
    REPETITION_PATTERN = re.compile(r"(\b\w+\b)(\s+\1\b){2,}", re.IGNORECASE)
    ARTIFACT_PATTERNS = [
        re.compile(r"\[.*?\]"),
        re.compile(r"\(.*?\)"),
        re.compile(r"[^\w\s\.\,\!\?\-\:\;\'\u00C0-\u024F]"),
        re.compile(r"\b(?:um|uh|ah|mm-hmm|uh-huh)\b", re.IGNORECASE),
    ]

    @classmethod
    def validate(
        cls,
        text: str,
        segments: list[dict] | None = None,
        detected_language: str = "",
        expected_language: str = "",
    ) -> QualityReport:
        report = QualityReport()

        if not text or not text.strip():
            report.passed = False
            report.warnings.append("Empty transcript")
            return report

        if segments:
            confidences = [s.get("confidence", 1.0) for s in segments if "confidence" in s]
            if confidences:
                report.min_segment_confidence = min(confidences)
                report.avg_segment_confidence = sum(confidences) / len(confidences)
                if report.min_segment_confidence < cls.MIN_SEGMENT_CONFIDENCE:
                    report.warnings.append(
                        f"Low confidence segment: {report.min_segment_confidence:.2f}"
                    )

        if cls.REPETITION_PATTERN.search(text):
            report.has_repetitions = True
            report.warnings.append("Repeated phrases detected")

        artifact_count = sum(len(p.findall(text)) for p in cls.ARTIFACT_PATTERNS)
        if artifact_count > 3:
            report.has_artifacts = True
            report.warnings.append(f"Audio artifacts detected ({artifact_count} instances)")

        sentence_endings = len(re.findall(r"[.!?]", text))
        report.is_complete_sentence = sentence_endings >= 1

        if not report.is_complete_sentence:
            report.warnings.append("Transcript appears incomplete (no sentence endings)")

        score = 1.0
        score -= 0.15 if report.has_repetitions else 0
        score -= 0.15 if report.has_artifacts else 0
        score -= 0.10 if not report.is_complete_sentence else 0
        score -= 0.10 * (1 - report.avg_segment_confidence) if segments else 0
        score = max(0.0, min(1.0, score))
        report.overall_score = score
        report.passed = score >= cls.MIN_OVERALL_SCORE

        return report


class LanguageValidator:
    COMMON_LANGUAGES = {"en", "es", "fr", "de", "it", "pt", "ja", "zh", "ko", "ru", "ar"}

    @classmethod
    def validate_language(
        cls,
        detected_language: str,
        expected_language: str,
        confidence: float = 0.0,
    ) -> tuple[bool, str]:
        if not expected_language:
            return True, "No expected language specified"

        if detected_language != expected_language:
            if confidence < 0.5:
                return False, (
                    f"Low confidence language detection: "
                    f"detected={detected_language} ({confidence:.2f}), "
                    f"expected={expected_language}"
                )
            return True, (
                f"Language mismatch (detected={detected_language}, "
                f"expected={expected_language}) but confidence={confidence:.2f} > 0.5"
            )

        return True, "Language matches expected"
