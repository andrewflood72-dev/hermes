"""Abstract base parser for Hermes SERFF filing document parsers.

All concrete parsers (RateParser, RuleParser, FormParser) inherit from BaseParser
which handles: PDF reading orchestration, result storage, parse logging, and
low-confidence field queueing for human review.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from hermes.config import settings

logger = logging.getLogger(__name__)


class ParseResult(BaseModel):
    """Structured output from any parser run."""

    document_id: UUID
    parser_type: str
    tables_extracted: int = 0
    rules_extracted: int = 0
    forms_extracted: int = 0
    factors_extracted: int = 0
    confidence_avg: float = 0.0
    confidence_min: float = 1.0
    ai_calls_made: int = 0
    ai_tokens_used: int = 0
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    duration_seconds: float = 0.0
    status: str = "completed"  # completed, failed, partial


class ConfidenceTracker:
    """Accumulates confidence scores across extracted fields and tables."""

    def __init__(self) -> None:
        self._scores: list[float] = []

    def record(self, score: float) -> None:
        """Record a single confidence score."""
        if 0.0 <= score <= 1.0:
            self._scores.append(score)

    @property
    def average(self) -> float:
        """Return the mean confidence across all recorded scores."""
        if not self._scores:
            return 0.0
        return sum(self._scores) / len(self._scores)

    @property
    def minimum(self) -> float:
        """Return the lowest recorded confidence score."""
        if not self._scores:
            return 0.0
        return min(self._scores)

    def reset(self) -> None:
        """Clear all recorded scores."""
        self._scores.clear()


class BaseParser(ABC):
    """Abstract base class for all Hermes document parsers.

    Subclasses must implement:
      - ``_extract_content``: reads the PDF and returns a dict of raw results
      - ``_store_results``: persists the extracted dict to the appropriate tables

    The public ``parse`` method orchestrates the full pipeline:
    read → extract → store → log → return ``ParseResult``.
    """

    parser_type: str = "base"
    parser_version: str = "1.0.0"

    def __init__(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}"
        )
        self.confidence_tracker = ConfidenceTracker()
        self._ai_calls_made: int = 0
        self._ai_tokens_used: int = 0

    # ── Public entry point ─────────────────────────────────────────────────

    async def parse(self, document_id: UUID, file_path: str) -> ParseResult:
        """Parse a SERFF filing PDF and persist structured results.

        Args:
            document_id: UUID of the ``hermes_filing_documents`` record.
            file_path: Absolute path to the downloaded PDF on disk.

        Returns:
            A ``ParseResult`` summarising extraction statistics, confidence
            scores, and any errors or warnings encountered.
        """
        started_at = time.monotonic()
        self.confidence_tracker.reset()
        self._ai_calls_made = 0
        self._ai_tokens_used = 0

        result = ParseResult(
            document_id=document_id,
            parser_type=self.parser_type,
        )

        self.logger.info(
            "Starting %s parse | document_id=%s | file=%s",
            self.parser_type,
            document_id,
            file_path,
        )

        try:
            extracted = await self._extract_content(file_path)
            result.tables_extracted = extracted.get("tables_extracted", 0)
            result.rules_extracted = extracted.get("rules_extracted", 0)
            result.forms_extracted = extracted.get("forms_extracted", 0)
            result.factors_extracted = extracted.get("factors_extracted", 0)
            result.warnings = extracted.get("warnings", [])

            await self._store_results(document_id, extracted)

        except FileNotFoundError:
            msg = f"PDF not found at path: {file_path}"
            self.logger.error(msg)
            result.errors.append(msg)
            result.status = "failed"
        except Exception as exc:  # noqa: BLE001
            msg = f"Unexpected error during {self.parser_type} parse: {exc}"
            self.logger.exception(msg)
            result.errors.append(msg)
            result.status = "partial" if result.tables_extracted > 0 else "failed"
        finally:
            result.duration_seconds = round(time.monotonic() - started_at, 3)
            result.confidence_avg = round(self.confidence_tracker.average, 4)
            result.confidence_min = round(self.confidence_tracker.minimum, 4)
            result.ai_calls_made = self._ai_calls_made
            result.ai_tokens_used = self._ai_tokens_used

            await self._log_parse(document_id, result)

        self.logger.info(
            "Completed %s parse | document_id=%s | status=%s | "
            "confidence_avg=%.4f | duration=%.2fs",
            self.parser_type,
            document_id,
            result.status,
            result.confidence_avg,
            result.duration_seconds,
        )
        return result

    # ── Abstract methods ───────────────────────────────────────────────────

    @abstractmethod
    async def _extract_content(self, file_path: str) -> dict[str, Any]:
        """Extract structured content from a PDF file.

        Args:
            file_path: Absolute path to the PDF.

        Returns:
            A dict containing extracted data plus the standard summary keys:
            ``tables_extracted``, ``rules_extracted``, ``forms_extracted``,
            ``factors_extracted``, and ``warnings``.
        """

    @abstractmethod
    async def _store_results(
        self, document_id: UUID, results: dict[str, Any]
    ) -> None:
        """Persist extracted results to the database.

        Args:
            document_id: UUID of the source ``hermes_filing_documents`` record.
            results: The dict returned by ``_extract_content``.
        """

    # ── Logging & review queue ─────────────────────────────────────────────

    async def _log_parse(
        self, document_id: UUID, result: ParseResult
    ) -> None:
        """Write a row to ``hermes_parse_log`` for auditing and monitoring.

        Uses a raw asyncpg connection sourced from ``hermes.db`` so that this
        single write does not require a full SQLAlchemy session.

        Args:
            document_id: UUID of the parsed document.
            result: The ``ParseResult`` to persist.
        """
        try:
            # Import here to avoid circular imports at module load time.
            from hermes.db import get_connection  # type: ignore[import]

            async with get_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO hermes_parse_log (
                        document_id, parser_type, parser_version,
                        tables_extracted, rules_extracted, forms_extracted,
                        factors_extracted, confidence_avg, confidence_min,
                        ai_calls_made, ai_tokens_used,
                        errors, warnings,
                        started_at, completed_at, duration_seconds, status
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9,
                        $10, $11, $12::jsonb, $13::jsonb,
                        NOW() - ($14 || ' seconds')::interval,
                        NOW(), $14, $15
                    )
                    """,
                    str(document_id),
                    result.parser_type,
                    self.parser_version,
                    result.tables_extracted,
                    result.rules_extracted,
                    result.forms_extracted,
                    result.factors_extracted,
                    result.confidence_avg,
                    result.confidence_min,
                    result.ai_calls_made,
                    result.ai_tokens_used,
                    str(result.errors),
                    str(result.warnings),
                    result.duration_seconds,
                    result.status,
                )
        except Exception as exc:  # noqa: BLE001
            # Never let a logging failure crash the parse run.
            self.logger.warning("Failed to write parse log: %s", exc)

    def _queue_low_confidence(
        self,
        document_id: UUID,
        field_name: str,
        value: Any,
        confidence: float,
        context: str,
    ) -> None:
        """Queue a low-confidence extracted field for human review.

        Inserts a row into ``hermes_parse_review_queue`` if the supplied
        ``confidence`` is below the 0.70 threshold.

        Args:
            document_id: UUID of the source document.
            field_name: Name of the field that was extracted.
            value: The extracted value (will be coerced to str).
            confidence: Extraction confidence score (0.0 – 1.0).
            context: Surrounding text snippet for reviewer context.
        """
        if confidence >= 0.7:
            return

        priority = "high" if confidence < 0.5 else "medium"

        import asyncio

        async def _insert() -> None:
            try:
                from hermes.db import get_connection  # type: ignore[import]

                async with get_connection() as conn:
                    await conn.execute(
                        """
                        INSERT INTO hermes_parse_review_queue (
                            document_id, table_name, field_name,
                            extracted_value, confidence, context_text,
                            review_priority
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                        str(document_id),
                        f"hermes_{self.parser_type}_extracted",
                        field_name,
                        str(value),
                        confidence,
                        context[:2000],
                        priority,
                    )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "Failed to queue low-confidence field '%s': %s",
                    field_name,
                    exc,
                )

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_insert())
            else:
                loop.run_until_complete(_insert())
        except RuntimeError:
            # No event loop available — log and move on.
            self.logger.warning(
                "Cannot queue review for field '%s' (no event loop): "
                "confidence=%.4f",
                field_name,
                confidence,
            )
