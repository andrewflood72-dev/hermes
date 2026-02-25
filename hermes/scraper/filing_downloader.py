"""Filing document downloader and metadata persistence.

``FilingDownloader`` is responsible for retrieving individual PDF (and other)
documents from SERFF portal download URLs and writing them to an organised
local directory tree.  It also handles upsert of filing metadata and document
records into the Hermes database (``hermes_filings``,
``hermes_filing_documents``).
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes.config import settings
from hermes.scraper.base import FilingResult

logger = logging.getLogger("hermes.scraper")


class FilingDownloader:
    """Download SERFF filing documents and persist metadata to the database.

    Parameters
    ----------
    storage_base:
        Root directory for all downloaded filing documents.  Sub-directories
        are created automatically following the scheme
        ``{storage_base}/{state}/{carrier_naic}/{serff_tracking}/{filename}``.
    """

    def __init__(self, storage_base: str) -> None:
        self.storage_base = Path(storage_base)
        self.storage_base.mkdir(parents=True, exist_ok=True)
        self._engine: AsyncEngine | None = None
        logger.info("FilingDownloader initialised with storage_base=%s", self.storage_base)

    # ------------------------------------------------------------------
    # Engine
    # ------------------------------------------------------------------

    async def _get_engine(self) -> AsyncEngine:
        """Return (creating if necessary) a shared SQLAlchemy async engine."""
        if self._engine is None:
            self._engine = create_async_engine(
                settings.database_url,
                pool_size=2,
                max_overflow=0,
                echo=False,
            )
        return self._engine

    async def close(self) -> None:
        """Dispose of the database engine connection pool."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None

    # ------------------------------------------------------------------
    # Single document download
    # ------------------------------------------------------------------

    async def download_document(
        self, page: Any, url: str, filing: FilingResult
    ) -> str:
        """Download a single document from *url* and return the local path.

        Uses Playwright's built-in download interception so the file is
        streamed directly to disk rather than held in memory.

        Parameters
        ----------
        page:
            Playwright ``Page`` (the browser context must have been opened with
            ``accept_downloads=True``).
        url:
            Absolute download URL.
        filing:
            The filing this document belongs to (used to determine storage path).

        Returns
        -------
        str
            Absolute path of the saved file.

        Raises
        ------
        RuntimeError
            If the download fails or the file cannot be written.
        """
        doc_name = url.split("/")[-1].split("?")[0] or "document.pdf"
        dest_path = self._build_file_path(filing, doc_name)

        if self._is_already_downloaded(dest_path):
            logger.debug("Skip (already downloaded): %s", dest_path)
            return str(dest_path)

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading %s -> %s", url, dest_path)

        try:
            async with page.expect_download(timeout=60000) as dl_info:
                # Navigate to the URL; many SFA document URLs trigger a download.
                await page.goto(url, wait_until="commit", timeout=30000)
            download = await dl_info.value
            await download.save_as(str(dest_path))
        except Exception as exc:
            # Some URLs open inline (not as a download).  Fall back to
            # direct HTTP via Playwright's request API.
            logger.debug(
                "expect_download failed for %s (%s); attempting direct fetch", url, exc
            )
            try:
                response = await page.request.get(url, timeout=30000)
                if response.ok:
                    body = await response.body()
                    dest_path.write_bytes(body)
                else:
                    raise RuntimeError(
                        f"HTTP {response.status} downloading {url}"
                    )
            except Exception as inner_exc:
                raise RuntimeError(
                    f"Could not download {url}: {inner_exc}"
                ) from inner_exc

        logger.info(
            "Saved document: %s (%d bytes)", dest_path, dest_path.stat().st_size
        )
        return str(dest_path)

    # ------------------------------------------------------------------
    # Bulk download
    # ------------------------------------------------------------------

    async def download_all_documents(
        self, page: Any, filing: FilingResult
    ) -> list[str]:
        """Download every document listed in *filing.document_urls*.

        Skips URLs that have already been downloaded (deduplication via
        :meth:`_is_already_downloaded`).

        Parameters
        ----------
        page:
            Playwright ``Page`` (``accept_downloads=True``).
        filing:
            Filing whose ``document_urls`` should all be fetched.

        Returns
        -------
        list[str]
            Absolute local paths of all successfully downloaded documents.
        """
        saved: list[str] = []

        if not filing.document_urls:
            logger.debug(
                "No document URLs for filing %s", filing.serff_tracking_number
            )
            return saved

        logger.info(
            "Downloading %d document(s) for filing %s",
            len(filing.document_urls),
            filing.serff_tracking_number,
        )

        for url in filing.document_urls:
            try:
                path = await self.download_document(page, url, filing)
                saved.append(path)
            except Exception as exc:
                logger.error(
                    "Failed to download %s (filing %s): %s",
                    url,
                    filing.serff_tracking_number,
                    exc,
                )

        return saved

    # ------------------------------------------------------------------
    # Path construction
    # ------------------------------------------------------------------

    def _build_file_path(self, filing: FilingResult, document_name: str) -> Path:
        """Construct an organised local file path for a document.

        The directory hierarchy is::

            {storage_base}/{state}/{carrier_naic}/{serff_tracking}/{filename}

        Where ``state`` is derived from ``filing.raw_metadata.get('state', 'XX')``,
        ``carrier_naic`` defaults to ``"unknown_naic"`` when absent, and
        ``serff_tracking`` is sanitised to remove characters unsafe on Windows.

        Parameters
        ----------
        filing:
            Filing record providing state, NAIC, and tracking number.
        document_name:
            Raw filename (from URL or portal metadata).

        Returns
        -------
        Path
            Full absolute path where the document should be saved.
        """
        state = (
            filing.raw_metadata.get("state", "")
            or filing.raw_metadata.get("state_code", "XX")
        ).upper()[:2]

        naic = self._safe_name(filing.carrier_naic or "unknown_naic")
        tracking = self._safe_name(
            filing.serff_tracking_number or "unknown_tracking"
        )
        doc_filename = self._safe_name(document_name)[:200]

        return self.storage_base / state / naic / tracking / doc_filename

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _is_already_downloaded(self, path: Path) -> bool:
        """Return ``True`` if *path* already exists and is non-empty.

        Parameters
        ----------
        path:
            Candidate file path.

        Returns
        -------
        bool
            ``True`` when the file exists with size > 0.
        """
        return path.exists() and path.stat().st_size > 0

    # ------------------------------------------------------------------
    # Database persistence
    # ------------------------------------------------------------------

    async def store_filing_metadata(self, filing: FilingResult) -> None:
        """Upsert the filing and its documents into the Hermes database.

        Inserts or updates a row in ``hermes_filings`` (matching on
        ``serff_tracking_number`` + ``state``), then inserts any document
        records into ``hermes_filing_documents`` that do not already exist.

        Parameters
        ----------
        filing:
            Populated :class:`~hermes.scraper.base.FilingResult`.
        """
        import json

        try:
            engine = await self._get_engine()
            state = (
                filing.raw_metadata.get("state", "")
                or filing.raw_metadata.get("state_code", "XX")
            ).upper()[:2]

            async with engine.begin() as conn:
                # ── Upsert hermes_filings ──────────────────────────
                filing_upsert = text(
                    """
                    INSERT INTO hermes_filings (
                        id, serff_tracking_number, carrier_naic_code,
                        carrier_name_filed, state, filing_type, line_of_business,
                        status, effective_date, filed_date, filing_description,
                        raw_metadata, created_at, updated_at
                    ) VALUES (
                        :id, :serff, :naic, :carrier_name, :state,
                        :filing_type, :lob, :status,
                        :effective_date, :filed_date, :description,
                        :metadata::jsonb, NOW(), NOW()
                    )
                    ON CONFLICT (serff_tracking_number, state) DO UPDATE SET
                        carrier_naic_code   = EXCLUDED.carrier_naic_code,
                        carrier_name_filed  = EXCLUDED.carrier_name_filed,
                        filing_type         = EXCLUDED.filing_type,
                        line_of_business    = EXCLUDED.line_of_business,
                        status              = EXCLUDED.status,
                        effective_date      = EXCLUDED.effective_date,
                        filed_date          = EXCLUDED.filed_date,
                        filing_description  = EXCLUDED.filing_description,
                        raw_metadata        = EXCLUDED.raw_metadata,
                        updated_at          = NOW()
                    RETURNING id
                    """
                )

                row = await conn.execute(
                    filing_upsert,
                    {
                        "id": str(uuid.uuid4()),
                        "serff": filing.serff_tracking_number,
                        "naic": filing.carrier_naic,
                        "carrier_name": filing.carrier_name,
                        "state": state,
                        "filing_type": filing.filing_type,
                        "lob": filing.line_of_business,
                        "status": filing.status,
                        "effective_date": self._parse_date(filing.effective_date),
                        "filed_date": self._parse_date(filing.filed_date),
                        "description": filing.description,
                        "metadata": json.dumps(filing.raw_metadata),
                    },
                )
                filing_id = row.scalar_one()
                logger.debug(
                    "Upserted hermes_filings id=%s for %s",
                    filing_id,
                    filing.serff_tracking_number,
                )

                # ── Insert hermes_filing_documents ────────────────
                for url in filing.document_urls:
                    doc_name = url.split("/")[-1].split("?")[0] or "document"
                    dest_path = self._build_file_path(filing, doc_name)

                    file_size: int | None = None
                    checksum: str | None = None
                    if dest_path.exists():
                        file_size = dest_path.stat().st_size
                        checksum = self._sha256(dest_path)

                    doc_insert = text(
                        """
                        INSERT INTO hermes_filing_documents (
                            id, filing_id, document_name, file_path,
                            file_size_bytes, checksum_sha256, download_url,
                            created_at, updated_at
                        ) VALUES (
                            :id, :filing_id, :doc_name, :file_path,
                            :file_size, :checksum, :download_url,
                            NOW(), NOW()
                        )
                        ON CONFLICT DO NOTHING
                        """
                    )
                    await conn.execute(
                        doc_insert,
                        {
                            "id": str(uuid.uuid4()),
                            "filing_id": filing_id,
                            "doc_name": doc_name,
                            "file_path": str(dest_path) if dest_path.exists() else None,
                            "file_size": file_size,
                            "checksum": checksum,
                            "download_url": url,
                        },
                    )

            logger.info(
                "Stored metadata for filing %s (%d docs)",
                filing.serff_tracking_number,
                len(filing.document_urls),
            )

        except Exception as exc:
            logger.error(
                "Failed to store metadata for %s: %s",
                filing.serff_tracking_number,
                exc,
            )
            raise

    # ------------------------------------------------------------------
    # Private utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_name(value: str) -> str:
        """Sanitise *value* for use as a filesystem path component.

        Replaces characters that are invalid or inconvenient on Windows/Linux
        with underscores and strips leading/trailing whitespace.

        Parameters
        ----------
        value:
            Raw string to sanitise.

        Returns
        -------
        str
            Filesystem-safe string.
        """
        return "".join(
            c if c.isalnum() or c in ("-", "_", ".") else "_"
            for c in (value or "unknown").strip()
        )

    @staticmethod
    def _parse_date(value: str | None) -> date | None:
        """Attempt to parse a date string from common US filing formats.

        Tries ``MM/DD/YYYY``, ``YYYY-MM-DD``, and ``MM-DD-YYYY``.

        Parameters
        ----------
        value:
            Raw date string or ``None``.

        Returns
        -------
        date | None
            Parsed ``datetime.date`` or ``None`` if unparseable.
        """
        if not value:
            return None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(value.strip(), fmt).date()
            except ValueError:
                continue
        logger.debug("Could not parse date: %r", value)
        return None

    @staticmethod
    def _sha256(path: Path) -> str:
        """Compute the SHA-256 hex digest of a file.

        Parameters
        ----------
        path:
            Path to the file.

        Returns
        -------
        str
            64-character hex string.
        """
        h = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
