"""Abstract base scraper for SERFF Filing Access portals.

All state-specific scrapers extend ``BaseSERFFScraper``.  The base class owns
the Playwright browser lifecycle, session management, retry logic, rate
limiting, pagination orchestration, and scrape-log persistence.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.async_api import Browser, BrowserContext, Playwright, async_playwright
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from hermes.config import Settings, settings

logger = logging.getLogger("hermes.scraper")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class SearchParams(BaseModel):
    """Parameters used to drive a SERFF portal search."""

    state: str = Field(..., description="Two-letter state code, e.g. 'TX'")
    line_of_business: str | None = Field(
        None, description="Line of business filter, e.g. 'Commercial Auto'"
    )
    carrier_naic: str | None = Field(None, description="Carrier NAIC number")
    carrier_name: str | None = Field(None, description="Carrier legal name (partial OK)")
    filing_type: str | None = Field(
        None, description="Filing type: rate, rule, form, combination"
    )
    status: str | None = Field(
        None, description="Filing status: approved, pending, withdrawn, etc."
    )
    date_from: str | None = Field(None, description="Filed-date range start (MM/DD/YYYY)")
    date_to: str | None = Field(None, description="Filed-date range end (MM/DD/YYYY)")
    max_pages: int = Field(
        default=50, description="Maximum results pages to scrape (safety cap)"
    )


class FilingResult(BaseModel):
    """A single filing record parsed from a SERFF portal results table."""

    serff_tracking_number: str
    carrier_name: str
    carrier_naic: str | None = None
    filing_type: str | None = None
    line_of_business: str | None = None
    status: str | None = None
    effective_date: str | None = None
    filed_date: str | None = None
    description: str | None = None
    document_urls: list[str] = Field(default_factory=list)
    raw_metadata: dict[str, Any] = Field(default_factory=dict)


class ScrapeResult(BaseModel):
    """Summary of a completed scrape run."""

    state: str
    filings_found: int = 0
    filings_new: int = 0
    documents_downloaded: int = 0
    errors: list[str] = Field(default_factory=list)
    duration_seconds: float = 0.0


# ---------------------------------------------------------------------------
# Base scraper
# ---------------------------------------------------------------------------


class BaseSERFFScraper(ABC):
    """Abstract base class for all state SERFF Filing Access scrapers.

    Subclasses must implement :meth:`search_filings` with state-specific
    selector logic.  All other lifecycle steps (browser init, agreement
    acceptance, pagination, download, DB logging) are handled here.

    Parameters
    ----------
    state:
        Two-letter state code (e.g. ``"TX"``).
    config:
        Hermes ``Settings`` instance.  Defaults to the module-level singleton.
    """

    def __init__(self, state: str, config: Settings = settings) -> None:
        self.state = state.upper()
        self.config = config
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._engine: AsyncEngine | None = None

        # Ensure local storage directories exist.
        self._storage_root = Path(config.filing_storage_path)
        state_dir = self._storage_root / self.state
        state_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Initialised %s for state=%s", self.__class__.__name__, self.state)

    # ------------------------------------------------------------------
    # Browser lifecycle
    # ------------------------------------------------------------------

    async def init_browser(self) -> None:
        """Launch a headless Chromium browser via Playwright."""
        import os
        logger.debug("Launching Playwright Chromium (headless)")
        self._playwright = await async_playwright().start()
        # Use SOCKS proxy if HERMES_SOCKS_PROXY is set (for SSH tunnel routing)
        socks_proxy = os.environ.get("HERMES_SOCKS_PROXY")
        use_xvfb = bool(os.environ.get("DISPLAY"))
        launch_kwargs = dict(
            headless=not use_xvfb,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        if socks_proxy:
            launch_kwargs["proxy"] = {"server": socks_proxy}
            logger.info("Using SOCKS proxy: %s", socks_proxy)
        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        self._context = await self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            accept_downloads=True,
        )
        # Set a global navigation timeout from settings.
        self._context.set_default_navigation_timeout(
            self.config.scrape_session_timeout * 1000
        )
        logger.info("Browser ready")

    async def close(self) -> None:
        """Tear down browser and Playwright cleanly."""
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        if self._engine:
            await self._engine.dispose()
        logger.info("Browser closed")

    # ------------------------------------------------------------------
    # Portal navigation helpers
    # ------------------------------------------------------------------

    async def accept_user_agreement(self, page: Any) -> None:
        """Click through the SERFF Filing Access user-agreement page.

        SFA presents a disclaimer that must be acknowledged before searches
        are accessible.  This method locates the accept/agree button and
        waits for the subsequent navigation to complete.

        Parameters
        ----------
        page:
            Playwright ``Page`` object already navigated to the SFA portal.
        """
        logger.debug("Looking for user-agreement accept button")
        # Common button texts used across state portals.
        accept_selectors = [
            "input[value*='Accept']",
            "input[value*='Agree']",
            "input[value*='I Agree']",
            "button:has-text('Accept')",
            "button:has-text('Agree')",
            "a:has-text('Accept')",
            "a:has-text('Agree')",
        ]
        for selector in accept_selectors:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=3000):
                    logger.info("Accepting user agreement via selector: %s", selector)
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    return
            except Exception:
                continue

        logger.warning(
            "User-agreement button not found; page may already be past agreement"
        )

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def search_filings(
        self, page: Any, params: SearchParams
    ) -> list[FilingResult]:
        """Execute the state-specific portal search and return all results.

        Implementations should handle pagination internally (or defer to
        :meth:`scrape` for page-level iteration).

        Parameters
        ----------
        page:
            An open Playwright ``Page`` after the user agreement has been
            accepted.
        params:
            Search parameters supplied by the caller.

        Returns
        -------
        list[FilingResult]
            All filing records found across all pages.
        """

    # ------------------------------------------------------------------
    # Document download
    # ------------------------------------------------------------------

    async def download_filing_documents(
        self, page: Any, filing: FilingResult
    ) -> list[str]:
        """Download all PDFs associated with *filing* and return local paths.

        Uses Playwright's download API to capture each file to the carrier's
        organised storage directory.

        Parameters
        ----------
        page:
            Playwright ``Page`` (browser context must have ``accept_downloads``
            enabled, which :meth:`init_browser` sets).
        filing:
            The filing whose ``document_urls`` should be downloaded.

        Returns
        -------
        list[str]
            Absolute local file paths of every successfully saved document.
        """
        saved_paths: list[str] = []
        carrier_dir = self._get_storage_path(filing.carrier_name)
        filing_dir = carrier_dir / (filing.serff_tracking_number or "unknown")
        filing_dir.mkdir(parents=True, exist_ok=True)

        for url in filing.document_urls:
            try:
                dest_name = url.split("/")[-1].split("?")[0] or "document.pdf"
                dest_path = filing_dir / dest_name

                if dest_path.exists():
                    logger.debug("Already downloaded: %s", dest_path)
                    saved_paths.append(str(dest_path))
                    continue

                await self._rate_limit()
                logger.info("Downloading %s -> %s", url, dest_path)

                async with page.expect_download() as dl_info:
                    await page.goto(url, wait_until="commit")
                download = await dl_info.value
                await download.save_as(str(dest_path))
                saved_paths.append(str(dest_path))
                logger.debug("Saved: %s (%d B)", dest_path, dest_path.stat().st_size)

            except Exception as exc:
                logger.error(
                    "Failed to download %s for filing %s: %s",
                    url,
                    filing.serff_tracking_number,
                    exc,
                )

        return saved_paths

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def scrape(self, params: SearchParams) -> ScrapeResult:
        """Execute a full scrape run: init browser, search, paginate, download.

        This is the primary public method.  It handles the complete lifecycle:

        1. Launches the browser.
        2. Navigates to the state portal and accepts the user agreement.
        3. Delegates to :meth:`search_filings` (state-specific).
        4. Downloads documents for each filing.
        5. Persists a scrape-log record via :meth:`_log_scrape`.
        6. Closes the browser.

        Parameters
        ----------
        params:
            Search parameters.

        Returns
        -------
        ScrapeResult
            Summary of the completed run including counts and any errors.
        """
        result = ScrapeResult(state=self.state)
        start = time.monotonic()

        try:
            await self.init_browser()
            assert self._context is not None
            page = await self._context.new_page()

            # Navigate to state portal with retry.
            portal_url = f"{self.config.serff_base_url}/sfa/search/{self.state}"
            await self._navigate_with_retry(page, portal_url)
            await self.accept_user_agreement(page)

            # Run state-specific search logic.
            logger.info(
                "Starting search: state=%s lob=%s carrier_naic=%s",
                self.state,
                params.line_of_business,
                params.carrier_naic,
            )
            filings = await self.search_filings(page, params)
            result.filings_found = len(filings)
            logger.info("Found %d filings", result.filings_found)

            # Download documents for each filing.
            for filing in filings:
                await self._rate_limit()
                try:
                    paths = await self.download_filing_documents(page, filing)
                    result.documents_downloaded += len(paths)
                    if paths:
                        result.filings_new += 1
                except Exception as exc:
                    msg = (
                        f"Document download error for {filing.serff_tracking_number}: {exc}"
                    )
                    logger.error(msg)
                    result.errors.append(msg)

        except Exception as exc:
            msg = f"Scrape failed for state={self.state}: {exc}"
            logger.exception(msg)
            result.errors.append(msg)

        finally:
            result.duration_seconds = round(time.monotonic() - start, 2)
            await self._log_scrape(result, params)
            await self.close()

        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_storage_path(self, carrier_name: str) -> Path:
        """Return the base storage directory for a specific carrier.

        Creates the directory tree if it does not already exist.

        Parameters
        ----------
        carrier_name:
            Raw carrier name string (will be sanitised for filesystem use).

        Returns
        -------
        Path
            ``{filing_storage_path}/{state}/{sanitised_carrier_name}/``
        """
        safe_name = "".join(
            c if c.isalnum() or c in (" ", "-", "_") else "_"
            for c in (carrier_name or "unknown")
        ).strip()[:100]
        path = self._storage_root / self.state / safe_name
        path.mkdir(parents=True, exist_ok=True)
        return path

    async def _rate_limit(self) -> None:
        """Pause for the configured delay between requests."""
        delay = self.config.scrape_delay_seconds
        if delay > 0:
            logger.debug("Rate limit: sleeping %.1fs", delay)
            await asyncio.sleep(delay)

    async def _navigate_with_retry(self, page: Any, url: str) -> None:
        """Navigate to *url* with exponential-backoff retry via tenacity.

        Parameters
        ----------
        page:
            Playwright ``Page``.
        url:
            Full URL to navigate to.

        Raises
        ------
        RetryError
            If all retry attempts are exhausted.
        """
        try:
            async for attempt in AsyncRetrying(
                retry=retry_if_exception_type(Exception),
                stop=stop_after_attempt(self.config.scrape_max_retries),
                wait=wait_exponential(multiplier=2, min=2, max=30),
                reraise=True,
            ):
                with attempt:
                    logger.debug(
                        "Navigating to %s (attempt %d)",
                        url,
                        attempt.retry_state.attempt_number,
                    )
                    await page.goto(url, wait_until="networkidle", timeout=30000)
        except RetryError as exc:
            logger.error("Navigation to %s failed after retries: %s", url, exc)
            raise

    async def _get_engine(self) -> AsyncEngine:
        """Return (creating if necessary) the shared SQLAlchemy async engine."""
        if self._engine is None:
            self._engine = create_async_engine(
                self.config.database_url,
                pool_size=2,
                max_overflow=0,
                echo=False,
            )
        return self._engine

    async def _log_scrape(self, result: ScrapeResult, params: SearchParams) -> None:
        """Persist a scrape-log row to ``hermes_scrape_log``.

        Parameters
        ----------
        result:
            The completed :class:`ScrapeResult`.
        params:
            The :class:`SearchParams` used for this run (stored as JSONB).
        """
        try:
            engine = await self._get_engine()
            now = datetime.now(timezone.utc)
            started_at = now  # approximate; real timing tracked via duration
            completed_at = now

            log_status = "failed" if result.errors else "completed"
            error_message = "; ".join(result.errors) if result.errors else None

            stmt = text(
                """
                INSERT INTO hermes_scrape_log (
                    id, state, line, carrier_naic, search_params,
                    filings_found, filings_new, documents_downloaded,
                    errors, started_at, completed_at, duration_seconds,
                    status, error_message, scrape_type
                ) VALUES (
                    :id, :state, :line, :carrier_naic,
                    CAST(:search_params AS jsonb),
                    :filings_found, :filings_new, :documents_downloaded,
                    CAST(:errors AS jsonb),
                    :started_at, :completed_at, :duration_seconds,
                    :status, :error_message, 'incremental'
                )
                """
            )

            import json

            async with engine.begin() as conn:
                await conn.execute(
                    stmt,
                    {
                        "id": str(uuid.uuid4()),
                        "state": result.state,
                        "line": params.line_of_business,
                        "carrier_naic": params.carrier_naic,
                        "search_params": json.dumps(params.model_dump()),
                        "filings_found": result.filings_found,
                        "filings_new": result.filings_new,
                        "documents_downloaded": result.documents_downloaded,
                        "errors": json.dumps(result.errors),
                        "started_at": started_at,
                        "completed_at": completed_at,
                        "duration_seconds": result.duration_seconds,
                        "status": log_status,
                        "error_message": error_message,
                    },
                )
            logger.info("Scrape log written: status=%s", log_status)

        except Exception as exc:
            # Logging failure must never mask the scrape result.
            logger.error("Failed to write scrape log: %s", exc)
