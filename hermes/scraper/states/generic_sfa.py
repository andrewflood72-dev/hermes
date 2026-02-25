"""Generic SERFF Filing Access scraper — works for any state.

The SERFF SFA portal uses the same PrimeFaces JSF components across all states.
This scraper handles the common interaction pattern:
  Home → Begin Search → Accept Agreement → Search Form → Results

State-specific scrapers can subclass this and override behavior if needed,
but the default implementation works for CA, TX, NY, FL and most other states.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
import uuid
from datetime import date as _date, datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import create_async_engine

from hermes.config import settings
from hermes.scraper.base import (
    BaseSERFFScraper,
    FilingResult,
    ScrapeResult,
    SearchParams,
)

logger = logging.getLogger("hermes.scraper")

# Default page size (SERFF SFA shows 20 rows per page)
SFA_PAGE_SIZE = 20


class GenericSFAScraper(BaseSERFFScraper):
    """Generic SERFF Filing Access scraper that works for any state.

    The SERFF SFA portal at filingaccess.serff.com uses identical PrimeFaces
    components across all states. This scraper handles:
    - PrimeFaces selectOneMenu interaction (click trigger → click option)
    - PrimeFaces blockUI overlay waits
    - DataTable results parsing
    - PrimeFaces paginator navigation
    - Per-filing transaction persistence to hermes_filings
    """

    SFA_HOME_URL_TEMPLATE = "https://filingaccess.serff.com/sfa/home/{state}"

    def __init__(self, state: str, config=settings) -> None:
        super().__init__(state=state, config=config)

    # ------------------------------------------------------------------
    # Override scrape() for SFA-specific navigation
    # ------------------------------------------------------------------

    async def scrape(self, params: SearchParams) -> ScrapeResult:
        """Full scrape: navigate, search, parse all pages, persist to DB."""
        result = ScrapeResult(state=self.state)
        start = time.monotonic()

        try:
            await self.init_browser()
            assert self._context is not None
            page = await self._context.new_page()

            # Step 1: Navigate to search form
            await self._navigate_to_search_form(page)

            # Step 2: Fill and submit the search form
            await self._fill_and_submit_search(page, params)

            # Step 3: Check if we landed on results page
            if "filingSearchResults" not in page.url:
                logger.warning(
                    "Did not reach results page. URL: %s", page.url
                )
                result.errors.append("Search did not produce results page")
                return result

            # Step 4: Parse all result pages
            all_filings = await self._parse_all_result_pages(page, params.max_pages)
            result.filings_found = len(all_filings)
            logger.info("[%s] Parsed %d total filings from SERFF SFA", self.state, len(all_filings))

            # Step 5: Persist to database
            if all_filings:
                result.filings_new = await self._persist_filings(all_filings)
                logger.info("[%s] Persisted %d new/updated filings to DB", self.state, result.filings_new)

        except Exception as exc:
            msg = f"{self.state} scrape failed: {exc}"
            logger.exception(msg)
            result.errors.append(msg)

        finally:
            result.duration_seconds = round(time.monotonic() - start, 2)
            await self._log_scrape_safe(result, params)
            await self.close()

        return result

    # ------------------------------------------------------------------
    # search_filings (required by ABC)
    # ------------------------------------------------------------------

    async def search_filings(
        self, page: Any, params: SearchParams
    ) -> list[FilingResult]:
        """Implement abstract method — delegates to scrape() workflow."""
        await self._navigate_to_search_form(page)
        await self._fill_and_submit_search(page, params)
        if "filingSearchResults" not in page.url:
            return []
        return await self._parse_all_result_pages(page, params.max_pages)

    # ------------------------------------------------------------------
    # Navigation: Home → Agreement → Search Form
    # ------------------------------------------------------------------

    async def _navigate_to_search_form(self, page: Any) -> None:
        """Navigate the SFA portal from Home through Agreement to Search Form."""
        home_url = self.SFA_HOME_URL_TEMPLATE.format(state=self.state)
        logger.info("[%s] Navigating to SFA search form", self.state)

        # Step 1: Load home page (establishes session)
        await self._navigate_with_retry(page, home_url)

        # Step 2: Click "Begin Search" to reach user agreement
        try:
            # Try link first, then button with text
            begin_selectors = [
                "a[href='/sfa/userAgreement.xhtml']",
                "button:has-text('Begin Search')",
                "a:has-text('Begin Search')",
                "input[value='Begin Search']",
            ]
            clicked = False
            for sel in begin_selectors:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=3000):
                        await btn.click()
                        await page.wait_for_load_state("networkidle", timeout=15000)
                        clicked = True
                        break
                except Exception:
                    continue
            if not clicked:
                raise Exception("No Begin Search element found")
        except Exception:
            logger.warning("[%s] Could not click Begin Search — trying direct URL", self.state)
            await self._navigate_with_retry(
                page, f"https://filingaccess.serff.com/sfa/home/{self.state}"
            )
            # Try one more time after reload
            try:
                await page.click("button:has-text('Begin Search')", timeout=5000)
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

        # Step 3: Accept user agreement (try multiple selectors)
        accepted = False
        accept_selectors = [
            "button:has-text('Accept')",
            ".ui-button:has-text('Accept')",
            "input[value='Accept']",
            "a:has-text('Accept')",
            "button:has-text('Agree')",
            "button:has-text('I Agree')",
        ]
        for selector in accept_selectors:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=3000):
                    btn_text = (await btn.text_content() or "").strip()
                    logger.info("[%s] Accepting user agreement via '%s': [%s]", self.state, selector, btn_text)
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=20000)
                    await asyncio.sleep(2)
                    accepted = True
                    break
            except Exception:
                continue
        if not accepted:
            logger.warning("[%s] Could not find Accept button on agreement page", self.state)

        logger.info("[%s] On search form: %s", self.state, page.url)

    # ------------------------------------------------------------------
    # Fill and submit search form
    # ------------------------------------------------------------------

    async def _fill_and_submit_search(
        self, page: Any, params: SearchParams
    ) -> None:
        """Fill PrimeFaces search form and submit."""
        logger.info("[%s] Filling search form: lob=%s naic=%s", self.state, params.line_of_business, params.carrier_naic)

        # 1. Select Business Type = "Property & Casualty"
        #    Must interact with PrimeFaces selectOneMenu through its UI
        try:
            trigger = page.locator(".ui-selectonemenu-trigger").first
            await trigger.click(timeout=5000)
            await asyncio.sleep(1)

            panel = page.locator(".ui-selectonemenu-panel").first
            items = await panel.locator("li").all()
            for item in items:
                text = (await item.text_content()).strip()
                if "property" in text.lower():
                    await item.click()
                    break

            await page.wait_for_load_state("networkidle", timeout=15000)
            await asyncio.sleep(2)
            logger.info("[%s] Selected Business Type: Property & Casualty via UI", self.state)
        except Exception as exc:
            logger.warning("[%s] Could not set Business Type via UI: %s", self.state, exc)

        # Wait for PrimeFaces blockUI overlay to clear
        await self._wait_for_blockui(page)

        # 2. Fill Company NAIC Code
        if params.carrier_naic:
            try:
                await page.fill(
                    "input[id='simpleSearch:companyCode']",
                    params.carrier_naic,
                    timeout=3000,
                )
            except Exception:
                pass

        # 3. Fill Company Name
        if params.carrier_name:
            try:
                await page.fill(
                    "input[id='simpleSearch:companyName']",
                    params.carrier_name,
                    timeout=3000,
                )
            except Exception:
                pass

        # 4. Fill Insurance Product Name
        if params.line_of_business:
            try:
                await page.fill(
                    "input[id='simpleSearch:productName']",
                    params.line_of_business,
                    timeout=3000,
                )
            except Exception:
                pass

        # 5. Fill Start Submission Date
        #    SERFF requires at least one field beyond Business Type,
        #    so default to 24 months ago if nothing else is provided.
        date_from = params.date_from
        if not date_from and not params.carrier_naic and not params.carrier_name and not params.line_of_business:
            from datetime import timedelta
            fallback = _date.today() - timedelta(days=730)
            date_from = fallback.strftime("%m/%d/%Y")
            logger.info("[%s] No filters provided — defaulting start date to %s", self.state, date_from)
        if date_from:
            try:
                await page.fill(
                    "input[id='simpleSearch:submissionStartDate_input']",
                    date_from,
                    timeout=3000,
                )
            except Exception:
                pass

        # 6. Verify fields before submitting
        bt_val = await page.evaluate(
            "() => document.getElementById('simpleSearch:businessType_input')?.value"
        )
        pn_val = await page.evaluate(
            "() => document.getElementById('simpleSearch:productName')?.value"
        )
        logger.info("[%s] Pre-submit: businessType=%s product=%s", self.state, bt_val, pn_val)

        # 7. Submit the search
        await self._wait_for_blockui(page)
        logger.info("[%s] Clicking Search button...", self.state)

        await page.click("button[id='simpleSearch:saveBtn']", timeout=10000)

        try:
            await page.wait_for_url("**/filingSearchResults**", timeout=60000)
        except Exception:
            await asyncio.sleep(5)

        await asyncio.sleep(2)
        logger.info("[%s] After submit: %s", self.state, page.url)

    # ------------------------------------------------------------------
    # Parse all result pages
    # ------------------------------------------------------------------

    async def _parse_all_result_pages(
        self, page: Any, max_pages: int
    ) -> list[FilingResult]:
        """Parse all result pages from the PrimeFaces DataTable."""
        all_filings: list[FilingResult] = []
        page_num = 1

        while page_num <= max_pages:
            logger.info("[%s] Parsing results page %d", self.state, page_num)

            page_filings = await self._parse_results_table(page)
            if not page_filings:
                logger.info("[%s] No filings on page %d — stopping", self.state, page_num)
                break

            all_filings.extend(page_filings)
            logger.info(
                "[%s] Page %d: %d filings (total so far: %d)",
                self.state, page_num, len(page_filings), len(all_filings),
            )

            if len(page_filings) < SFA_PAGE_SIZE:
                break

            if not await self._click_next_page(page):
                break

            page_num += 1

        return all_filings

    # ------------------------------------------------------------------
    # Results table parsing
    # ------------------------------------------------------------------

    async def _parse_results_table(self, page: Any) -> list[FilingResult]:
        """Parse the PrimeFaces DataTable on the current results page."""
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        datatable = soup.find("table", class_="ui-datatable-data") or soup.find(
            "tbody", class_="ui-datatable-data"
        )

        if datatable is None:
            for table in soup.find_all("table"):
                header_text = " ".join(
                    th.get_text(strip=True).lower()
                    for th in table.find_all(["th", "td"])[:10]
                )
                if "serff" in header_text and "company" in header_text:
                    datatable = table
                    break

        if datatable is None:
            logger.warning("[%s] No results table found on page", self.state)
            return []

        # Find header row
        page_soup_tables = soup.find_all("table")
        headers: list[str] = []
        for table in page_soup_tables:
            thead = table.find("thead")
            if thead:
                header_cells = thead.find_all("th")
                if header_cells:
                    headers = [th.get_text(strip=True).lower() for th in header_cells]
                    if any("serff" in h or "company" in h for h in headers):
                        break
                    headers = []

        if not headers:
            rows = datatable.find_all("tr")
            if rows:
                first_cells = rows[0].find_all(["th", "td"])
                headers = [c.get_text(strip=True).lower() for c in first_cells]

        if not headers:
            logger.warning("[%s] Could not find column headers", self.state)
            return []

        col_map = self._map_serff_columns(headers)
        logger.debug("[%s] Column mapping: %s from headers: %s", self.state, col_map, headers)

        data_rows = datatable.find_all("tr")
        results: list[FilingResult] = []

        for row in data_rows:
            cells = row.find_all("td")
            if not cells or len(cells) < 3:
                continue

            filing = self._parse_filing_row(cells, col_map)
            if filing:
                results.append(filing)

        logger.info("[%s] Parsed %d filings from results table", self.state, len(results))
        return results

    def _map_serff_columns(self, headers: list[str]) -> dict[str, int]:
        """Map actual SERFF SFA column headers to semantic names."""
        mapping: dict[str, int] = {}
        for i, h in enumerate(headers):
            h_lower = h.strip().lower()
            if "serff" in h_lower or "tracking" in h_lower:
                mapping.setdefault("serff", i)
            elif "naic" in h_lower and "company" in h_lower:
                mapping.setdefault("naic", i)
            elif "naic" in h_lower:
                mapping.setdefault("naic", i)
            elif "company name" in h_lower:
                mapping.setdefault("carrier", i)
            elif "company" in h_lower and "carrier" not in mapping:
                mapping.setdefault("carrier", i)
            elif "insurance product" in h_lower or "product name" in h_lower:
                mapping.setdefault("product", i)
            elif "sub type" in h_lower:
                mapping.setdefault("sub_type", i)
            elif "filing type" in h_lower:
                mapping.setdefault("filing_type", i)
            elif "type" in h_lower and "filing_type" not in mapping and "sub_type" not in mapping:
                mapping.setdefault("filing_type", i)
            elif "filing status" in h_lower or "status" in h_lower:
                mapping.setdefault("status", i)
            elif "effective" in h_lower:
                mapping.setdefault("effective", i)
        return mapping

    def _parse_filing_row(
        self, cells: list[Any], col_map: dict[str, int]
    ) -> FilingResult | None:
        """Convert a table row into a FilingResult."""

        def _cell(key: str) -> str:
            idx = col_map.get(key)
            if idx is None or idx >= len(cells):
                return ""
            return cells[idx].get_text(strip=True)

        serff_num = _cell("serff")
        if not serff_num:
            return None

        carrier_name = _cell("carrier")
        naic = _cell("naic")
        product_name = _cell("product")
        sub_type = _cell("sub_type")
        filing_type = _cell("filing_type")
        status = _cell("status")

        status_normalized = self._normalize_status(status)
        filing_type_normalized = self._normalize_filing_type(filing_type)

        return FilingResult(
            serff_tracking_number=serff_num,
            carrier_name=carrier_name,
            carrier_naic=naic or None,
            filing_type=filing_type_normalized,
            line_of_business=product_name or None,
            status=status_normalized,
            effective_date=_cell("effective") or None,
            raw_metadata={
                "state": self.state,
                "sub_type_of_insurance": sub_type,
                "filing_type_raw": filing_type,
                "filing_status_raw": status,
                "product_name": product_name,
            },
        )

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    async def _click_next_page(self, page: Any) -> bool:
        """Click the next page link in the PrimeFaces paginator."""
        try:
            next_btn = page.locator(
                ".ui-paginator-next:not(.ui-state-disabled)"
            ).first
            if await next_btn.is_visible(timeout=3000):
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=30000)
                await asyncio.sleep(2)
                return True
        except Exception:
            pass

        try:
            next_link = page.locator("a:has-text('>')").first
            if await next_link.is_visible(timeout=2000):
                await next_link.click()
                await page.wait_for_load_state("networkidle", timeout=30000)
                await asyncio.sleep(2)
                return True
        except Exception:
            pass

        logger.info("[%s] No next page link found — end of results", self.state)
        return False

    # ------------------------------------------------------------------
    # Database persistence
    # ------------------------------------------------------------------

    async def _persist_filings(self, filings: list[FilingResult]) -> int:
        """Persist scraped filings to hermes_filings and hermes_carriers tables.

        Each filing is persisted in its own transaction to avoid one failure
        poisoning the entire batch.
        """
        engine = await self._get_engine()
        persisted = 0

        for filing in filings:
            try:
                async with engine.begin() as conn:
                    carrier_id = None
                    if filing.carrier_naic:
                        carrier_id = await self._upsert_carrier(
                            conn, filing.carrier_naic, filing.carrier_name
                        )

                    # Truncate values to fit column constraints
                    sub_line = (filing.raw_metadata.get("sub_type_of_insurance") or "")[:100]
                    lob = (filing.line_of_business or "unknown")[:100]
                    product_name = (filing.line_of_business or "")[:500]
                    carrier_name = (filing.carrier_name or "")[:500]
                    filing_type = (filing.filing_type or "unknown")[:20]
                    status = (filing.status or "pending")[:20]

                    await conn.execute(
                        sa_text("""
                            INSERT INTO hermes_filings (
                                id, serff_tracking_number, carrier_id,
                                carrier_naic_code, carrier_name_filed,
                                state, filing_type, line_of_business,
                                sub_line, product_name, status,
                                effective_date, raw_metadata
                            ) VALUES (
                                :id, :serff, :carrier_id,
                                :naic, :carrier_name,
                                :state, :filing_type, :lob,
                                :sub_line, :product_name, :status,
                                :effective_date,
                                CAST(:raw_metadata AS jsonb)
                            )
                            ON CONFLICT (serff_tracking_number, state)
                            DO UPDATE SET
                                carrier_id = COALESCE(EXCLUDED.carrier_id, hermes_filings.carrier_id),
                                carrier_naic_code = COALESCE(EXCLUDED.carrier_naic_code, hermes_filings.carrier_naic_code),
                                carrier_name_filed = COALESCE(EXCLUDED.carrier_name_filed, hermes_filings.carrier_name_filed),
                                filing_type = COALESCE(EXCLUDED.filing_type, hermes_filings.filing_type),
                                line_of_business = COALESCE(EXCLUDED.line_of_business, hermes_filings.line_of_business),
                                sub_line = COALESCE(EXCLUDED.sub_line, hermes_filings.sub_line),
                                product_name = COALESCE(EXCLUDED.product_name, hermes_filings.product_name),
                                status = COALESCE(EXCLUDED.status, hermes_filings.status),
                                effective_date = COALESCE(EXCLUDED.effective_date, hermes_filings.effective_date),
                                raw_metadata = hermes_filings.raw_metadata || EXCLUDED.raw_metadata,
                                updated_at = NOW()
                        """),
                        {
                            "id": str(uuid.uuid4()),
                            "serff": filing.serff_tracking_number[:50],
                            "carrier_id": str(carrier_id) if carrier_id else None,
                            "naic": (filing.carrier_naic or "")[:10] or None,
                            "carrier_name": carrier_name,
                            "state": self.state,
                            "filing_type": filing_type,
                            "lob": lob,
                            "sub_line": sub_line or None,
                            "product_name": product_name or None,
                            "status": status,
                            "effective_date": None,
                            "raw_metadata": json.dumps(filing.raw_metadata),
                        },
                    )
                    persisted += 1

            except Exception as exc:
                logger.error(
                    "[%s] Failed to persist filing %s: %s",
                    self.state, filing.serff_tracking_number, exc,
                )

        return persisted

    async def _upsert_carrier(
        self, conn: Any, naic_code: str, legal_name: str
    ) -> str | None:
        """Insert or find a carrier by NAIC code. Returns carrier UUID."""
        try:
            result = await conn.execute(
                sa_text("SELECT id FROM hermes_carriers WHERE naic_code = :naic"),
                {"naic": naic_code},
            )
            row = result.fetchone()
            if row:
                return str(row.id)

            carrier_id = str(uuid.uuid4())
            await conn.execute(
                sa_text("""
                    INSERT INTO hermes_carriers (id, naic_code, legal_name, domicile_state, status)
                    VALUES (:id, :naic, :name, :state, 'active')
                    ON CONFLICT (naic_code) DO UPDATE SET
                        legal_name = COALESCE(EXCLUDED.legal_name, hermes_carriers.legal_name),
                        updated_at = NOW()
                """),
                {"id": carrier_id, "naic": naic_code, "name": legal_name, "state": self.state},
            )
            return carrier_id

        except Exception as exc:
            logger.warning("[%s] Could not upsert carrier %s: %s", self.state, naic_code, exc)
            return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _wait_for_blockui(self, page: Any, timeout: int = 5000) -> None:
        """Wait for PrimeFaces blockUI overlay to disappear."""
        try:
            blocker = page.locator(".ui-blockui:not(.ui-helper-hidden)")
            await blocker.wait_for(state="hidden", timeout=timeout)
        except Exception:
            pass

    async def _log_scrape_safe(
        self, result: ScrapeResult, params: SearchParams
    ) -> None:
        """Log scrape result to DB with proper asyncpg parameter handling."""
        try:
            engine = await self._get_engine()
            from datetime import datetime, timezone

            now = datetime.now(timezone.utc)
            log_status = "failed" if result.errors else "completed"
            error_message = "; ".join(result.errors) if result.errors else None

            async with engine.begin() as conn:
                await conn.execute(
                    sa_text("""
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
                    """),
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
                        "started_at": now,
                        "completed_at": now,
                        "duration_seconds": result.duration_seconds,
                        "status": log_status,
                        "error_message": error_message,
                    },
                )
            logger.info("[%s] Scrape log written: status=%s", self.state, log_status)
        except Exception as exc:
            logger.error("[%s] Failed to write scrape log: %s", self.state, exc)

    @staticmethod
    def _normalize_status(raw_status: str) -> str:
        """Normalize SERFF filing status to DB-friendly values."""
        s = raw_status.lower().strip()
        if "approved" in s:
            return "approved"
        elif "acknowledged" in s:
            return "approved"
        elif "withdrawn" in s:
            return "withdrawn"
        elif "pending" in s:
            return "pending"
        elif "disapproved" in s or "rejected" in s:
            return "disapproved"
        elif "closed" in s:
            return "approved"
        else:
            return "pending"

    @staticmethod
    def _normalize_filing_type(raw_type: str) -> str:
        """Normalize SERFF filing type to DB-friendly values."""
        t = raw_type.lower().strip()
        if "rate" in t and "rule" in t and "form" in t:
            return "rate_rule_form"
        elif "rate" in t and "rule" in t:
            return "rate_rule"
        elif "rate" in t:
            return "rate"
        elif "rule" in t and "form" in t:
            return "rule_form"
        elif "rule" in t:
            return "rule"
        elif "form" in t:
            return "form"
        elif "program" in t:
            return "new_program"
        else:
            return t[:20] or "unknown"

    # ==================================================================
    # DETAIL PAGE SCRAPING — click into each filing for real data
    # ==================================================================

    # Keep low to avoid SERFF CAPTCHA rate limits + memory pressure.
    # SERFF blocks after ~30 rapid parallel requests.
    PARALLEL_TABS = 2
    # Restart browser every N filings to prevent memory/session issues
    BROWSER_RESTART_INTERVAL = 200

    async def scrape_filing_details(
        self,
        batch_size: int = 200,
        limit: int | None = None,
        skip_download: bool = False,
        download_only: bool = False,
    ) -> dict[str, int]:
        """Scrape detail data for every filing in our DB.

        DIRECT approach — no search/pagination needed:
        1. Load all SERFF tracking numbers from our DB
        2. Extract numeric filing ID from each (part after '-')
        3. Open filingSummary.xhtml?filingId={id} in parallel tabs
        4. Extract metadata, download docs, update DB

        Restarts browser every 200 filings to prevent memory leaks.

        If download_only=True, re-visit already-scraped filings that have
        metadata but no documents in hermes_filing_documents.
        """
        stats = {"scraped": 0, "documents": 0, "errors": 0, "skipped": 0}
        start = time.monotonic()

        # Load all filing data
        filing_map = await self._preload_filing_map()

        if download_only:
            # Only process filings that have metadata but no documents
            needs_docs = await self._get_filings_without_docs()
            work_queue = []
            for serff_num, filing_id in filing_map.items():
                if filing_id not in needs_docs:
                    stats["skipped"] += 1
                    continue
                parts = serff_num.split("-", 1)
                if len(parts) != 2 or not parts[1]:
                    continue
                numeric_id = parts[1].lstrip("G")
                if not numeric_id.isdigit():
                    stats["skipped"] += 1
                    continue
                # Skip G-prefix filings — they are restricted group filings
                if "-G" in serff_num:
                    stats["skipped"] += 1
                    continue
                work_queue.append({
                    "serff_num": serff_num,
                    "filing_id": filing_id,
                    "numeric_id": numeric_id,
                })
            skip_download = False  # Force downloads on
        else:
            scraped_set = await self._get_scraped_serff_nums()
            work_queue = []
            for serff_num, filing_id in filing_map.items():
                if serff_num in scraped_set:
                    stats["skipped"] += 1
                    continue
                parts = serff_num.split("-", 1)
                if len(parts) != 2 or not parts[1]:
                    continue
                numeric_id = parts[1].lstrip("G")
                if not numeric_id.isdigit():
                    stats["skipped"] += 1
                    continue
                work_queue.append({
                    "serff_num": serff_num,
                    "filing_id": filing_id,
                    "numeric_id": numeric_id,
                })

        if limit:
            work_queue = work_queue[:limit]

        total_work = len(work_queue)
        already_done = len(filing_map) - total_work - stats["skipped"]
        logger.info(
            "[%s] %d filings in DB, %d already done, %d to process%s",
            self.state, len(filing_map), already_done, total_work,
            " (download-only)" if download_only else "",
        )

        if total_work == 0:
            logger.info("[%s] Nothing to scrape", self.state)
            return stats

        detail_base_url = (
            "https://filingaccess.serff.com/sfa/search/filingSummary.xhtml"
        )

        # Process all items using a pointer into work_queue
        idx = 0
        consecutive_errors = 0
        session_restarts = 0

        while idx < total_work:
            session_count = 0
            session_expired = False

            try:
                await self.init_browser()
                assert self._context is not None
                page = await self._context.new_page()
                await self._establish_sfa_session(page)

                pending_updates: list[dict] = []

                while idx < total_work and session_count < self.BROWSER_RESTART_INTERVAL:
                    # Build a batch of PARALLEL_TABS items
                    batch_end = min(idx + self.PARALLEL_TABS, total_work)
                    batch = work_queue[idx:batch_end]

                    tasks = [
                        self._process_direct_detail(
                            detail_base_url, item["numeric_id"],
                            item["serff_num"], item["filing_id"],
                            skip_download, download_only,
                        )
                        for item in batch
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    batch_ok = 0
                    for result in results:
                        if isinstance(result, Exception) or result is None:
                            stats["errors"] += 1
                            consecutive_errors += 1
                        elif result.get("error_type"):
                            # Permanently failed — mark in DB but count as "processed"
                            pending_updates.append(result)
                            stats["errors"] += 1
                            # Don't increment consecutive_errors for permanent failures
                        else:
                            pending_updates.append(result)
                            stats["scraped"] += 1
                            stats["documents"] += result.get("doc_count", 0)
                            consecutive_errors = 0
                            batch_ok += 1

                    idx += len(batch)
                    session_count += len(batch)

                    # Batch write every 20 filings
                    if len(pending_updates) >= 20:
                        await self._batch_update_filings(pending_updates)
                        pending_updates.clear()

                    # Session expired: 18+ consecutive errors = break and restart
                    if consecutive_errors >= 18:
                        # Rewind index so failed items get retried with fresh session
                        rewind = min(consecutive_errors, 18)
                        idx = max(0, idx - rewind)
                        logger.warning(
                            "[%s] %d consecutive errors — restarting browser "
                            "(rewinding %d items)",
                            self.state, consecutive_errors, rewind,
                        )
                        consecutive_errors = 0
                        session_expired = True
                        session_restarts += 1
                        # If we've restarted 3+ times in a row, skip ahead
                        if session_restarts >= 3:
                            logger.warning(
                                "[%s] %d restarts — these filings may be "
                                "genuinely inaccessible, skipping batch",
                                self.state, session_restarts,
                            )
                            idx += 18  # Skip the problematic batch
                            session_restarts = 0
                        break

                    session_restarts = 0  # Reset on successful batches

                    # Delay between requests to avoid SERFF CAPTCHA (3s per filing)
                    await asyncio.sleep(3)

                # Flush remaining
                if pending_updates:
                    await self._batch_update_filings(pending_updates)

            except Exception as exc:
                logger.error(
                    "[%s] Browser session failed: %s", self.state, exc, exc_info=True
                )
                stats["errors"] += 1

            finally:
                await self.close()

            # Progress after each browser session
            elapsed = round(time.monotonic() - start, 1)
            rate = stats["scraped"] / max(elapsed, 1) * 3600
            pct = idx / total_work * 100
            logger.info(
                "[%s] Session done: %d/%d (%.0f%%) scraped, %d docs, "
                "%d errors, %.0f/hr, %.1fs elapsed",
                self.state, stats["scraped"], total_work, pct,
                stats["documents"], stats["errors"], rate, elapsed,
            )

            # Cooldown between browser sessions
            if idx < total_work:
                wait = 15 if session_expired else 5
                logger.info("[%s] Cooling down %ds before next session", self.state, wait)
                await asyncio.sleep(wait)

        elapsed = round(time.monotonic() - start, 1)
        rate = stats["scraped"] / max(elapsed, 1) * 3600
        logger.info(
            "[%s] COMPLETE: %d scraped, %d skipped, %d docs, "
            "%d errors in %.1fs (%.0f filings/hr)",
            self.state, stats["scraped"], stats["skipped"],
            stats["documents"], stats["errors"], elapsed, rate,
        )
        return stats

    async def _discover_detail_base_url(self, page: Any) -> str | None:
        """Quick test to discover the detail page base URL from session."""
        # Just return the known SERFF SFA detail page URL
        # The session cookies from _establish_sfa_session are what matter
        return "https://filingaccess.serff.com/sfa/search/filingSummary.xhtml"

    async def _process_direct_detail(
        self,
        detail_base_url: str,
        numeric_id: str,
        serff_num: str,
        filing_id: str,
        skip_download: bool,
        download_only: bool = False,
    ) -> dict | None:
        """Open a filing's detail page directly and extract metadata.

        If download_only=True, skip metadata extraction and only download docs.
        """
        detail_page = None
        try:
            detail_url = f"{detail_base_url}?filingId={numeric_id}"
            detail_page = await self._context.new_page()
            await detail_page.goto(detail_url, wait_until="load", timeout=20000)
            # Wait for PrimeFaces AJAX to populate the page content.
            # networkidle is unreliable with PrimeFaces (persistent connections).
            # A fixed sleep is more predictable.
            await asyncio.sleep(3)

            # Check we got a real detail page
            final_url = detail_page.url
            if "filingSummary" not in final_url:
                # Determine error type for tracking
                if "unauthorized" in final_url:
                    error_type = "unauthorized"
                elif "500" in final_url or "error" in final_url.lower():
                    error_type = "not_found"
                elif "sessionExpired" in final_url:
                    error_type = "session_expired"  # transient — don't mark permanent
                else:
                    error_type = "redirect"
                logger.warning(
                    "[%s] %s (id=%s): %s → %s",
                    self.state, serff_num, numeric_id, error_type, final_url,
                )
                if error_type in ("unauthorized", "not_found"):
                    return {
                        "filing_id": filing_id,
                        "serff_num": serff_num,
                        "metadata": {},
                        "doc_count": 0,
                        "error_type": error_type,
                    }
                return None

            if download_only:
                # Download-only mode: get carrier name from DB raw_metadata
                # and just download documents
                carrier = "unknown"
                try:
                    # Quick extraction of carrier name from the page
                    title_el = detail_page.locator("label:has-text('Company Name')")
                    if await title_el.count() > 0:
                        sib = title_el.first.locator("xpath=following-sibling::*[1]")
                        if await sib.count() > 0:
                            carrier = (await sib.text_content(timeout=2000)).strip() or "unknown"
                except Exception:
                    pass

                docs = await self._download_detail_documents(
                    detail_page, self._context, filing_id, carrier, serff_num,
                )
                if docs:
                    await self._batch_insert_documents(
                        [{"filing_id": filing_id, **d} for d in docs]
                    )
                return {
                    "filing_id": filing_id,
                    "serff_num": serff_num,
                    "metadata": {"scrape_status": "success"},
                    "doc_count": len(docs),
                    "download_only": True,
                }

            # Extract metadata
            metadata = await self._extract_detail_metadata(detail_page)
            if not metadata:
                logger.warning("[%s] %s (id=%s): empty metadata (will retry)", self.state, serff_num, numeric_id)
                return None  # Transient — will retry next run

            result = {
                "filing_id": filing_id,
                "serff_num": serff_num,
                "metadata": metadata,
                "doc_count": 0,
            }

            if not skip_download:
                carrier = metadata.get("Company Name", "unknown")
                docs = await self._download_detail_documents(
                    detail_page, self._context, filing_id, carrier, serff_num,
                )
                result["doc_count"] = len(docs)

            return result

        except Exception as exc:
            logger.warning("[%s] Detail error %s: %s", self.state, serff_num, exc)
            return None

        finally:
            if detail_page:
                try:
                    await detail_page.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Pre-load helpers (eliminate per-row DB queries)
    # ------------------------------------------------------------------

    async def _preload_filing_map(self) -> dict[str, str]:
        """Load ALL serff_tracking_number → filing_id for this state."""
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                sa_text(
                    "SELECT id, serff_tracking_number FROM hermes_filings "
                    "WHERE state = :state"
                ),
                {"state": self.state},
            )
            return {r.serff_tracking_number: str(r.id) for r in result.fetchall()}

    async def _get_scraped_serff_nums(self) -> set[str]:
        """Get the set of SERFF tracking numbers already detail-scraped or marked inaccessible."""
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                sa_text(
                    "SELECT serff_tracking_number FROM hermes_filings "
                    "WHERE state = :state AND ("
                    "  filed_date IS NOT NULL "
                    "  OR raw_metadata->>'scrape_status' IN ('unauthorized', 'not_found')"
                    ")"
                ),
                {"state": self.state},
            )
            return {r.serff_tracking_number for r in result.fetchall()}

    async def _get_filings_without_docs(self) -> set[str]:
        """Get filing IDs that have metadata but no documents downloaded."""
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                sa_text(
                    "SELECT CAST(f.id AS text) FROM hermes_filings f "
                    "WHERE f.state = :state AND f.filed_date IS NOT NULL "
                    "AND NOT EXISTS ("
                    "  SELECT 1 FROM hermes_filing_documents d WHERE d.filing_id = f.id"
                    ")"
                ),
                {"state": self.state},
            )
            return {r[0] for r in result.fetchall()}

    # ------------------------------------------------------------------
    # URL pattern discovery
    # ------------------------------------------------------------------

    async def _discover_detail_url_pattern(self, page: Any) -> str | None:
        """Click the first row to learn the detail page URL pattern.

        If data-rk matches the filingId query parameter, we can open detail
        pages directly in new tabs without navigating away from results.
        Returns the base URL string or None.

        After discovery, re-searches to restore results page (no go_back).
        """
        try:
            rows = page.locator("tbody.ui-datatable-data tr")
            first_row = rows.first
            if not await first_row.is_visible(timeout=3000):
                return None

            first_data_rk = await first_row.get_attribute("data-rk")
            logger.info("[%s] First row data-rk=%s", self.state, first_data_rk)

            # Click first row → navigates to detail
            await first_row.click()
            # Wait for URL to change to detail page
            try:
                await page.wait_for_url("**/filingSummary**", timeout=15000)
            except Exception:
                await asyncio.sleep(2)

            detail_url = page.url
            if "filingSummary" not in detail_url:
                logger.info("[%s] First row click didn't reach detail: %s", self.state, detail_url)
                await self._navigate_to_search_and_search(page)
                return None

            # Parse URL to check if data-rk matches filingId param
            from urllib.parse import urlparse, parse_qs

            parsed = urlparse(detail_url)
            qs = parse_qs(parsed.query)
            filing_id_param = qs.get("filingId", [None])[0]
            base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

            logger.info(
                "[%s] Detail URL: filingId=%s, data-rk=%s, match=%s",
                self.state, filing_id_param, first_data_rk,
                filing_id_param == first_data_rk if filing_id_param and first_data_rk else "N/A",
            )

            # Navigate back to search and re-search (don't use go_back — it's fragile)
            await self._navigate_to_search_and_search(page)

            if filing_id_param and first_data_rk:
                if filing_id_param == first_data_rk:
                    return base_url
                else:
                    # data-rk doesn't match filingId — try using data-rk anyway
                    # (might be an internal ID the server recognizes)
                    return base_url

            elif filing_id_param:
                logger.info("[%s] No data-rk attribute — can't use new-tab", self.state)
                return None

        except Exception as exc:
            logger.warning("[%s] URL pattern discovery failed: %s", self.state, exc)
            try:
                await self._navigate_to_search_and_search(page)
            except Exception:
                pass

        return None

    async def _navigate_to_search_and_search(self, page: Any) -> None:
        """Navigate to search form and re-run broad P&C search."""
        await page.goto(
            "https://filingaccess.serff.com/sfa/search/filingSearch.xhtml",
            wait_until="load", timeout=15000,
        )
        await asyncio.sleep(0.5)
        await self._run_search_for_product(
            page, product_name="", date_from="01/01/2020"
        )

    # ------------------------------------------------------------------
    # FAST PATH: new-tab parallel scraping
    # ------------------------------------------------------------------

    async def _scrape_pages_newtab(
        self,
        results_page: Any,
        detail_base_url: str,
        filing_map: dict[str, str],
        scraped_set: set[str],
        skip_download: bool,
        stats: dict[str, int],
        limit: int | None,
    ) -> None:
        """Scrape detail pages by opening new tabs. Results page stays intact."""
        page_num = 1
        start = time.monotonic()
        pending_updates: list[dict] = []

        while True:
            if limit and stats["scraped"] >= limit:
                break

            # Ensure DataTable is visible
            try:
                await results_page.wait_for_selector(
                    "tbody.ui-datatable-data tr", state="visible", timeout=10000
                )
            except Exception:
                logger.warning("[%s] No rows visible on page %d", self.state, page_num)
                break

            rows = await results_page.locator("tbody.ui-datatable-data tr").all()
            if not rows:
                break

            logger.info("[%s] Page %d: %d rows", self.state, page_num, len(rows))

            # Collect row data from HTML (fast — no navigation)
            row_tasks: list[dict] = []
            for row in rows:
                if limit and (stats["scraped"] + len(row_tasks)) >= limit:
                    break
                try:
                    data_rk = await row.get_attribute("data-rk")
                    if not data_rk:
                        continue
                    cells = await row.locator("td").all()
                    serff_num = await self._get_serff_from_row(cells)
                    if serff_num and serff_num in scraped_set:
                        stats["skipped"] += 1
                        continue
                    row_tasks.append({"data_rk": data_rk, "serff_num": serff_num})
                except Exception:
                    continue

            # Process rows in parallel batches
            for batch_start in range(0, len(row_tasks), self.PARALLEL_TABS):
                batch = row_tasks[batch_start:batch_start + self.PARALLEL_TABS]
                if limit and stats["scraped"] >= limit:
                    break

                tasks = [
                    self._process_detail_tab(
                        detail_base_url, rt["data_rk"], rt["serff_num"],
                        filing_map, scraped_set, skip_download,
                    )
                    for rt in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        stats["errors"] += 1
                        logger.error("[%s] Tab error: %s", self.state, result)
                    elif result is None:
                        pass  # skipped (not in DB or no metadata)
                    else:
                        pending_updates.append(result)
                        stats["scraped"] += 1
                        if result.get("doc_count", 0) > 0:
                            stats["documents"] += result["doc_count"]

            # Batch write all updates for this page
            if pending_updates:
                await self._batch_update_filings(pending_updates)
                pending_updates.clear()

            # Progress logging
            elapsed = round(time.monotonic() - start, 1)
            rate = stats["scraped"] / max(elapsed, 1) * 3600
            logger.info(
                "[%s] After page %d: %d scraped, %d skipped, %d errors (%.0f/hr)",
                self.state, page_num, stats["scraped"],
                stats["skipped"], stats["errors"], rate,
            )

            # Pagination — results page was NEVER navigated away from
            if not await self._click_next_page_newtab(results_page, page_num):
                break
            page_num += 1

        # Flush remaining
        if pending_updates:
            await self._batch_update_filings(pending_updates)

    async def _click_next_page_newtab(self, page: Any, current_page: int) -> bool:
        """Click next page using paginator text change detection.

        Unlike the standard _click_next_page which waits for networkidle
        (unreliable with multiple tabs), this method:
        1. Captures current paginator text
        2. Captures current first row's data-rk
        3. Clicks the next button
        4. Waits for either paginator text OR first row data-rk to change
        """
        # Get current state for change detection
        old_pag_text = ""
        try:
            pag = page.locator(".ui-paginator-current")
            if await pag.count() > 0:
                old_pag_text = (await pag.first.text_content()).strip()
        except Exception:
            pass

        old_first_rk = ""
        try:
            first_row = page.locator("tbody.ui-datatable-data tr").first
            old_first_rk = await first_row.get_attribute("data-rk") or ""
        except Exception:
            pass

        # Click next page button
        clicked = False
        try:
            next_btn = page.locator(".ui-paginator-next:not(.ui-state-disabled)").first
            if await next_btn.is_visible(timeout=3000):
                await next_btn.click()
                clicked = True
        except Exception:
            pass

        if not clicked:
            logger.info("[%s] No next page button on page %d", self.state, current_page)
            return False

        # Wait for page content to change (up to 15s)
        for _ in range(30):
            await asyncio.sleep(0.5)
            try:
                new_pag_text = ""
                pag = page.locator(".ui-paginator-current")
                if await pag.count() > 0:
                    new_pag_text = (await pag.first.text_content()).strip()

                new_first_rk = ""
                first_row = page.locator("tbody.ui-datatable-data tr").first
                if await first_row.is_visible(timeout=1000):
                    new_first_rk = await first_row.get_attribute("data-rk") or ""

                # If either changed, the new page loaded
                if (new_pag_text and new_pag_text != old_pag_text) or \
                   (new_first_rk and new_first_rk != old_first_rk):
                    logger.debug(
                        "[%s] Page changed: pag '%s'->'%s', rk '%s'->'%s'",
                        self.state, old_pag_text, new_pag_text,
                        old_first_rk[:10], new_first_rk[:10],
                    )
                    return True
            except Exception:
                continue

        logger.warning("[%s] Pagination timeout on page %d", self.state, current_page)
        return False

    async def _process_detail_tab(
        self,
        detail_base_url: str,
        data_rk: str,
        serff_num: str | None,
        filing_map: dict[str, str],
        scraped_set: set[str],
        skip_download: bool,
    ) -> dict | None:
        """Open a filing detail page in a new tab and extract metadata."""
        detail_page = None
        try:
            detail_url = f"{detail_base_url}?filingId={data_rk}"
            detail_page = await self._context.new_page()
            await detail_page.goto(detail_url, timeout=15000)
            try:
                await detail_page.wait_for_load_state("load", timeout=10000)
            except Exception:
                pass

            # Check we got a real detail page
            if "filingSummary" not in detail_page.url:
                logger.info(
                    "[%s] rk=%s: not detail page, url=%s",
                    self.state, data_rk, detail_page.url,
                )
                return None

            # Extract metadata
            metadata = await self._extract_detail_metadata(detail_page)
            if not metadata:
                logger.info("[%s] rk=%s: empty metadata", self.state, data_rk)
                return None

            # Look up filing ID from our pre-loaded map
            detail_serff = metadata.get("SERFF Tracking Number", serff_num or "")
            filing_id = filing_map.get(detail_serff) or (
                filing_map.get(serff_num) if serff_num else None
            )

            if not filing_id:
                logger.debug(
                    "[%s] rk=%s serff=%s: not in our DB (row_serff=%s)",
                    self.state, data_rk, detail_serff, serff_num,
                )
                if detail_serff:
                    scraped_set.add(detail_serff)
                if serff_num:
                    scraped_set.add(serff_num)
                return None

            # Mark as scraped
            if detail_serff:
                scraped_set.add(detail_serff)
            if serff_num:
                scraped_set.add(serff_num)

            result = {
                "filing_id": filing_id,
                "serff_num": detail_serff or serff_num,
                "metadata": metadata,
                "doc_count": 0,
            }

            # Download documents if requested
            if not skip_download:
                carrier = metadata.get("Company Name", "unknown")
                docs = await self._download_detail_documents(
                    detail_page, self._context, filing_id,
                    carrier, detail_serff or serff_num or data_rk,
                )
                result["doc_count"] = len(docs)

            return result

        except Exception as exc:
            logger.error(
                "[%s] Detail tab error (rk=%s): %s",
                self.state, data_rk, exc, exc_info=True,
            )
            raise

        finally:
            if detail_page:
                try:
                    await detail_page.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # FALLBACK PATH: click-and-back with date-range chunks
    # ------------------------------------------------------------------

    async def _scrape_pages_clickback(
        self,
        page: Any,
        filing_map: dict[str, str],
        scraped_set: set[str],
        skip_download: bool,
        stats: dict[str, int],
        limit: int | None,
    ) -> None:
        """Scrape via click-and-back. Re-searches for each page to avoid
        PrimeFaces ViewState corruption from navigate-back cycles."""
        page_num = 1
        start = time.monotonic()
        pending_updates: list[dict] = []

        while True:
            if limit and stats["scraped"] >= limit:
                break

            # Ensure we're on results page with data
            try:
                await page.wait_for_selector(
                    "tbody.ui-datatable-data tr", state="visible", timeout=10000
                )
            except Exception:
                break

            rows_on_page = await page.locator("tbody.ui-datatable-data tr").count()
            if rows_on_page == 0:
                break

            logger.info("[%s] Page %d: %d rows", self.state, page_num, rows_on_page)

            for row_idx in range(rows_on_page):
                if limit and stats["scraped"] >= limit:
                    break
                result = await self._process_result_row_v2(
                    page, row_idx, filing_map, scraped_set, skip_download, stats,
                )
                if result:
                    pending_updates.append(result)

            # Batch write
            if pending_updates:
                await self._batch_update_filings(pending_updates)
                pending_updates.clear()

            # Try pagination. If it fails, re-search and skip forward.
            if not await self._click_next_page_robust(page, page_num):
                # Attempt recovery: re-search and skip to next page
                recovered = await self._recover_to_page(page, page_num + 1)
                if not recovered:
                    break

            page_num += 1

            if page_num % 5 == 0:
                elapsed = round(time.monotonic() - start, 1)
                rate = stats["scraped"] / max(elapsed, 1) * 3600
                logger.info(
                    "[%s] Page %d: %d scraped, %d errors (%.0f/hr)",
                    self.state, page_num, stats["scraped"],
                    stats["errors"], rate,
                )

        if pending_updates:
            await self._batch_update_filings(pending_updates)

    async def _process_result_row_v2(
        self,
        page: Any,
        row_idx: int,
        filing_map: dict[str, str],
        scraped_set: set[str],
        skip_download: bool,
        stats: dict[str, int],
    ) -> dict | None:
        """Click-and-back for a single row. Uses pre-loaded filing_map."""
        navigated = False
        try:
            row = page.locator("tbody.ui-datatable-data tr").nth(row_idx)
            if not await row.is_visible(timeout=3000):
                return None
            cells = await row.locator("td").all()
            if len(cells) < 5:
                return None

            serff_num = await self._get_serff_from_row(cells)
            if not serff_num:
                return None

            if serff_num in scraped_set:
                stats["skipped"] += 1
                return None

            # Click the row → detail page
            await row.click()
            navigated = True
            try:
                await page.wait_for_load_state("load", timeout=15000)
            except Exception:
                pass
            await asyncio.sleep(0.3)

            if "filingSummary" not in page.url:
                stats["errors"] += 1
                return None

            metadata = await self._extract_detail_metadata(page)
            if not metadata:
                scraped_set.add(serff_num)
                return None

            detail_serff = metadata.get("SERFF Tracking Number", serff_num)
            filing_id = filing_map.get(detail_serff) or filing_map.get(serff_num)
            if not filing_id:
                scraped_set.add(serff_num)
                scraped_set.add(detail_serff)
                return None

            result = {
                "filing_id": filing_id,
                "serff_num": detail_serff,
                "metadata": metadata,
                "doc_count": 0,
            }

            if not skip_download:
                carrier = metadata.get("Company Name", "unknown")
                docs = await self._download_detail_documents(
                    page, self._context, filing_id, carrier, detail_serff,
                )
                result["doc_count"] = len(docs)

            scraped_set.add(serff_num)
            scraped_set.add(detail_serff)
            stats["scraped"] += 1
            return result

        except Exception as exc:
            stats["errors"] += 1
            logger.error("[%s] Row %d error: %s", self.state, row_idx, exc, exc_info=True)
            return None

        finally:
            if navigated and "filingSearchResults" not in page.url:
                try:
                    await page.go_back()
                    await page.wait_for_load_state("load", timeout=10000)
                except Exception:
                    pass
                await asyncio.sleep(0.3)

    # ------------------------------------------------------------------
    # Robust pagination (for fallback path)
    # ------------------------------------------------------------------

    async def _click_next_page_robust(self, page: Any, current_page: int) -> bool:
        """Click next page with extra waits to handle post-click-back state."""
        # Ensure DataTable is visible before checking paginator
        try:
            await page.wait_for_selector(
                "tbody.ui-datatable-data", state="visible", timeout=5000
            )
        except Exception:
            return False

        # Standard next button
        try:
            next_btn = page.locator(".ui-paginator-next:not(.ui-state-disabled)").first
            if await next_btn.is_visible(timeout=3000):
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=30000)
                await asyncio.sleep(1)
                return True
        except Exception:
            pass

        # Fallback: '>' link
        try:
            next_link = page.locator("a:has-text('>')").first
            if await next_link.is_visible(timeout=2000):
                await next_link.click()
                await page.wait_for_load_state("networkidle", timeout=30000)
                await asyncio.sleep(1)
                return True
        except Exception:
            pass

        logger.info("[%s] No next page link found on page %d", self.state, current_page)
        return False

    async def _recover_to_page(self, page: Any, target_page: int) -> bool:
        """Re-search and navigate to a specific results page."""
        logger.info("[%s] Recovering: re-searching and skipping to page %d", self.state, target_page)
        try:
            # Navigate to search form (session cookies still valid)
            await page.goto(
                "https://filingaccess.serff.com/sfa/search/filingSearch.xhtml",
                timeout=15000,
            )
            await page.wait_for_load_state("load", timeout=15000)

            has_results = await self._run_search_for_product(
                page, product_name="", date_from="01/01/2020"
            )
            if not has_results:
                return False

            # Click next (target_page - 1) times
            for _ in range(target_page - 1):
                if not await self._click_next_page(page):
                    return False

            return True
        except Exception as exc:
            logger.error("[%s] Recovery failed: %s", self.state, exc)
            return False

    # ------------------------------------------------------------------
    # SFA session and search
    # ------------------------------------------------------------------

    async def _establish_sfa_session(self, page: Any) -> None:
        """Navigate to the SFA portal and accept the user agreement.

        SERFF requires: Home page → Begin Search → Agreement → Accept.
        Detects CAPTCHA/rate-limit blocks (HTTP 405 "Human Verification").
        """
        home_url = self.SFA_HOME_URL_TEMPLATE.format(state=self.state)
        logger.info("[%s] Establishing SFA session", self.state)

        for attempt in range(3):
            try:
                # Step 1: Home page (sets up SERFF state session)
                resp = await page.goto(home_url, wait_until="load", timeout=30000)
                await asyncio.sleep(2)

                # Check for CAPTCHA / rate-limit block
                title = await page.title()
                status = resp.status if resp else 0
                if status == 405 or "verification" in title.lower():
                    logger.error(
                        "[%s] SERFF CAPTCHA/rate-limit detected (HTTP %d, title=%r). "
                        "Need browser restart. Waiting 180s.",
                        self.state, status, title,
                    )
                    await asyncio.sleep(180)
                    raise RuntimeError(f"SERFF CAPTCHA detected, need fresh browser")

                if "filingSearch" in page.url:
                    logger.info("[%s] SFA session established (attempt %d)", self.state, attempt + 1)
                    return

                # Step 2: Click "Begin Search" → agreement page
                try:
                    begin_link = page.locator("a:has-text('Begin')")
                    if await begin_link.is_visible(timeout=8000):
                        await begin_link.click()
                        await asyncio.sleep(3)
                except Exception:
                    await page.goto(
                        "https://filingaccess.serff.com/sfa/userAgreement.xhtml",
                        wait_until="load", timeout=15000,
                    )
                    await asyncio.sleep(2)

                if "filingSearch" in page.url:
                    logger.info("[%s] SFA session established (attempt %d)", self.state, attempt + 1)
                    return

                # Step 3: Click Accept button
                accept_btn = page.locator("button:has-text('Accept')").first
                if await accept_btn.is_visible(timeout=8000):
                    logger.info("[%s] Clicking Accept (attempt %d)", self.state, attempt + 1)
                    await accept_btn.click()
                    try:
                        await page.wait_for_url("**/filingSearch**", timeout=15000)
                    except Exception:
                        await asyncio.sleep(3)
                else:
                    logger.warning("[%s] Attempt %d: Accept button not visible on %s", self.state, attempt + 1, page.url)

                if "filingSearch" in page.url:
                    logger.info("[%s] SFA session established (attempt %d)", self.state, attempt + 1)
                    return

                logger.warning(
                    "[%s] Attempt %d: on %s after accept click",
                    self.state, attempt + 1, page.url,
                )

            except Exception as exc:
                logger.warning("[%s] Session attempt %d failed: %s", self.state, attempt + 1, exc)

            await asyncio.sleep(5)

        raise RuntimeError(
            f"[{self.state}] Failed to establish SFA session after 3 attempts"
        )

    async def _run_search_for_product(
        self, page: Any, product_name: str = "", date_from: str = ""
    ) -> bool:
        """Run a P&C search on the SFA portal. Returns True if results loaded."""
        try:
            trigger = page.locator(".ui-selectonemenu-trigger").first
            await trigger.click(timeout=5000)
            await asyncio.sleep(0.5)
            panel = page.locator(".ui-selectonemenu-panel").first
            for item in await panel.locator("li").all():
                if "property" in (await item.text_content()).strip().lower():
                    await item.click()
                    break
            await page.wait_for_load_state("networkidle", timeout=15000)
            await asyncio.sleep(1)
        except Exception as exc:
            logger.warning("[%s] P&C selection: %s", self.state, exc)

        await self._wait_for_blockui(page)

        if product_name:
            try:
                await page.fill(
                    "input[id='simpleSearch:productName']",
                    product_name, timeout=3000,
                )
            except Exception:
                pass

        if date_from:
            try:
                await page.fill(
                    "input[id='simpleSearch:submissionStartDate_input']",
                    date_from, timeout=3000,
                )
            except Exception:
                pass

        await page.click("button[id='simpleSearch:saveBtn']", timeout=10000)
        try:
            await page.wait_for_url("**/filingSearchResults**", timeout=60000)
        except Exception:
            await asyncio.sleep(3)

        has_results = "filingSearchResults" in page.url
        if has_results:
            pag = page.locator(".ui-paginator-current")
            pag_text = ""
            if await pag.count() > 0:
                pag_text = await pag.first.text_content()
            logger.info("[%s] Search results: %s", self.state, pag_text)
        else:
            logger.warning("[%s] Search produced no results", self.state)

        return has_results

    # ------------------------------------------------------------------
    # Row parsing
    # ------------------------------------------------------------------

    async def _get_serff_from_row(self, cells: list[Any]) -> str | None:
        """Extract the SERFF tracking number from a result table row."""
        serff_re = re.compile(r"^[A-Z]{3,6}-[A-Z]?\d{5,12}$")

        for idx in (6, 5, 7, 4, 3, 8):
            if idx < len(cells):
                text = (await cells[idx].text_content()).strip()
                if serff_re.match(text):
                    return text

        for cell in cells:
            text = (await cell.text_content()).strip()
            if serff_re.match(text):
                return text

        return None

    # ------------------------------------------------------------------
    # Detail page metadata extraction
    # ------------------------------------------------------------------

    async def _extract_detail_metadata(self, page: Any) -> dict[str, str]:
        """Parse the SERFF SFA filing detail page for all metadata."""
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")
        metadata: dict[str, str] = {}

        # Method 1: <label> + target element via `for` attribute, or sibling
        for label in soup.find_all("label"):
            key = label.get_text(strip=True).rstrip(":")
            if not key or len(key) > 100:
                continue

            for_id = label.get("for", "")
            if for_id:
                target = soup.find(id=for_id)
                if target and target.name != "label":
                    val = target.get_text(strip=True) or target.get("value", "")
                    if val:
                        metadata[key] = val
                        continue

            ns = label.find_next_sibling()
            if ns and ns.name in ("span", "div", "output", "input", "select"):
                val = ns.get_text(strip=True) or ns.get("value", "")
                if val and len(val) < 2000:
                    metadata[key] = val

        # Method 2: <tr> with <th>/<td> pairs
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) == 2:
                key = cells[0].get_text(strip=True).rstrip(":")
                val = cells[1].get_text(strip=True)
                if key and val and len(key) < 100:
                    metadata.setdefault(key, val)

        # Method 3: <dt>/<dd> pairs
        for dt, dd in zip(soup.find_all("dt"), soup.find_all("dd")):
            key = dt.get_text(strip=True).rstrip(":")
            val = dd.get_text(strip=True)
            if key and val:
                metadata.setdefault(key, val)

        # Panel text for filing description
        panels_text = []
        for panel in soup.find_all(class_=re.compile(r"ui-panel")):
            title_el = panel.find(class_="ui-panel-title")
            title = title_el.get_text(strip=True) if title_el else ""
            content_el = panel.find(class_="ui-panel-content")
            if content_el:
                text = content_el.get_text(strip=True)
                if text and title:
                    panels_text.append(f"[{title}] {text[:1000]}")

        if panels_text:
            metadata["_panels"] = " | ".join(panels_text)

        # Rate change regex patterns
        body_text = soup.get_text()
        rate_patterns = [
            (r"overall\s+(?:rate\s+)?(?:change|indication|impact)\s*[:\s]*([+-]?\d+\.?\d*)\s*%", "overall_rate_change"),
            (r"rate\s+(?:change|increase|decrease)\s*[:\s]*([+-]?\d+\.?\d*)\s*%", "rate_change"),
            (r"(?:minimum|min)\s+(?:rate\s+)?change\s*[:\s]*([+-]?\d+\.?\d*)\s*%", "rate_min"),
            (r"(?:maximum|max)\s+(?:rate\s+)?change\s*[:\s]*([+-]?\d+\.?\d*)\s*%", "rate_max"),
            (r"(?:affected|impacted)\s+policyholders?\s*[:\s]*(\d[\d,]*)", "affected_policyholders"),
        ]
        for pattern, key in rate_patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                metadata[f"_rate_{key}"] = match.group(1).replace(",", "")

        return metadata

    # ------------------------------------------------------------------
    # Document download
    # ------------------------------------------------------------------

    async def _download_detail_documents(
        self,
        page: Any,
        context: Any,
        filing_id: str,
        carrier_name: str,
        serff_num: str,
    ) -> list[dict[str, Any]]:
        """Download all documents from the filing detail page."""
        downloaded: list[dict[str, Any]] = []

        safe_carrier = re.sub(r"[^\w\s\-]", "_", carrier_name or "unknown")[:100].strip()
        filing_dir = self._storage_root / self.state / safe_carrier / serff_num
        filing_dir.mkdir(parents=True, exist_ok=True)

        doc_links = await page.locator("a.ui-commandlink").all()
        if not doc_links:
            return downloaded

        for link in doc_links:
            try:
                doc_name = (await link.text_content(timeout=2000)).strip()
                if not doc_name or len(doc_name) < 3:
                    continue

                onclick = await link.get_attribute("onclick") or ""
                if "downloadAttachment" not in onclick and "download" not in onclick.lower():
                    if not any(
                        doc_name.lower().endswith(ext)
                        for ext in (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip")
                    ):
                        continue

                safe_name = re.sub(r"[^\w\s\-.]", "_", doc_name)[:200].strip()
                if not safe_name:
                    continue
                file_path = filing_dir / safe_name

                if file_path.exists() and file_path.stat().st_size > 0:
                    doc_info = self._make_doc_info(
                        doc_name, safe_name, str(file_path), file_path.stat().st_size
                    )
                    downloaded.append(doc_info)
                    continue

                saved = await self._click_and_save_download(page, context, link, file_path)

                if saved and file_path.exists():
                    file_size = file_path.stat().st_size
                    doc_info = self._make_doc_info(
                        doc_name, safe_name, str(file_path), file_size
                    )
                    downloaded.append(doc_info)
                    logger.info(
                        "[%s] %s: downloaded %s (%d bytes)",
                        self.state, serff_num, safe_name, file_size,
                    )

            except Exception as exc:
                logger.warning("[%s] %s: download error: %s", self.state, serff_num, exc)

        return downloaded

    async def _click_and_save_download(
        self, page: Any, context: Any, link: Any, file_path: Path
    ) -> bool:
        """Click a PrimeFaces command link and save the resulting download.

        PrimeFaces command links typically trigger a form POST that returns
        content with Content-Disposition: attachment.  Playwright captures
        these as download events on the *same* page (no new tab).

        Fallback: some links open a new tab with a PDF — handle that too.
        """
        # Strategy 1: expect_download on the same page (most PrimeFaces downloads)
        try:
            async with page.expect_download(timeout=15000) as download_info:
                await link.click()
            download = await download_info.value
            await download.save_as(str(file_path))
            return True
        except Exception:
            pass

        # Strategy 2: link may have opened a new page/tab with the PDF
        try:
            new_page = await context.wait_for_event("page", timeout=3000)
        except Exception:
            return False

        try:
            try:
                download = await new_page.wait_for_event("download", timeout=5000)
                await download.save_as(str(file_path))
                return True
            except Exception:
                pass

            try:
                await new_page.wait_for_load_state("load", timeout=5000)
                content_type = await new_page.evaluate("() => document.contentType")
                if content_type and "pdf" in content_type.lower():
                    pdf_url = new_page.url
                    if pdf_url and pdf_url != "about:blank":
                        resp = await new_page.request.get(pdf_url)
                        body = await resp.body()
                        file_path.write_bytes(body)
                        return True
            except Exception:
                pass
        finally:
            try:
                await new_page.close()
            except Exception:
                pass

        return False

    def _make_doc_info(
        self, doc_name: str, safe_name: str, file_path: str, file_size: int
    ) -> dict[str, Any]:
        """Build a document info dict for DB insertion."""
        name_lower = doc_name.lower()
        if any(kw in name_lower for kw in ["rate", "rating", "premium", "actuarial"]):
            doc_type = "rate"
        elif any(kw in name_lower for kw in ["rule", "underwriting", "guideline", "eligibility"]):
            doc_type = "rule"
        elif any(kw in name_lower for kw in ["form", "application", "endorsement", "certificate"]):
            doc_type = "form"
        else:
            doc_type = "other"

        ext = Path(safe_name).suffix.lower()
        mime_map = {
            ".pdf": "application/pdf",
            ".doc": "application/msword",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xls": "application/vnd.ms-excel",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".zip": "application/zip",
        }
        mime_type = mime_map.get(ext, "application/octet-stream")

        checksum = None
        try:
            content = Path(file_path).read_bytes()
            checksum = hashlib.sha256(content).hexdigest()
        except Exception:
            pass

        return {
            "document_name": doc_name[:500],
            "document_type": doc_type,
            "file_path": file_path[:1000],
            "file_size_bytes": file_size,
            "mime_type": mime_type,
            "checksum_sha256": checksum,
        }

    # ------------------------------------------------------------------
    # Batch DB operations
    # ------------------------------------------------------------------

    async def _batch_update_filings(self, updates: list[dict]) -> None:
        """Batch update multiple filings in a single transaction."""
        if not updates:
            return

        engine = await self._get_engine()
        async with engine.begin() as conn:
            for update in updates:
                try:
                    metadata = update["metadata"]
                    filing_id = update["filing_id"]
                    serff_num = update["serff_num"]
                    error_type = update.get("error_type")

                    # Download-only entries: docs already inserted, nothing to update
                    if update.get("download_only"):
                        continue

                    # For permanently failed filings, just mark the status
                    if error_type in ("unauthorized", "not_found", "empty"):
                        await conn.execute(
                            sa_text("""
                                UPDATE hermes_filings SET
                                    raw_metadata = COALESCE(raw_metadata, '{}'::jsonb)
                                        || CAST(:status_json AS jsonb),
                                    updated_at = NOW()
                                WHERE id = CAST(:filing_id AS uuid)
                            """),
                            {
                                "filing_id": filing_id,
                                "status_json": json.dumps({"scrape_status": error_type}),
                            },
                        )
                        continue

                    filed_date = self._parse_date(
                        metadata.get("Submission Date", "")
                        or metadata.get("Date Filed", "")
                        or metadata.get("Filed Date", "")
                    )
                    disposition_date = self._parse_date(
                        metadata.get("Disposition Date", "")
                    )
                    effective_date = self._parse_date(
                        metadata.get("Effective Date", "")
                        or metadata.get("Proposed Effective Date", "")
                    )

                    description_parts = []
                    if metadata.get("Filing Description"):
                        description_parts.append(metadata["Filing Description"])
                    if metadata.get("_panels"):
                        description_parts.append(metadata["_panels"])
                    filing_description = " | ".join(description_parts)[:5000] or None

                    overall_rate_change = None
                    rate_min = None
                    rate_max = None
                    affected = None

                    for src_key in ("_rate_overall_rate_change", "_rate_rate_change"):
                        if metadata.get(src_key) and overall_rate_change is None:
                            try:
                                overall_rate_change = float(metadata[src_key])
                            except ValueError:
                                pass
                    if metadata.get("_rate_rate_min"):
                        try:
                            rate_min = float(metadata["_rate_rate_min"])
                        except ValueError:
                            pass
                    if metadata.get("_rate_rate_max"):
                        try:
                            rate_max = float(metadata["_rate_rate_max"])
                        except ValueError:
                            pass
                    if metadata.get("_rate_affected_policyholders"):
                        try:
                            affected = int(metadata["_rate_affected_policyholders"])
                        except ValueError:
                            pass

                    detail_metadata = {
                        k: v for k, v in metadata.items()
                        if not k.startswith("_") and len(v) < 2000
                    }
                    detail_metadata["scrape_status"] = "success"

                    await conn.execute(
                        sa_text("""
                            UPDATE hermes_filings SET
                                filed_date = COALESCE(:filed_date, filed_date),
                                disposition_date = COALESCE(:disposition_date, disposition_date),
                                effective_date = COALESCE(:effective_date, effective_date),
                                filing_description = COALESCE(:description, filing_description),
                                overall_rate_change_pct = COALESCE(:rate_change, overall_rate_change_pct),
                                rate_change_min_pct = COALESCE(:rate_min, rate_change_min_pct),
                                rate_change_max_pct = COALESCE(:rate_max, rate_change_max_pct),
                                affected_policyholders = COALESCE(:affected, affected_policyholders),
                                raw_metadata = raw_metadata || CAST(:detail_meta AS jsonb),
                                updated_at = NOW()
                            WHERE id = CAST(:filing_id AS uuid)
                        """),
                        {
                            "filing_id": filing_id,
                            "filed_date": filed_date,
                            "disposition_date": disposition_date,
                            "effective_date": effective_date,
                            "description": filing_description,
                            "rate_change": overall_rate_change,
                            "rate_min": rate_min,
                            "rate_max": rate_max,
                            "affected": affected,
                            "detail_meta": json.dumps(detail_metadata),
                        },
                    )
                except Exception as exc:
                    logger.error(
                        "[%s] Batch update failed for %s: %s",
                        self.state, update.get("serff_num", "?"), exc,
                    )

        logger.info("[%s] Batch updated %d filings", self.state, len(updates))

    async def _batch_insert_documents(self, doc_records: list[dict]) -> None:
        """Batch insert document records in a single transaction."""
        if not doc_records:
            return

        engine = await self._get_engine()
        async with engine.begin() as conn:
            for doc in doc_records:
                try:
                    await conn.execute(
                        sa_text("""
                            INSERT INTO hermes_filing_documents (
                                id, filing_id, document_name, document_type,
                                file_path, file_size_bytes, mime_type,
                                checksum_sha256, parsed_flag
                            ) VALUES (
                                :id, CAST(:filing_id AS uuid), :doc_name, :doc_type,
                                :file_path, :file_size, :mime_type,
                                :checksum, FALSE
                            )
                            ON CONFLICT DO NOTHING
                        """),
                        {
                            "id": str(uuid.uuid4()),
                            "filing_id": doc["filing_id"],
                            "doc_name": doc["document_name"],
                            "doc_type": doc["document_type"],
                            "file_path": doc["file_path"],
                            "file_size": doc["file_size_bytes"],
                            "mime_type": doc["mime_type"],
                            "checksum": doc.get("checksum_sha256"),
                        },
                    )
                except Exception as exc:
                    logger.error("[%s] Doc insert failed: %s", self.state, exc)

    async def _insert_document_record(
        self, filing_id: str, doc_info: dict[str, Any]
    ) -> None:
        """Insert a single document record."""
        doc_info["filing_id"] = filing_id
        await self._batch_insert_documents([doc_info])

    # ------------------------------------------------------------------
    # Date parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_date(date_str: str) -> _date | None:
        """Parse various SERFF date formats to a date object for asyncpg."""
        if not date_str or not date_str.strip():
            return None

        date_str = date_str.strip()

        for fmt in (
            "%m/%d/%Y",   # 01/13/2025
            "%m/%d/%y",   # 1/13/25
            "%Y-%m-%d",   # 2025-01-13
            "%m-%d-%Y",   # 01-13-2025
            "%B %d, %Y",  # January 13, 2025
            "%b %d, %Y",  # Jan 13, 2025
        ):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None
