"""SERFF Filing Access portal navigation helpers.

``SERFFNavigator`` encapsulates all generic interactions with the SFA web
interface — from selecting a state on the landing page, accepting the
disclaimer, filling and submitting the search form, to parsing the results
table and extracting document download links.  State-specific scrapers
delegate the mechanical page-interaction work to this class.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from hermes.scraper.base import FilingResult, SearchParams

logger = logging.getLogger("hermes.scraper")


class SERFFNavigator:
    """Generic SERFF Filing Access portal navigator.

    All methods accept a Playwright ``Page`` as their first argument so that
    a single navigator instance can be reused across multiple pages / browser
    contexts.
    """

    # Base URL for the SFA portal, overridden by settings where needed.
    BASE_URL: str = "https://filingaccess.serff.com"

    # ------------------------------------------------------------------
    # Portal entry
    # ------------------------------------------------------------------

    async def navigate_to_state_portal(self, page: Any, state: str) -> None:
        """Navigate to the SFA landing page and select a state.

        The SFA home page (``/sfa/home``) presents a drop-down list of states.
        This method selects the requested state and submits the form, landing
        on the state-specific search/disclaimer page.

        Parameters
        ----------
        page:
            Playwright ``Page`` object (any URL).
        state:
            Two-letter state code (e.g. ``"TX"``).
        """
        state = state.upper()
        logger.info("Navigating to SFA home for state=%s", state)

        home_url = f"{self.BASE_URL}/sfa/home"
        await page.goto(home_url, wait_until="networkidle", timeout=30000)

        # Try the state-selector drop-down (value attribute is state code).
        try:
            await page.select_option("select[name='state']", state, timeout=5000)
            logger.debug("Selected state from drop-down: %s", state)
        except Exception:
            # Some portal versions use a direct link list instead.
            try:
                state_link = page.locator(f"a:has-text('{state}')")
                if not await state_link.is_visible(timeout=3000):
                    # Try full state name links — fall back to direct URL.
                    logger.debug(
                        "State drop-down not found; navigating directly to state portal"
                    )
                    await page.goto(
                        f"{self.BASE_URL}/sfa/search/{state}",
                        wait_until="networkidle",
                        timeout=30000,
                    )
                    return
                await state_link.first.click()
            except Exception as exc:
                logger.warning(
                    "Could not select state %s via UI; using direct URL. Error: %s",
                    state,
                    exc,
                )
                await page.goto(
                    f"{self.BASE_URL}/sfa/search/{state}",
                    wait_until="networkidle",
                    timeout=30000,
                )
                return

        # Submit the state-selection form if a submit button is present.
        try:
            submit = page.locator(
                "input[type='submit'], button[type='submit']"
            ).first
            if await submit.is_visible(timeout=2000):
                await submit.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Agreement
    # ------------------------------------------------------------------

    async def accept_agreement(self, page: Any) -> None:
        """Locate and click the SFA user-agreement accept button.

        SFA portals require clicking through a disclaimer before exposing
        the search form.  Different states use slightly different button
        labels; this method tries the most common variants.

        Parameters
        ----------
        page:
            Playwright ``Page`` on the user-agreement screen.
        """
        logger.debug("Accepting SFA user agreement")
        candidates = [
            "input[value='Accept']",
            "input[value='I Agree']",
            "input[value='Agree']",
            "input[value*='Accept']",
            "input[value*='Agree']",
            "button:has-text('Accept')",
            "button:has-text('Agree')",
            "button:has-text('I Agree')",
            "a:has-text('Accept')",
            "a:has-text('Agree')",
        ]
        for selector in candidates:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=2000):
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    logger.info("Agreement accepted via: %s", selector)
                    return
            except Exception:
                continue
        logger.warning("No agreement button found — page may already be past agreement")

    # ------------------------------------------------------------------
    # Search form
    # ------------------------------------------------------------------

    async def fill_search_form(self, page: Any, params: SearchParams) -> None:
        """Populate the SERFF search form with the supplied parameters.

        Fills whichever of the standard SFA search fields are present on the
        current page.  Missing or non-applicable fields are silently skipped.

        Parameters
        ----------
        page:
            Playwright ``Page`` on the SFA search form.
        params:
            Search parameters to apply.
        """
        logger.debug("Filling SFA search form: %s", params.model_dump(exclude_none=True))

        # NAIC number
        if params.carrier_naic:
            await self._try_fill(
                page,
                [
                    "input[name*='naic']",
                    "input[id*='naic']",
                    "input[name*='NAIC']",
                    "input[id*='NAIC']",
                    "input[name*='companyNaic']",
                ],
                params.carrier_naic,
            )

        # Carrier name
        if params.carrier_name:
            await self._try_fill(
                page,
                [
                    "input[name*='companyName']",
                    "input[name*='company_name']",
                    "input[id*='companyName']",
                    "input[name*='carrierName']",
                ],
                params.carrier_name,
            )

        # Line of business
        if params.line_of_business:
            await self._try_select(
                page,
                [
                    "select[name*='typeOfInsurance']",
                    "select[name*='lineOfBusiness']",
                    "select[id*='typeOfInsurance']",
                    "select[name*='line']",
                ],
                params.line_of_business,
            )

        # Filing type
        if params.filing_type:
            await self._try_select(
                page,
                [
                    "select[name*='filingType']",
                    "select[id*='filingType']",
                    "select[name*='filing_type']",
                ],
                params.filing_type,
            )

        # Status
        if params.status:
            await self._try_select(
                page,
                [
                    "select[name*='status']",
                    "select[id*='status']",
                    "select[name*='filingStatus']",
                ],
                params.status,
            )

        # Date range — from
        if params.date_from:
            await self._try_fill(
                page,
                [
                    "input[name*='dateFrom']",
                    "input[name*='startDate']",
                    "input[name*='date_from']",
                    "input[id*='dateFrom']",
                    "input[name*='fromDate']",
                ],
                params.date_from,
            )

        # Date range — to
        if params.date_to:
            await self._try_fill(
                page,
                [
                    "input[name*='dateTo']",
                    "input[name*='endDate']",
                    "input[name*='date_to']",
                    "input[id*='dateTo']",
                    "input[name*='toDate']",
                ],
                params.date_to,
            )

    # ------------------------------------------------------------------
    # Search submission
    # ------------------------------------------------------------------

    async def submit_search(self, page: Any) -> None:
        """Click the search/submit button on the SFA search form.

        Parameters
        ----------
        page:
            Playwright ``Page`` with a populated search form.
        """
        logger.debug("Submitting SFA search form")
        submit_selectors = [
            "input[type='submit'][value*='Search']",
            "button[type='submit']:has-text('Search')",
            "input[type='submit']",
            "button[type='submit']",
            "button:has-text('Search')",
        ]
        for selector in submit_selectors:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=2000):
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=30000)
                    logger.info("Search submitted via: %s", selector)
                    return
            except Exception:
                continue
        raise RuntimeError("Could not find a search submit button on the page")

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    async def get_results_page(self, page: Any, page_num: int) -> None:
        """Navigate to a specific numbered results page.

        Attempts to find a pagination link whose text matches *page_num* and
        clicks it.  If no link is found (e.g. the current page IS *page_num*)
        the method returns silently.

        Parameters
        ----------
        page:
            Playwright ``Page`` showing a results table.
        page_num:
            1-based page number to navigate to.
        """
        logger.debug("Navigating to results page %d", page_num)

        # Try a link whose exact text is the page number.
        pager_link = page.locator(
            f"a:has-text('{page_num}')"
        ).first
        try:
            if await pager_link.is_visible(timeout=3000):
                await pager_link.click()
                await page.wait_for_load_state("networkidle", timeout=20000)
                return
        except Exception:
            pass

        # Try common "next page" button patterns.
        if page_num > 1:
            next_selectors = [
                "a:has-text('Next')",
                "a:has-text('>')",
                "input[value='Next']",
            ]
            for sel in next_selectors:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=2000):
                        await btn.click()
                        await page.wait_for_load_state("networkidle", timeout=20000)
                        return
                except Exception:
                    continue

        logger.debug("Page %d link not found — may already be on last page", page_num)

    # ------------------------------------------------------------------
    # Results parsing
    # ------------------------------------------------------------------

    async def parse_results_table(self, page: Any) -> list[FilingResult]:
        """Parse the SFA search-results table and return a list of filings.

        Reads the current page's HTML and uses BeautifulSoup to locate the
        main results ``<table>``.  Each ``<tr>`` (excluding the header) is
        mapped to a :class:`~hermes.scraper.base.FilingResult`.

        Parameters
        ----------
        page:
            Playwright ``Page`` displaying a results table.

        Returns
        -------
        list[FilingResult]
            Filings extracted from the visible results table.  Returns an
            empty list if no table or no data rows are found.
        """
        logger.debug("Parsing results table")
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        # SFA tables are identified by class or by containing SERFF tracking numbers.
        tables = soup.find_all("table")
        results: list[FilingResult] = []

        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Identify header row and column positions.
            header_row = rows[0]
            headers = [
                th.get_text(strip=True).lower()
                for th in header_row.find_all(["th", "td"])
            ]

            # Must look like a filings table.
            if not any("serff" in h or "tracking" in h or "filing" in h for h in headers):
                continue

            col_index = self._map_columns(headers)

            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue

                filing = self._parse_row(cells, col_index)
                if filing:
                    results.append(filing)

        logger.info("Parsed %d filings from results table", len(results))
        return results

    # ------------------------------------------------------------------
    # Filing detail
    # ------------------------------------------------------------------

    async def get_filing_detail(
        self, page: Any, tracking_number: str
    ) -> FilingResult | None:
        """Retrieve full filing details from the SFA filing-detail page.

        Navigates to the detail page for *tracking_number* and extracts
        all metadata fields.

        Parameters
        ----------
        page:
            Playwright ``Page``.
        tracking_number:
            SERFF tracking number (e.g. ``"TXDO-129012345-2024"``).

        Returns
        -------
        FilingResult | None
            Populated filing record, or ``None`` if the page could not be
            parsed.
        """
        logger.debug("Fetching filing detail for %s", tracking_number)
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        metadata: dict = {}
        # SFA detail pages use definition-list or table layout for field/value pairs.
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).lower().replace(" ", "_").rstrip(":")
                value = cells[1].get_text(strip=True)
                if key and value:
                    metadata[key] = value

        # Also try <dl>/<dt>/<dd> pattern.
        for dt, dd in zip(soup.find_all("dt"), soup.find_all("dd")):
            key = dt.get_text(strip=True).lower().replace(" ", "_").rstrip(":")
            value = dd.get_text(strip=True)
            if key and value:
                metadata[key] = value

        if not metadata:
            return None

        return FilingResult(
            serff_tracking_number=tracking_number,
            carrier_name=metadata.get("company_name", metadata.get("carrier_name", "")),
            carrier_naic=metadata.get("company_naic", metadata.get("naic_number")),
            filing_type=metadata.get("filing_type"),
            line_of_business=metadata.get("type_of_insurance", metadata.get("line_of_business")),
            status=metadata.get("filing_status", metadata.get("status")),
            effective_date=metadata.get("effective_date"),
            filed_date=metadata.get("date_filed", metadata.get("filed_date")),
            description=metadata.get("filing_description", metadata.get("description")),
            raw_metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Document links
    # ------------------------------------------------------------------

    async def get_document_links(self, page: Any) -> list[dict]:
        """Extract all downloadable document links from a filing detail page.

        Scans all anchor tags whose ``href`` attribute looks like a document
        download URL (PDF, DOC, etc.) or explicitly contains ``/document/``.

        Parameters
        ----------
        page:
            Playwright ``Page`` on a filing detail page.

        Returns
        -------
        list[dict]
            Each entry has keys ``name`` (link text) and ``url`` (absolute URL).
        """
        logger.debug("Extracting document links")
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        doc_links: list[dict] = []
        seen_urls: set[str] = set()

        doc_url_pattern = re.compile(
            r"(/document/|\.pdf|\.doc|\.docx|/download/|/filing/.*doc)", re.I
        )

        for anchor in soup.find_all("a", href=True):
            href: str = anchor["href"]
            if not doc_url_pattern.search(href):
                continue

            # Resolve relative URLs.
            if href.startswith("/"):
                href = f"https://filingaccess.serff.com{href}"
            elif not href.startswith("http"):
                continue

            if href in seen_urls:
                continue
            seen_urls.add(href)

            link_text = anchor.get_text(strip=True) or href.split("/")[-1]
            doc_links.append({"name": link_text, "url": href})

        logger.info("Found %d document links", len(doc_links))
        return doc_links

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _map_columns(self, headers: list[str]) -> dict[str, int]:
        """Return a mapping of semantic column name -> column index.

        Parameters
        ----------
        headers:
            Lower-cased header cell text values.

        Returns
        -------
        dict[str, int]
            Keys: ``serff``, ``carrier``, ``naic``, ``type``, ``lob``,
            ``status``, ``effective``, ``filed``.
        """
        mapping: dict[str, int] = {}
        for i, h in enumerate(headers):
            if "serff" in h or "tracking" in h:
                mapping.setdefault("serff", i)
            elif "company" in h or "carrier" in h:
                mapping.setdefault("carrier", i)
            elif "naic" in h:
                mapping.setdefault("naic", i)
            elif "filing type" in h or "type" in h:
                mapping.setdefault("type", i)
            elif "insurance" in h or "line" in h or "lob" in h:
                mapping.setdefault("lob", i)
            elif "status" in h:
                mapping.setdefault("status", i)
            elif "effective" in h:
                mapping.setdefault("effective", i)
            elif "filed" in h or "date" in h:
                mapping.setdefault("filed", i)
        return mapping

    def _parse_row(
        self, cells: list[Any], col_index: dict[str, int]
    ) -> FilingResult | None:
        """Convert a table row's cells into a :class:`FilingResult`.

        Parameters
        ----------
        cells:
            BeautifulSoup ``<td>`` elements for one result row.
        col_index:
            Column-position mapping from :meth:`_map_columns`.

        Returns
        -------
        FilingResult | None
            Parsed result, or ``None`` if the SERFF tracking number is empty.
        """

        def _cell(key: str) -> str:
            idx = col_index.get(key)
            if idx is None or idx >= len(cells):
                return ""
            return cells[idx].get_text(strip=True)

        # Extract any document hrefs from this row.
        doc_urls: list[str] = []
        for anchor in cells[col_index.get("serff", 0)].find_all("a", href=True):
            href = anchor["href"]
            if href.startswith("/"):
                href = f"https://filingaccess.serff.com{href}"
            doc_urls.append(href)

        serff_num = _cell("serff")
        if not serff_num:
            return None

        return FilingResult(
            serff_tracking_number=serff_num,
            carrier_name=_cell("carrier"),
            carrier_naic=_cell("naic") or None,
            filing_type=_cell("type") or None,
            line_of_business=_cell("lob") or None,
            status=_cell("status") or None,
            effective_date=_cell("effective") or None,
            filed_date=_cell("filed") or None,
            document_urls=doc_urls,
        )

    async def _try_fill(
        self, page: Any, selectors: list[str], value: str
    ) -> bool:
        """Try each selector in order and fill the first visible input.

        Parameters
        ----------
        page:
            Playwright ``Page``.
        selectors:
            CSS selectors to try in priority order.
        value:
            Text to type into the field.

        Returns
        -------
        bool
            ``True`` if a field was successfully filled, ``False`` otherwise.
        """
        for sel in selectors:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=1500):
                    await el.fill(value)
                    logger.debug("Filled %s = %r", sel, value)
                    return True
            except Exception:
                continue
        logger.debug("Could not fill any of: %s", selectors)
        return False

    async def _try_select(
        self, page: Any, selectors: list[str], value: str
    ) -> bool:
        """Try each selector in order and select the matching option.

        Attempts selection by exact value, then by label text (partial match).

        Parameters
        ----------
        page:
            Playwright ``Page``.
        selectors:
            CSS selectors for ``<select>`` elements to try.
        value:
            Option value or visible label text to select.

        Returns
        -------
        bool
            ``True`` if an option was successfully selected.
        """
        for sel in selectors:
            try:
                el = page.locator(sel).first
                if not await el.is_visible(timeout=1500):
                    continue
                # Try exact value match first.
                try:
                    await el.select_option(value=value, timeout=1000)
                    logger.debug("Selected %s (by value) in %s", value, sel)
                    return True
                except Exception:
                    pass
                # Fall back to label match.
                try:
                    await el.select_option(label=value, timeout=1000)
                    logger.debug("Selected %s (by label) in %s", value, sel)
                    return True
                except Exception:
                    pass
            except Exception:
                continue
        logger.debug("Could not select any of: %s", selectors)
        return False
