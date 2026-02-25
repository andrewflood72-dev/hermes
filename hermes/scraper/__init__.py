"""Hermes SERFF Scraper Engine.

Exports the core scraper classes and state-specific implementations used to
automate retrieval of insurance regulatory filings from SERFF Filing Access
(filingaccess.serff.com) portals.
"""

from hermes.scraper.base import (
    BaseSERFFScraper,
    FilingResult,
    ScrapeResult,
    SearchParams,
)
from hermes.scraper.filing_downloader import FilingDownloader
from hermes.scraper.states.california import CaliforniaScraper
from hermes.scraper.states.texas import TexasScraper

__all__ = [
    "BaseSERFFScraper",
    "FilingDownloader",
    "FilingResult",
    "ScrapeResult",
    "SearchParams",
    "TexasScraper",
    "CaliforniaScraper",
]
