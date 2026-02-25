"""Deep SERFF Filing Intelligence — click into every filing for real data.

Usage:
    python -m hermes.scripts.deep_scrape --state CA
    python -m hermes.scripts.deep_scrape --state CA --limit 50 --skip-download
    python -m hermes.scripts.deep_scrape --all
    python -m hermes.scripts.deep_scrape --state TX --batch-size 50

This script navigates to each filing's SERFF SFA detail page, extracts
metadata (dates, status, description), downloads attached documents
(underwriting guidelines, rate schedules, actuarial support PDFs), and
updates hermes_filings + hermes_filing_documents.
"""

import argparse
import asyncio
import logging
import sys
import time

from hermes.scraper.states.generic_sfa import GenericSFAScraper

# All states we've scraped listing data for
ALL_STATES = ["CA", "TX", "NY", "OH"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hermes.deep_scrape")


async def scrape_state(
    state: str,
    batch_size: int = 100,
    limit: int | None = None,
    skip_download: bool = False,
    download_only: bool = False,
) -> dict[str, int]:
    """Run detail scrape for a single state."""
    mode = "download-only" if download_only else ("metadata-only" if skip_download else "full")
    logger.info("=" * 60)
    logger.info("Starting deep scrape for %s (mode=%s)", state, mode)
    logger.info("  batch_size=%d  limit=%s", batch_size, limit)
    logger.info("=" * 60)

    scraper = GenericSFAScraper(state=state)
    start = time.monotonic()

    try:
        stats = await scraper.scrape_filing_details(
            batch_size=batch_size,
            limit=limit,
            skip_download=skip_download,
            download_only=download_only,
        )
    except Exception as exc:
        logger.error("State %s failed: %s", state, exc, exc_info=True)
        stats = {"scraped": 0, "documents": 0, "errors": 1}

    elapsed = round(time.monotonic() - start, 1)
    logger.info(
        "State %s complete in %.1fs: %d scraped, %d docs, %d errors",
        state, elapsed, stats.get("scraped", 0),
        stats.get("documents", 0), stats.get("errors", 0),
    )
    return stats


async def main():
    parser = argparse.ArgumentParser(
        description="Deep SERFF filing intelligence scraper"
    )
    parser.add_argument(
        "--state", type=str, help="Two-letter state code (e.g., CA, TX, NY, OH)"
    )
    parser.add_argument(
        "--all", action="store_true", help="Scrape all states sequentially"
    )
    parser.add_argument(
        "--batch-size", type=int, default=100,
        help="Filings per browser session (default: 100)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max filings to scrape per state (default: all)"
    )
    parser.add_argument(
        "--skip-download", action="store_true",
        help="Extract metadata only, skip PDF downloads"
    )
    parser.add_argument(
        "--download-only", action="store_true",
        help="Download documents for filings that already have metadata but no docs"
    )
    args = parser.parse_args()

    if not args.state and not args.all:
        parser.error("Specify --state STATE or --all")

    if args.skip_download and args.download_only:
        parser.error("--skip-download and --download-only are mutually exclusive")

    states = ALL_STATES if args.all else [args.state.upper()]
    total_stats = {"scraped": 0, "documents": 0, "errors": 0}

    for state in states:
        stats = await scrape_state(
            state=state,
            batch_size=args.batch_size,
            limit=args.limit,
            skip_download=args.skip_download,
            download_only=args.download_only,
        )
        for k in total_stats:
            total_stats[k] += stats.get(k, 0)

    logger.info("=" * 60)
    logger.info(
        "ALL DONE: %d filings scraped, %d documents, %d errors",
        total_stats["scraped"], total_stats["documents"], total_stats["errors"],
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
