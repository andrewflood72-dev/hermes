#!/usr/bin/env python3
"""Scrape PMI rate filings from top 5 mortgage guaranty carriers.

Targets rate filings only across 10 key states, then filters downloaded
PDFs by page count (100–500 pages) to isolate actual rate manuals vs
small amendments or massive combined filings.

Usage:
    cd C:/Users/andre/hermes
    python -m scripts.scrape_pmi_carriers              # full run
    python -m scripts.scrape_pmi_carriers --phase 1    # search only
    python -m scripts.scrape_pmi_carriers --phase 2    # download only
    python -m scripts.scrape_pmi_carriers --phase 3    # page-count analysis only
    python -m scripts.scrape_pmi_carriers --states CA TX FL  # subset of states
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

# Ensure hermes package is importable when run as `python -m scripts.scrape_pmi_carriers`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hermes.config import settings
from hermes.scraper.base import SearchParams, ScrapeResult
from hermes.scraper.states.generic_sfa import GenericSFAScraper

logger = logging.getLogger("hermes.pmi_scraper")

# ---------------------------------------------------------------------------
# PMI Carrier definitions
# ---------------------------------------------------------------------------

PMI_CARRIERS = [
    {"name": "MGIC Investment Corporation", "naic": "29858", "short": "MGIC", "domicile": "WI"},
    {"name": "Arch Mortgage Insurance Company", "naic": "40266", "short": "Arch MI", "domicile": "WI"},
    {"name": "Essent Guaranty Inc", "naic": "13634", "short": "Essent", "domicile": "DE"},
    {"name": "Enact Mortgage Insurance Corporation", "naic": "38458", "short": "Enact", "domicile": "NC"},
    {"name": "National Mortgage Insurance Corporation", "naic": "13695", "short": "NMI", "domicile": "WI"},
]

# Top 10 states by mortgage origination volume
TARGET_STATES = ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "MI", "NC", "WI"]

# SERFF product name search strategy (try in order, stop on first hit)
# "Mortgage Guaranty" is the standard SERFF Type of Insurance term (confirmed)
# Fallback: NAIC-only search with no product filter catches anything filed
# under non-standard names
PMI_SEARCH_STRATEGIES = [
    {"label": "Mortgage Guaranty", "lob": "Mortgage Guaranty"},
    {"label": "NAIC-only (no LOB filter)", "lob": None},
]

# Page count sweet spot for actual rate manuals
MIN_PAGES = 100
MAX_PAGES = 500


# ---------------------------------------------------------------------------
# Data tracking
# ---------------------------------------------------------------------------

@dataclass
class CarrierStateResult:
    carrier: str
    naic: str
    state: str
    filings_found: int = 0
    filings_new: int = 0
    rate_filings: int = 0
    errors: list[str] = field(default_factory=list)
    duration: float = 0.0


@dataclass
class PageCountResult:
    file_path: str
    pages: int
    size_mb: float
    carrier: str
    state: str
    in_range: bool = False


# ---------------------------------------------------------------------------
# Phase 1: Search SERFF for each carrier × state
# ---------------------------------------------------------------------------

async def phase1_search(
    carriers: list[dict],
    states: list[str],
) -> list[CarrierStateResult]:
    """Search SERFF for PMI rate filings across all carrier×state combos."""
    results: list[CarrierStateResult] = []
    total_combos = len(carriers) * len(states)
    completed = 0

    for carrier in carriers:
        for state in states:
            completed += 1
            short = carrier["short"]
            naic = carrier["naic"]
            logger.info(
                "=== [%d/%d] Searching %s (NAIC %s) in %s ===",
                completed, total_combos, short, naic, state,
            )

            csr = CarrierStateResult(carrier=short, naic=naic, state=state)
            start = time.monotonic()

            # Try search strategies in order — stop on first hit
            best_result: ScrapeResult | None = None
            for strategy in PMI_SEARCH_STRATEGIES:
                label = strategy["label"]
                lob = strategy["lob"]
                try:
                    scraper = GenericSFAScraper(state=state)
                    params = SearchParams(
                        state=state,
                        carrier_naic=naic,
                        line_of_business=lob,
                        filing_type=None,  # filter post-hoc; SERFF type filter unreliable
                        date_from="01/01/2020",  # last ~6 years
                        max_pages=10,  # cap at 200 filings per combo
                    )
                    result = await scraper.scrape(params)

                    if result.filings_found > 0:
                        logger.info(
                            "[%s/%s] Found %d filings via '%s'",
                            short, state, result.filings_found, label,
                        )
                        best_result = result
                        break
                    else:
                        logger.info(
                            "[%s/%s] 0 filings via '%s', trying next strategy",
                            short, state, label,
                        )

                except Exception as exc:
                    msg = f"{short}/{state} search failed ({label}): {exc}"
                    logger.error(msg)
                    csr.errors.append(msg)

            if best_result:
                csr.filings_found = best_result.filings_found
                csr.filings_new = best_result.filings_new
                if best_result.errors:
                    csr.errors.extend(best_result.errors)

            csr.duration = round(time.monotonic() - start, 2)
            results.append(csr)

            logger.info(
                "[%s/%s] Done: %d found, %d new, %.1fs",
                short, state, csr.filings_found, csr.filings_new, csr.duration,
            )

    return results


# ---------------------------------------------------------------------------
# Phase 2: Download documents for rate filings
# ---------------------------------------------------------------------------

async def phase2_download(
    carriers: list[dict],
    states: list[str],
) -> dict[str, int]:
    """Download documents for PMI rate filings already in the DB.

    Uses scrape_filing_details() which opens each filing's detail page
    and downloads attached documents.
    """
    stats = {"total_scraped": 0, "total_docs": 0, "total_errors": 0}

    for carrier in carriers:
        for state in states:
            short = carrier["short"]
            naic = carrier["naic"]
            logger.info(
                "=== Downloading docs for %s in %s ===", short, state,
            )

            try:
                scraper = GenericSFAScraper(state=state)
                detail_stats = await scraper.scrape_filing_details(
                    batch_size=100,
                    limit=None,
                    skip_download=False,
                )
                stats["total_scraped"] += detail_stats.get("scraped", 0)
                stats["total_docs"] += detail_stats.get("documents", 0)
                stats["total_errors"] += detail_stats.get("errors", 0)
                logger.info(
                    "[%s/%s] Detail scrape: scraped=%d docs=%d errors=%d",
                    short, state,
                    detail_stats.get("scraped", 0),
                    detail_stats.get("documents", 0),
                    detail_stats.get("errors", 0),
                )
            except Exception as exc:
                logger.error("[%s/%s] Detail scrape failed: %s", short, state, exc)
                stats["total_errors"] += 1

    return stats


# ---------------------------------------------------------------------------
# Phase 3: Page count analysis — filter PDFs by page count
# ---------------------------------------------------------------------------

def phase3_analyze(
    carriers: list[dict],
    states: list[str],
) -> list[PageCountResult]:
    """Scan downloaded PDFs and filter by page count (100–500 pages)."""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        logger.error(
            "PyMuPDF (fitz) not installed. Install with: pip install PyMuPDF"
        )
        return []

    storage_root = Path(settings.filing_storage_path)
    all_results: list[PageCountResult] = []
    in_range_count = 0

    # Build a set of carrier NAICs for filtering
    target_naics = {c["naic"] for c in carriers}

    for state in states:
        state_dir = storage_root / state
        if not state_dir.exists():
            logger.info("[%s] No filing directory found — skipping", state)
            continue

        # Walk through all PDFs under state directory
        for pdf_path in state_dir.rglob("*.pdf"):
            # Try to identify the carrier from directory structure
            # Structure: data/filings/{state}/{carrier_name_or_naic}/{serff_num}/file.pdf
            parts = pdf_path.relative_to(storage_root).parts
            carrier_dir = parts[1] if len(parts) > 1 else "unknown"

            # Check if this belongs to one of our target carriers
            carrier_short = _identify_carrier(carrier_dir, carriers)
            if carrier_short is None:
                continue  # not one of our PMI carriers

            try:
                doc = fitz.open(str(pdf_path))
                page_count = doc.page_count
                file_size_mb = pdf_path.stat().st_size / (1024 * 1024)
                doc.close()

                in_range = MIN_PAGES <= page_count <= MAX_PAGES
                result = PageCountResult(
                    file_path=str(pdf_path),
                    pages=page_count,
                    size_mb=round(file_size_mb, 2),
                    carrier=carrier_short,
                    state=state,
                    in_range=in_range,
                )
                all_results.append(result)

                if in_range:
                    in_range_count += 1
                    logger.info(
                        "  [TARGET] %s — %d pages, %.1f MB — %s/%s",
                        pdf_path.name, page_count, file_size_mb,
                        carrier_short, state,
                    )
                else:
                    logger.debug(
                        "  [skip] %s — %d pages, %.1f MB",
                        pdf_path.name, page_count, file_size_mb,
                    )

            except Exception as exc:
                logger.warning("Could not read %s: %s", pdf_path, exc)

    # Summary
    logger.info(
        "\n=== PAGE COUNT ANALYSIS ===\n"
        "Total PDFs scanned: %d\n"
        "In range (%d–%d pages): %d\n"
        "Outside range: %d",
        len(all_results), MIN_PAGES, MAX_PAGES,
        in_range_count, len(all_results) - in_range_count,
    )

    # Breakdown by carrier
    from collections import Counter
    carrier_counts = Counter()
    carrier_in_range = Counter()
    for r in all_results:
        carrier_counts[r.carrier] += 1
        if r.in_range:
            carrier_in_range[r.carrier] += 1

    logger.info("\n--- By Carrier ---")
    for carrier in sorted(carrier_counts.keys()):
        logger.info(
            "  %s: %d total, %d in range",
            carrier, carrier_counts[carrier], carrier_in_range[carrier],
        )

    # Print the target filings in a nice table
    targets = [r for r in all_results if r.in_range]
    if targets:
        logger.info("\n--- Target Rate Manuals (%d–%d pages) ---", MIN_PAGES, MAX_PAGES)
        targets.sort(key=lambda r: (r.carrier, r.state, -r.pages))
        for t in targets:
            logger.info(
                "  %-10s %-3s %4d pp  %6.1f MB  %s",
                t.carrier, t.state, t.pages, t.size_mb,
                Path(t.file_path).name,
            )

    return all_results


def _identify_carrier(
    dir_name: str, carriers: list[dict]
) -> str | None:
    """Try to match a directory name to one of our PMI carriers."""
    dir_lower = dir_name.lower().replace("_", " ").replace("-", " ")

    for c in carriers:
        # Match by NAIC code
        if c["naic"] in dir_name:
            return c["short"]
        # Match by name fragments
        short_lower = c["short"].lower()
        name_lower = c["name"].lower()
        if short_lower in dir_lower:
            return c["short"]
        # Check key words from carrier name
        keywords = name_lower.split()
        if any(kw in dir_lower for kw in keywords if len(kw) > 3):
            return c["short"]

    return None


# ---------------------------------------------------------------------------
# Summary report
# ---------------------------------------------------------------------------

def print_search_summary(results: list[CarrierStateResult]) -> None:
    """Print a summary table of Phase 1 search results."""
    print("\n" + "=" * 70)
    print("PMI FILING SEARCH SUMMARY")
    print("=" * 70)
    print(f"{'Carrier':<12} {'State':<6} {'Found':>6} {'New':>6} {'Errors':>7} {'Time':>7}")
    print("-" * 70)

    total_found = 0
    total_new = 0
    total_errors = 0
    for r in results:
        total_found += r.filings_found
        total_new += r.filings_new
        total_errors += len(r.errors)
        err_str = str(len(r.errors)) if r.errors else ""
        print(
            f"{r.carrier:<12} {r.state:<6} {r.filings_found:>6} "
            f"{r.filings_new:>6} {err_str:>7} {r.duration:>6.1f}s"
        )

    print("-" * 70)
    print(
        f"{'TOTAL':<12} {'':6} {total_found:>6} "
        f"{total_new:>6} {total_errors:>7}"
    )
    print("=" * 70)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape PMI rate filings from top 5 carriers"
    )
    parser.add_argument(
        "--phase", type=int, choices=[1, 2, 3], default=None,
        help="Run a specific phase (1=search, 2=download, 3=analyze). Default: all.",
    )
    parser.add_argument(
        "--states", nargs="+", default=None,
        help="Subset of states to scrape (e.g. --states CA TX FL)",
    )
    parser.add_argument(
        "--carriers", nargs="+", default=None,
        help="Subset of carriers by short name (e.g. --carriers MGIC Essent)",
    )
    args = parser.parse_args()

    states = [s.upper() for s in args.states] if args.states else TARGET_STATES
    if args.carriers:
        carriers = [
            c for c in PMI_CARRIERS
            if c["short"].upper() in [x.upper() for x in args.carriers]
        ]
        if not carriers:
            print(f"No matching carriers. Available: {[c['short'] for c in PMI_CARRIERS]}")
            sys.exit(1)
    else:
        carriers = PMI_CARRIERS

    run_all = args.phase is None
    total_start = time.monotonic()

    print(f"\nPMI Rate Filing Scraper")
    print(f"Carriers: {[c['short'] for c in carriers]}")
    print(f"States:   {states}")
    print(f"Phase:    {'all' if run_all else args.phase}\n")

    # Phase 1: Search
    if run_all or args.phase == 1:
        print("\n--- PHASE 1: Search SERFF for PMI rate filings ---\n")
        search_results = await phase1_search(carriers, states)
        print_search_summary(search_results)

    # Phase 2: Download documents
    if run_all or args.phase == 2:
        print("\n--- PHASE 2: Download filing documents ---\n")
        dl_stats = await phase2_download(carriers, states)
        print(f"\nDownload complete: {dl_stats}")

    # Phase 3: Analyze page counts
    if run_all or args.phase == 3:
        print("\n--- PHASE 3: Page count analysis ---\n")
        phase3_analyze(carriers, states)

    elapsed = round(time.monotonic() - total_start, 1)
    print(f"\nTotal elapsed: {elapsed}s")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet down noisy libraries
    logging.getLogger("playwright").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    asyncio.run(main())
