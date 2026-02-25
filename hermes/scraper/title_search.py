"""Title-specific SearchParams builder for SERFF portal searches.

Sets line_of_business to "Title Insurance", provides title carrier NAIC
list, and state-specific filing type guidance for the SERFF scraper.
"""

from __future__ import annotations

import logging
from typing import Any

from hermes.scraper.base import SearchParams

logger = logging.getLogger("hermes.scraper.title_search")

# Title insurance carrier NAIC codes (60001-60008)
TITLE_CARRIER_NAICS = [
    "60001",  # Fidelity National
    "60002",  # First American
    "60003",  # Old Republic
    "60004",  # Stewart Title
    "60005",  # WFG National
    "60006",  # Investors Title
    "60007",  # Westcor
    "60008",  # North American (Doma)
]

# States where title rates are promulgated (state-set)
PROMULGATED_STATES = {"TX", "NM", "FL"}

# States with title-specific SERFF filing requirements
STATE_FILING_GUIDANCE: dict[str, dict[str, Any]] = {
    "TX": {
        "note": "TX title rates are promulgated by TDI — use TDI scraper instead of SERFF",
        "filing_type": "rate",
        "skip_serff": True,
    },
    "NM": {
        "note": "NM title rates are promulgated — limited SERFF filings",
        "filing_type": "rate",
        "skip_serff": False,
    },
    "FL": {
        "note": "FL promulgated rates — check both SERFF and OIR portal",
        "filing_type": "rate",
        "skip_serff": False,
    },
    "NY": {
        "note": "NY title rates filed individually — full SERFF search needed",
        "filing_type": "rate",
        "skip_serff": False,
    },
    "CA": {
        "note": "CA title carriers file via CDI portal — SERFF may have limited data",
        "filing_type": "rate",
        "skip_serff": False,
    },
}


def build_title_search_params(
    state: str,
    carrier_naic: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    max_pages: int = 50,
) -> SearchParams | None:
    """Build a SERFF SearchParams for title insurance filings.

    Parameters
    ----------
    state:
        Two-letter state code.
    carrier_naic:
        Optional specific carrier NAIC to search for.
    date_from:
        Filed-date range start (MM/DD/YYYY).
    date_to:
        Filed-date range end (MM/DD/YYYY).
    max_pages:
        Maximum results pages to scrape.

    Returns
    -------
    SearchParams or None if the state should use a non-SERFF source.
    """
    state = state.upper()
    guidance = STATE_FILING_GUIDANCE.get(state, {})

    if guidance.get("skip_serff"):
        logger.info(
            "Skipping SERFF search for %s title: %s",
            state, guidance.get("note", "state uses non-SERFF source"),
        )
        return None

    params = SearchParams(
        state=state,
        line_of_business="Title Insurance",
        carrier_naic=carrier_naic,
        filing_type=guidance.get("filing_type", "rate"),
        date_from=date_from,
        date_to=date_to,
        max_pages=max_pages,
    )

    if guidance.get("note"):
        logger.info("Title SERFF search note for %s: %s", state, guidance["note"])

    return params


def get_title_carriers_for_state(state: str) -> list[str]:
    """Return NAIC codes of title carriers active in a given state.

    Currently returns all 8 carriers — can be refined with state-specific
    license data from hermes_carrier_licenses.
    """
    return TITLE_CARRIER_NAICS.copy()


def is_promulgated_state(state: str) -> bool:
    """Check if a state has promulgated (state-set) title rates."""
    return state.upper() in PROMULGATED_STATES
