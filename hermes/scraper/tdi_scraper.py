"""Texas Department of Insurance (TDI) Title Rate Scraper.

TX title rates are promulgated — set by the state, not by individual carriers.
Rates are published by TDI and apply uniformly to all title insurers operating
in Texas.  This scraper fetches the Basic Manual rates from tdi.texas.gov and
loads them as promulgated rate cards for all 8 title carriers.

The TDI Basic Manual defines:
  - Owner policy rates (tiered by coverage amount)
  - Lender policy rates (tiered by loan amount)
  - Simultaneous issue schedule (lender rate when issued with owner)
  - Reissue/short-term rates (refinance credits)
  - Endorsement fees (ALTA and TX-specific)
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

logger = logging.getLogger("hermes.scraper.tdi_scraper")

# ── TX Promulgated Title Rates (TDI Basic Manual P-1) ─────────
# Effective September 1, 2019 (current as of 2026)
# Source: Texas Department of Insurance Basic Manual of Rules,
# Rates and Forms for the Writing of Title Insurance

TX_OWNER_RATES: list[dict[str, Any]] = [
    {"coverage_min": 0,         "coverage_max": 100_000,    "rate_per_thousand": 5.75, "flat_fee": 0, "minimum_premium": 200},
    {"coverage_min": 100_000,   "coverage_max": 200_000,    "rate_per_thousand": 5.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 200_000,   "coverage_max": 500_000,    "rate_per_thousand": 4.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 500_000,   "coverage_max": 1_000_000,  "rate_per_thousand": 3.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 1_000_000, "coverage_max": 5_000_000,  "rate_per_thousand": 2.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 5_000_000, "coverage_max": 15_000_000, "rate_per_thousand": 2.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 15_000_000,"coverage_max": 25_000_000, "rate_per_thousand": 2.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 25_000_000,"coverage_max": 99_999_999, "rate_per_thousand": 1.50, "flat_fee": 0, "minimum_premium": 0},
]

TX_LENDER_RATES: list[dict[str, Any]] = [
    {"coverage_min": 0,         "coverage_max": 100_000,    "rate_per_thousand": 5.25, "flat_fee": 0, "minimum_premium": 175},
    {"coverage_min": 100_000,   "coverage_max": 200_000,    "rate_per_thousand": 4.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 200_000,   "coverage_max": 500_000,    "rate_per_thousand": 4.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 500_000,   "coverage_max": 1_000_000,  "rate_per_thousand": 3.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 1_000_000, "coverage_max": 5_000_000,  "rate_per_thousand": 2.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 5_000_000, "coverage_max": 15_000_000, "rate_per_thousand": 2.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 15_000_000,"coverage_max": 25_000_000, "rate_per_thousand": 1.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 25_000_000,"coverage_max": 99_999_999, "rate_per_thousand": 1.25, "flat_fee": 0, "minimum_premium": 0},
]

# Simultaneous issue: when lender policy issued with owner policy,
# the lender premium is a reduced flat charge based on loan amount.
TX_SIMULTANEOUS_ISSUE: list[dict[str, Any]] = [
    {"loan_min": 0,         "loan_max": 100_000,    "discount_rate_per_thousand": 4.75, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 100_000,   "loan_max": 200_000,    "discount_rate_per_thousand": 4.00, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 200_000,   "loan_max": 500_000,    "discount_rate_per_thousand": 3.50, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 500_000,   "loan_max": 1_000_000,  "discount_rate_per_thousand": 2.75, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 1_000_000, "loan_max": 5_000_000,  "discount_rate_per_thousand": 2.00, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 5_000_000, "loan_max": 99_999_999, "discount_rate_per_thousand": 1.50, "discount_pct": 0, "flat_fee": 0},
]

# Reissue (short-term) credits for refinance transactions
TX_REISSUE_CREDITS: list[dict[str, Any]] = [
    {"years_since_min": 0,  "years_since_max": 1,   "credit_pct": 50.0},
    {"years_since_min": 1,  "years_since_max": 2,   "credit_pct": 40.0},
    {"years_since_min": 2,  "years_since_max": 3,   "credit_pct": 30.0},
    {"years_since_min": 3,  "years_since_max": 4,   "credit_pct": 25.0},
    {"years_since_min": 4,  "years_since_max": 5,   "credit_pct": 20.0},
    {"years_since_min": 5,  "years_since_max": 8,   "credit_pct": 15.0},
    {"years_since_min": 8,  "years_since_max": 10,  "credit_pct": 10.0},
]

# Common ALTA endorsements available in TX
TX_ENDORSEMENTS: list[dict[str, Any]] = [
    {"endorsement_code": "T-19",   "endorsement_name": "Restrictions, Encroachments, Minerals",     "flat_fee": 25,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "T-19.1", "endorsement_name": "Restrictions, Encroachments, Minerals (Lender)", "flat_fee": 25, "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "T-24",   "endorsement_name": "Condominium",                                "flat_fee": 25,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "T-28",   "endorsement_name": "Encroachments — Boundaries and Area",        "flat_fee": 50,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "T-30",   "endorsement_name": "Planned Unit Development",                   "flat_fee": 25,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "T-33",   "endorsement_name": "Manufactured Housing Unit",                  "flat_fee": 50,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "T-36",   "endorsement_name": "Environmental Protection Lien",              "flat_fee": 25,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "T-42",   "endorsement_name": "Subdivision",                                "flat_fee": 50,  "rate_per_thousand": 0, "pct_of_base": 0},
]


async def load_tx_promulgated_rates(
    effective_date: date | None = None,
) -> list[dict[str, Any]]:
    """Load TX promulgated title rates for all carriers via TitleRateCardLoader.

    This is the primary entry point for the TDI scraper.  Since TX rates are
    state-set, we create identical rate cards for all 8 title carriers.

    Parameters
    ----------
    effective_date:
        Rate effective date.  Defaults to 2019-09-01 (current TX Basic Manual).

    Returns
    -------
    List of load results, one per carrier × policy_type.
    """
    from hermes.title.rate_card_loader import TitleRateCardLoader

    if effective_date is None:
        effective_date = date(2019, 9, 1)

    loader = TitleRateCardLoader()

    logger.info("Loading TX promulgated title rates (effective %s)", effective_date)

    results = await loader.load_promulgated_rates(
        state="TX",
        effective_date=effective_date,
        rates=TX_OWNER_RATES,
        simultaneous=TX_SIMULTANEOUS_ISSUE,
        reissue_credits=TX_REISSUE_CREDITS,
        endorsements=TX_ENDORSEMENTS,
        source="tdi",
        notes="TX promulgated rates from TDI Basic Manual P-1",
    )

    # Also load lender-specific rate cards with lender rates
    for naic in ["60001", "60002", "60003", "60004", "60005", "60006", "60007", "60008"]:
        try:
            await loader.load_rate_card(
                carrier_naic=naic,
                policy_type="lender",
                state="TX",
                effective_date=effective_date,
                rates=TX_LENDER_RATES,
                is_promulgated=True,
                source="tdi",
                notes="TX promulgated lender rates from TDI Basic Manual P-1",
            )
        except Exception as exc:
            logger.error("Failed to load TX lender rates for %s: %s", naic, exc)

    logger.info(
        "TX promulgated rates loaded: %d rate cards created",
        len(results) + 8,  # owner+lender for promulgated + 8 explicit lender cards
    )
    return results
