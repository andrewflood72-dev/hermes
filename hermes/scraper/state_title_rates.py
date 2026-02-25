"""State-specific title insurance rate data and loaders for FL, NM, NY, CA.

FL and NM are promulgated states — rates are set by the state regulator and
apply uniformly to all title insurers.  NY and CA are filed states — each
carrier files its own rate schedule with the state DOI, producing real price
differentiation in matching results.

Follows the pattern established by tdi_scraper.py for TX promulgated rates.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

logger = logging.getLogger("hermes.scraper.state_title_rates")


# ── FL Promulgated Rates (Florida OIR) ────────────────────────────────
# Effective July 1, 2023
# Source: Florida Office of Insurance Regulation — Title Insurance
# FL is a slightly cheaper market than TX

FL_OWNER_RATES: list[dict[str, Any]] = [
    {"coverage_min": 0,          "coverage_max": 100_000,    "rate_per_thousand": 5.00, "flat_fee": 0, "minimum_premium": 175},
    {"coverage_min": 100_000,    "coverage_max": 200_000,    "rate_per_thousand": 4.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 200_000,    "coverage_max": 500_000,    "rate_per_thousand": 3.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 500_000,    "coverage_max": 1_000_000,  "rate_per_thousand": 3.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 1_000_000,  "coverage_max": 5_000_000,  "rate_per_thousand": 2.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 5_000_000,  "coverage_max": 15_000_000, "rate_per_thousand": 2.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 15_000_000, "coverage_max": 25_000_000, "rate_per_thousand": 1.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 25_000_000, "coverage_max": 99_999_999, "rate_per_thousand": 1.25, "flat_fee": 0, "minimum_premium": 0},
]

FL_LENDER_RATES: list[dict[str, Any]] = [
    {"coverage_min": 0,          "coverage_max": 100_000,    "rate_per_thousand": 4.50, "flat_fee": 0, "minimum_premium": 150},
    {"coverage_min": 100_000,    "coverage_max": 200_000,    "rate_per_thousand": 4.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 200_000,    "coverage_max": 500_000,    "rate_per_thousand": 3.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 500_000,    "coverage_max": 1_000_000,  "rate_per_thousand": 2.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 1_000_000,  "coverage_max": 5_000_000,  "rate_per_thousand": 2.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 5_000_000,  "coverage_max": 15_000_000, "rate_per_thousand": 1.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 15_000_000, "coverage_max": 25_000_000, "rate_per_thousand": 1.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 25_000_000, "coverage_max": 99_999_999, "rate_per_thousand": 1.00, "flat_fee": 0, "minimum_premium": 0},
]

FL_SIMULTANEOUS_ISSUE: list[dict[str, Any]] = [
    {"loan_min": 0,          "loan_max": 100_000,    "discount_rate_per_thousand": 4.00, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 100_000,    "loan_max": 200_000,    "discount_rate_per_thousand": 3.50, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 200_000,    "loan_max": 500_000,    "discount_rate_per_thousand": 2.75, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 500_000,    "loan_max": 1_000_000,  "discount_rate_per_thousand": 2.25, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 1_000_000,  "loan_max": 5_000_000,  "discount_rate_per_thousand": 1.75, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 5_000_000,  "loan_max": 99_999_999, "discount_rate_per_thousand": 1.25, "discount_pct": 0, "flat_fee": 0},
]

FL_REISSUE_CREDITS: list[dict[str, Any]] = [
    {"years_since_min": 0, "years_since_max": 1, "credit_pct": 40.0},
    {"years_since_min": 1, "years_since_max": 2, "credit_pct": 30.0},
    {"years_since_min": 2, "years_since_max": 3, "credit_pct": 25.0},
    {"years_since_min": 3, "years_since_max": 5, "credit_pct": 15.0},
    {"years_since_min": 5, "years_since_max": 7, "credit_pct": 10.0},
]

FL_ENDORSEMENTS: list[dict[str, Any]] = [
    {"endorsement_code": "ALTA 9",   "endorsement_name": "Restrictions, Encroachments, Minerals",      "flat_fee": 25,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 8.1", "endorsement_name": "Environmental Protection Lien",              "flat_fee": 30,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 17",  "endorsement_name": "Access and Entry",                           "flat_fee": 50,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 18",  "endorsement_name": "Single Tax Parcel",                          "flat_fee": 25,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 22",  "endorsement_name": "Location",                                   "flat_fee": 35,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 25",  "endorsement_name": "Same as Survey",                             "flat_fee": 35,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 28",  "endorsement_name": "Easement — Damage or Enforced Removal",      "flat_fee": 50,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 35",  "endorsement_name": "Minerals and Other Subsurface Substances",   "flat_fee": 75,  "rate_per_thousand": 0, "pct_of_base": 0},
]


# ── NM Promulgated Rates (NM Office of Superintendent of Insurance) ──
# Effective January 1, 2022
# NM uses a simpler 6-tier schedule and is a pricier market

NM_OWNER_RATES: list[dict[str, Any]] = [
    {"coverage_min": 0,          "coverage_max": 100_000,    "rate_per_thousand": 6.00, "flat_fee": 0, "minimum_premium": 225},
    {"coverage_min": 100_000,    "coverage_max": 250_000,    "rate_per_thousand": 5.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 250_000,    "coverage_max": 500_000,    "rate_per_thousand": 4.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 500_000,    "coverage_max": 1_000_000,  "rate_per_thousand": 3.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 1_000_000,  "coverage_max": 5_000_000,  "rate_per_thousand": 2.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 5_000_000,  "coverage_max": 99_999_999, "rate_per_thousand": 1.75, "flat_fee": 0, "minimum_premium": 0},
]

NM_LENDER_RATES: list[dict[str, Any]] = [
    {"coverage_min": 0,          "coverage_max": 100_000,    "rate_per_thousand": 5.50, "flat_fee": 0, "minimum_premium": 200},
    {"coverage_min": 100_000,    "coverage_max": 250_000,    "rate_per_thousand": 4.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 250_000,    "coverage_max": 500_000,    "rate_per_thousand": 3.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 500_000,    "coverage_max": 1_000_000,  "rate_per_thousand": 3.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 1_000_000,  "coverage_max": 5_000_000,  "rate_per_thousand": 2.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 5_000_000,  "coverage_max": 99_999_999, "rate_per_thousand": 1.50, "flat_fee": 0, "minimum_premium": 0},
]

NM_SIMULTANEOUS_ISSUE: list[dict[str, Any]] = [
    {"loan_min": 0,          "loan_max": 100_000,    "discount_rate_per_thousand": 5.00, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 100_000,    "loan_max": 250_000,    "discount_rate_per_thousand": 4.00, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 250_000,    "loan_max": 500_000,    "discount_rate_per_thousand": 3.25, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 500_000,    "loan_max": 1_000_000,  "discount_rate_per_thousand": 2.50, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 1_000_000,  "loan_max": 99_999_999, "discount_rate_per_thousand": 1.75, "discount_pct": 0, "flat_fee": 0},
]

NM_REISSUE_CREDITS: list[dict[str, Any]] = [
    {"years_since_min": 0, "years_since_max": 1, "credit_pct": 35.0},
    {"years_since_min": 1, "years_since_max": 2, "credit_pct": 25.0},
    {"years_since_min": 2, "years_since_max": 4, "credit_pct": 15.0},
    {"years_since_min": 4, "years_since_max": 5, "credit_pct": 10.0},
]

NM_ENDORSEMENTS: list[dict[str, Any]] = [
    {"endorsement_code": "ALTA 9",   "endorsement_name": "Restrictions, Encroachments, Minerals",      "flat_fee": 35,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 8.1", "endorsement_name": "Environmental Protection Lien",              "flat_fee": 25,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 17",  "endorsement_name": "Access and Entry",                           "flat_fee": 50,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 22",  "endorsement_name": "Location",                                   "flat_fee": 40,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 25",  "endorsement_name": "Same as Survey",                             "flat_fee": 40,  "rate_per_thousand": 0, "pct_of_base": 0},
    {"endorsement_code": "ALTA 35",  "endorsement_name": "Minerals and Other Subsurface Substances",   "flat_fee": 100, "rate_per_thousand": 0, "pct_of_base": 0},
]


# ── NY Filed Rates (NY Department of Financial Services) ──────────────
# Effective January 1, 2024
# NY is an expensive market — each carrier files independently with DFS.
# Per-carrier multipliers applied to base rates create price differentiation.

NY_BASE_OWNER_RATES: list[dict[str, Any]] = [
    {"coverage_min": 0,          "coverage_max": 100_000,    "rate_per_thousand": 6.50, "flat_fee": 0, "minimum_premium": 250},
    {"coverage_min": 100_000,    "coverage_max": 200_000,    "rate_per_thousand": 5.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 200_000,    "coverage_max": 500_000,    "rate_per_thousand": 4.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 500_000,    "coverage_max": 1_000_000,  "rate_per_thousand": 3.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 1_000_000,  "coverage_max": 5_000_000,  "rate_per_thousand": 3.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 5_000_000,  "coverage_max": 15_000_000, "rate_per_thousand": 2.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 15_000_000, "coverage_max": 25_000_000, "rate_per_thousand": 2.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 25_000_000, "coverage_max": 99_999_999, "rate_per_thousand": 1.75, "flat_fee": 0, "minimum_premium": 0},
]

NY_BASE_LENDER_RATES: list[dict[str, Any]] = [
    {"coverage_min": 0,          "coverage_max": 100_000,    "rate_per_thousand": 5.75, "flat_fee": 0, "minimum_premium": 225},
    {"coverage_min": 100_000,    "coverage_max": 200_000,    "rate_per_thousand": 4.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 200_000,    "coverage_max": 500_000,    "rate_per_thousand": 4.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 500_000,    "coverage_max": 1_000_000,  "rate_per_thousand": 3.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 1_000_000,  "coverage_max": 5_000_000,  "rate_per_thousand": 2.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 5_000_000,  "coverage_max": 15_000_000, "rate_per_thousand": 2.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 15_000_000, "coverage_max": 25_000_000, "rate_per_thousand": 1.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 25_000_000, "coverage_max": 99_999_999, "rate_per_thousand": 1.50, "flat_fee": 0, "minimum_premium": 0},
]

NY_BASE_SIMULTANEOUS: list[dict[str, Any]] = [
    {"loan_min": 0,          "loan_max": 100_000,    "discount_rate_per_thousand": 5.25, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 100_000,    "loan_max": 200_000,    "discount_rate_per_thousand": 4.25, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 200_000,    "loan_max": 500_000,    "discount_rate_per_thousand": 3.50, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 500_000,    "loan_max": 1_000_000,  "discount_rate_per_thousand": 2.75, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 1_000_000,  "loan_max": 5_000_000,  "discount_rate_per_thousand": 2.25, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 5_000_000,  "loan_max": 99_999_999, "discount_rate_per_thousand": 1.50, "discount_pct": 0, "flat_fee": 0},
]

NY_BASE_REISSUE: list[dict[str, Any]] = [
    {"years_since_min": 0, "years_since_max": 1, "credit_pct": 45.0},
    {"years_since_min": 1, "years_since_max": 2, "credit_pct": 35.0},
    {"years_since_min": 2, "years_since_max": 3, "credit_pct": 25.0},
    {"years_since_min": 3, "years_since_max": 5, "credit_pct": 20.0},
    {"years_since_min": 5, "years_since_max": 8, "credit_pct": 15.0},
    {"years_since_min": 8, "years_since_max": 10, "credit_pct": 10.0},
]

# Pool of 10 ALTA endorsements — each carrier files a subset (6-8)
NY_ENDORSEMENT_POOL: list[dict[str, Any]] = [
    {"endorsement_code": "ALTA 5",   "endorsement_name": "Planned Unit Development",                   "base_fee": 25},
    {"endorsement_code": "ALTA 6",   "endorsement_name": "Variable Rate Mortgage",                     "base_fee": 25},
    {"endorsement_code": "ALTA 8.1", "endorsement_name": "Environmental Protection Lien",              "base_fee": 30},
    {"endorsement_code": "ALTA 9",   "endorsement_name": "Restrictions, Encroachments, Minerals",      "base_fee": 35},
    {"endorsement_code": "ALTA 17",  "endorsement_name": "Access and Entry",                           "base_fee": 60},
    {"endorsement_code": "ALTA 18",  "endorsement_name": "Single Tax Parcel",                          "base_fee": 30},
    {"endorsement_code": "ALTA 22",  "endorsement_name": "Location",                                   "base_fee": 40},
    {"endorsement_code": "ALTA 25",  "endorsement_name": "Same as Survey",                             "base_fee": 45},
    {"endorsement_code": "ALTA 28",  "endorsement_name": "Easement — Damage or Enforced Removal",      "base_fee": 60},
    {"endorsement_code": "ALTA 35",  "endorsement_name": "Minerals and Other Subsurface Substances",   "base_fee": 100},
]

# Per-carrier rate multipliers (applied to base rates, simultaneous, endorsement fees)
NY_CARRIER_MULTIPLIERS: dict[str, float] = {
    "60001": 0.95,  # Fidelity National
    "60002": 0.98,  # First American
    "60003": 1.02,  # Old Republic
    "60004": 1.00,  # Stewart Title
    "60005": 0.93,  # WFG National
    "60006": 1.05,  # Investors Title
    "60007": 1.08,  # Westcor
    "60008": 0.97,  # North American (Doma)
}

# Indices into NY_ENDORSEMENT_POOL that each carrier files
NY_CARRIER_ENDORSEMENT_INDICES: dict[str, list[int]] = {
    "60001": [0, 2, 3, 4, 5, 6, 7, 9],  # Fidelity — 8/10
    "60002": [0, 1, 2, 3, 4, 6, 7, 8],  # First American — 8/10
    "60003": [2, 3, 4, 5, 6, 7],         # Old Republic — 6/10
    "60004": [0, 1, 2, 3, 4, 5, 6, 7],  # Stewart — 8/10
    "60005": [0, 2, 3, 5, 7, 8, 9],     # WFG — 7/10
    "60006": [2, 3, 4, 5, 6, 9],         # Investors — 6/10
    "60007": [0, 1, 3, 4, 6, 7, 8, 9],  # Westcor — 8/10
    "60008": [2, 3, 5, 6, 7, 8],         # North American — 6/10
}

# Per-carrier reissue credit adjustment (additive, applied to base credit_pct)
NY_CARRIER_REISSUE_ADJ: dict[str, float] = {
    "60001": 2.0,   # Fidelity — generous
    "60002": 0.0,   # First American — base
    "60003": -2.0,  # Old Republic — tighter
    "60004": 0.0,   # Stewart — base
    "60005": 3.0,   # WFG — most generous
    "60006": -3.0,  # Investors — tightest
    "60007": -1.0,  # Westcor
    "60008": 1.0,   # North American
}


# ── CA Filed Rates (California Dept of Insurance) ─────────────────────
# Effective July 1, 2023
# CA is a moderate market — each carrier files independently with CDI.

CA_BASE_OWNER_RATES: list[dict[str, Any]] = [
    {"coverage_min": 0,          "coverage_max": 100_000,    "rate_per_thousand": 5.50, "flat_fee": 0, "minimum_premium": 200},
    {"coverage_min": 100_000,    "coverage_max": 200_000,    "rate_per_thousand": 4.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 200_000,    "coverage_max": 500_000,    "rate_per_thousand": 4.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 500_000,    "coverage_max": 1_000_000,  "rate_per_thousand": 3.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 1_000_000,  "coverage_max": 5_000_000,  "rate_per_thousand": 2.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 5_000_000,  "coverage_max": 15_000_000, "rate_per_thousand": 2.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 15_000_000, "coverage_max": 25_000_000, "rate_per_thousand": 1.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 25_000_000, "coverage_max": 99_999_999, "rate_per_thousand": 1.50, "flat_fee": 0, "minimum_premium": 0},
]

CA_BASE_LENDER_RATES: list[dict[str, Any]] = [
    {"coverage_min": 0,          "coverage_max": 100_000,    "rate_per_thousand": 4.75, "flat_fee": 0, "minimum_premium": 175},
    {"coverage_min": 100_000,    "coverage_max": 200_000,    "rate_per_thousand": 4.00, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 200_000,    "coverage_max": 500_000,    "rate_per_thousand": 3.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 500_000,    "coverage_max": 1_000_000,  "rate_per_thousand": 2.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 1_000_000,  "coverage_max": 5_000_000,  "rate_per_thousand": 2.25, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 5_000_000,  "coverage_max": 15_000_000, "rate_per_thousand": 1.75, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 15_000_000, "coverage_max": 25_000_000, "rate_per_thousand": 1.50, "flat_fee": 0, "minimum_premium": 0},
    {"coverage_min": 25_000_000, "coverage_max": 99_999_999, "rate_per_thousand": 1.25, "flat_fee": 0, "minimum_premium": 0},
]

CA_BASE_SIMULTANEOUS: list[dict[str, Any]] = [
    {"loan_min": 0,          "loan_max": 100_000,    "discount_rate_per_thousand": 4.25, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 100_000,    "loan_max": 200_000,    "discount_rate_per_thousand": 3.50, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 200_000,    "loan_max": 500_000,    "discount_rate_per_thousand": 3.00, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 500_000,    "loan_max": 1_000_000,  "discount_rate_per_thousand": 2.25, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 1_000_000,  "loan_max": 5_000_000,  "discount_rate_per_thousand": 1.75, "discount_pct": 0, "flat_fee": 0},
    {"loan_min": 5_000_000,  "loan_max": 99_999_999, "discount_rate_per_thousand": 1.25, "discount_pct": 0, "flat_fee": 0},
]

CA_BASE_REISSUE: list[dict[str, Any]] = [
    {"years_since_min": 0, "years_since_max": 1, "credit_pct": 40.0},
    {"years_since_min": 1, "years_since_max": 2, "credit_pct": 30.0},
    {"years_since_min": 2, "years_since_max": 4, "credit_pct": 20.0},
    {"years_since_min": 4, "years_since_max": 5, "credit_pct": 15.0},
    {"years_since_min": 5, "years_since_max": 8, "credit_pct": 10.0},
]

CA_ENDORSEMENT_POOL: list[dict[str, Any]] = [
    {"endorsement_code": "ALTA 5",    "endorsement_name": "Planned Unit Development",                   "base_fee": 25},
    {"endorsement_code": "ALTA 6",    "endorsement_name": "Variable Rate Mortgage",                     "base_fee": 20},
    {"endorsement_code": "ALTA 8.1",  "endorsement_name": "Environmental Protection Lien",              "base_fee": 25},
    {"endorsement_code": "ALTA 9",    "endorsement_name": "Restrictions, Encroachments, Minerals",      "base_fee": 30},
    {"endorsement_code": "ALTA 17",   "endorsement_name": "Access and Entry",                           "base_fee": 50},
    {"endorsement_code": "ALTA 18",   "endorsement_name": "Single Tax Parcel",                          "base_fee": 25},
    {"endorsement_code": "ALTA 22",   "endorsement_name": "Location",                                   "base_fee": 35},
    {"endorsement_code": "ALTA 25",   "endorsement_name": "Same as Survey",                             "base_fee": 40},
    {"endorsement_code": "ALTA 28",   "endorsement_name": "Easement — Damage or Enforced Removal",      "base_fee": 50},
    {"endorsement_code": "CLTA 100",  "endorsement_name": "Modified ALTA Homeowner (CA-specific)",      "base_fee": 75},
]

CA_CARRIER_MULTIPLIERS: dict[str, float] = {
    "60001": 0.97,  # Fidelity National
    "60002": 0.94,  # First American
    "60003": 1.03,  # Old Republic
    "60004": 1.01,  # Stewart Title
    "60005": 0.91,  # WFG National
    "60006": 1.06,  # Investors Title
    "60007": 1.10,  # Westcor
    "60008": 0.96,  # North American (Doma)
}

CA_CARRIER_ENDORSEMENT_INDICES: dict[str, list[int]] = {
    "60001": [0, 2, 3, 4, 5, 7, 8, 9],  # Fidelity — 8/10
    "60002": [0, 1, 2, 3, 4, 5, 6, 9],  # First American — 8/10
    "60003": [2, 3, 4, 6, 7, 8],         # Old Republic — 6/10
    "60004": [0, 1, 3, 4, 5, 6, 7, 9],  # Stewart — 8/10
    "60005": [0, 2, 3, 4, 7, 8, 9],     # WFG — 7/10
    "60006": [2, 3, 5, 6, 8, 9],         # Investors — 6/10
    "60007": [0, 1, 2, 3, 4, 6, 7, 8],  # Westcor — 8/10
    "60008": [1, 3, 4, 5, 7, 9],         # North American — 6/10
}

CA_CARRIER_REISSUE_ADJ: dict[str, float] = {
    "60001": 1.0,   # Fidelity
    "60002": 3.0,   # First American — generous
    "60003": -2.0,  # Old Republic — tighter
    "60004": 0.0,   # Stewart — base
    "60005": 4.0,   # WFG — most generous
    "60006": -3.0,  # Investors — tightest
    "60007": -2.0,  # Westcor
    "60008": 2.0,   # North American
}


# ── Helper functions ──────────────────────────────────────────────────


def _apply_rate_multiplier(
    rates: list[dict[str, Any]], multiplier: float,
) -> list[dict[str, Any]]:
    """Apply a carrier-specific multiplier to base rate tiers."""
    return [
        {
            **r,
            "rate_per_thousand": round(r["rate_per_thousand"] * multiplier, 2),
            "minimum_premium": round(r.get("minimum_premium", 0) * multiplier),
        }
        for r in rates
    ]


def _apply_simultaneous_multiplier(
    simultaneous: list[dict[str, Any]], multiplier: float,
) -> list[dict[str, Any]]:
    """Apply a carrier-specific multiplier to simultaneous issue discount tiers."""
    return [
        {
            **s,
            "discount_rate_per_thousand": round(
                s["discount_rate_per_thousand"] * multiplier, 2
            ),
        }
        for s in simultaneous
    ]


def _apply_reissue_adjustment(
    reissue: list[dict[str, Any]], adj: float,
) -> list[dict[str, Any]]:
    """Apply an additive adjustment to reissue credit percentages.

    Clamps resulting credit_pct to [5.0, 60.0].
    """
    return [
        {
            **rc,
            "credit_pct": round(max(5.0, min(60.0, rc["credit_pct"] + adj)), 1),
        }
        for rc in reissue
    ]


def _build_carrier_endorsements(
    pool: list[dict[str, Any]],
    indices: list[int],
    multiplier: float,
) -> list[dict[str, Any]]:
    """Build a carrier-specific endorsement list from a state pool."""
    return [
        {
            "endorsement_code": pool[i]["endorsement_code"],
            "endorsement_name": pool[i]["endorsement_name"],
            "flat_fee": round(pool[i]["base_fee"] * multiplier),
            "rate_per_thousand": 0,
            "pct_of_base": 0,
        }
        for i in indices
    ]


# ── FL Load Function ─────────────────────────────────────────────────


async def load_fl_promulgated_rates(
    effective_date: date | None = None,
) -> list[dict[str, Any]]:
    """Load FL promulgated title rates for all carriers.

    FL rates are set by the Office of Insurance Regulation and apply
    uniformly to all title insurers operating in Florida.

    Returns
    -------
    List of load results, one per carrier x policy_type.
    """
    from hermes.title.rate_card_loader import TitleRateCardLoader

    if effective_date is None:
        effective_date = date(2023, 7, 1)

    loader = TitleRateCardLoader()

    logger.info("Loading FL promulgated title rates (effective %s)", effective_date)

    results = await loader.load_promulgated_rates(
        state="FL",
        effective_date=effective_date,
        rates=FL_OWNER_RATES,
        simultaneous=FL_SIMULTANEOUS_ISSUE,
        reissue_credits=FL_REISSUE_CREDITS,
        endorsements=FL_ENDORSEMENTS,
        source="fl_oir",
        notes="FL promulgated rates from Florida OIR",
    )

    # Load lender-specific rate cards with correct lender rates
    for naic in ["60001", "60002", "60003", "60004", "60005", "60006", "60007", "60008"]:
        try:
            await loader.load_rate_card(
                carrier_naic=naic,
                policy_type="lender",
                state="FL",
                effective_date=effective_date,
                rates=FL_LENDER_RATES,
                is_promulgated=True,
                source="fl_oir",
                notes="FL promulgated lender rates from Florida OIR",
            )
        except Exception as exc:
            logger.error("Failed to load FL lender rates for %s: %s", naic, exc)

    logger.info(
        "FL promulgated rates loaded: %d rate cards created",
        len(results),
    )
    return results


# ── NM Load Function ─────────────────────────────────────────────────


async def load_nm_promulgated_rates(
    effective_date: date | None = None,
) -> list[dict[str, Any]]:
    """Load NM promulgated title rates for all carriers.

    NM rates are set by the Office of Superintendent of Insurance and
    apply uniformly to all title insurers operating in New Mexico.

    Returns
    -------
    List of load results, one per carrier x policy_type.
    """
    from hermes.title.rate_card_loader import TitleRateCardLoader

    if effective_date is None:
        effective_date = date(2022, 1, 1)

    loader = TitleRateCardLoader()

    logger.info("Loading NM promulgated title rates (effective %s)", effective_date)

    results = await loader.load_promulgated_rates(
        state="NM",
        effective_date=effective_date,
        rates=NM_OWNER_RATES,
        simultaneous=NM_SIMULTANEOUS_ISSUE,
        reissue_credits=NM_REISSUE_CREDITS,
        endorsements=NM_ENDORSEMENTS,
        source="nm_osi",
        notes="NM promulgated rates from NM Office of Superintendent of Insurance",
    )

    # Load lender-specific rate cards with correct lender rates
    for naic in ["60001", "60002", "60003", "60004", "60005", "60006", "60007", "60008"]:
        try:
            await loader.load_rate_card(
                carrier_naic=naic,
                policy_type="lender",
                state="NM",
                effective_date=effective_date,
                rates=NM_LENDER_RATES,
                is_promulgated=True,
                source="nm_osi",
                notes="NM promulgated lender rates from NM OSI",
            )
        except Exception as exc:
            logger.error("Failed to load NM lender rates for %s: %s", naic, exc)

    logger.info(
        "NM promulgated rates loaded: %d rate cards created",
        len(results),
    )
    return results


# ── NY Load Function ─────────────────────────────────────────────────


async def load_ny_filed_rates(
    effective_date: date | None = None,
) -> list[dict[str, Any]]:
    """Load NY filed title rates — per-carrier rate schedules.

    NY is a filed state: each carrier files its own rate schedule with the
    Department of Financial Services.  Per-carrier multipliers create real
    price differentiation in the matching results.

    Returns
    -------
    List of load results, one per carrier x policy_type.
    """
    from hermes.title.rate_card_loader import TitleRateCardLoader

    if effective_date is None:
        effective_date = date(2024, 1, 1)

    loader = TitleRateCardLoader()

    logger.info("Loading NY filed title rates (effective %s)", effective_date)

    results: list[dict[str, Any]] = []

    for naic, multiplier in NY_CARRIER_MULTIPLIERS.items():
        owner_rates = _apply_rate_multiplier(NY_BASE_OWNER_RATES, multiplier)
        lender_rates = _apply_rate_multiplier(NY_BASE_LENDER_RATES, multiplier)
        simultaneous = _apply_simultaneous_multiplier(NY_BASE_SIMULTANEOUS, multiplier)
        reissue = _apply_reissue_adjustment(
            NY_BASE_REISSUE, NY_CARRIER_REISSUE_ADJ[naic]
        )
        endorsements = _build_carrier_endorsements(
            NY_ENDORSEMENT_POOL, NY_CARRIER_ENDORSEMENT_INDICES[naic], multiplier
        )

        # Owner card — with simultaneous, reissue, endorsements
        r = await loader.load_rate_card(
            carrier_naic=naic,
            policy_type="owner",
            state="NY",
            effective_date=effective_date,
            rates=owner_rates,
            simultaneous=simultaneous,
            reissue_credits=reissue,
            endorsements=endorsements,
            is_promulgated=False,
            source="ny_dfs",
            notes=f"NY filed owner rates for carrier {naic} (×{multiplier})",
        )
        results.append(r)

        # Lender card — rates only
        r = await loader.load_rate_card(
            carrier_naic=naic,
            policy_type="lender",
            state="NY",
            effective_date=effective_date,
            rates=lender_rates,
            is_promulgated=False,
            source="ny_dfs",
            notes=f"NY filed lender rates for carrier {naic} (×{multiplier})",
        )
        results.append(r)

    logger.info(
        "NY filed rates loaded: %d rate cards created across %d carriers",
        len(results), len(NY_CARRIER_MULTIPLIERS),
    )
    return results


# ── CA Load Function ─────────────────────────────────────────────────


async def load_ca_filed_rates(
    effective_date: date | None = None,
) -> list[dict[str, Any]]:
    """Load CA filed title rates — per-carrier rate schedules.

    CA is a filed state: each carrier files its own rate schedule with the
    California Department of Insurance (CDI).  Per-carrier multipliers create
    real price differentiation in the matching results.

    Returns
    -------
    List of load results, one per carrier x policy_type.
    """
    from hermes.title.rate_card_loader import TitleRateCardLoader

    if effective_date is None:
        effective_date = date(2023, 7, 1)

    loader = TitleRateCardLoader()

    logger.info("Loading CA filed title rates (effective %s)", effective_date)

    results: list[dict[str, Any]] = []

    for naic, multiplier in CA_CARRIER_MULTIPLIERS.items():
        owner_rates = _apply_rate_multiplier(CA_BASE_OWNER_RATES, multiplier)
        lender_rates = _apply_rate_multiplier(CA_BASE_LENDER_RATES, multiplier)
        simultaneous = _apply_simultaneous_multiplier(CA_BASE_SIMULTANEOUS, multiplier)
        reissue = _apply_reissue_adjustment(
            CA_BASE_REISSUE, CA_CARRIER_REISSUE_ADJ[naic]
        )
        endorsements = _build_carrier_endorsements(
            CA_ENDORSEMENT_POOL, CA_CARRIER_ENDORSEMENT_INDICES[naic], multiplier
        )

        # Owner card — with simultaneous, reissue, endorsements
        r = await loader.load_rate_card(
            carrier_naic=naic,
            policy_type="owner",
            state="CA",
            effective_date=effective_date,
            rates=owner_rates,
            simultaneous=simultaneous,
            reissue_credits=reissue,
            endorsements=endorsements,
            is_promulgated=False,
            source="ca_cdi",
            notes=f"CA filed owner rates for carrier {naic} (×{multiplier})",
        )
        results.append(r)

        # Lender card — rates only
        r = await loader.load_rate_card(
            carrier_naic=naic,
            policy_type="lender",
            state="CA",
            effective_date=effective_date,
            rates=lender_rates,
            is_promulgated=False,
            source="ca_cdi",
            notes=f"CA filed lender rates for carrier {naic} (×{multiplier})",
        )
        results.append(r)

    logger.info(
        "CA filed rates loaded: %d rate cards created across %d carriers",
        len(results), len(CA_CARRIER_MULTIPLIERS),
    )
    return results
