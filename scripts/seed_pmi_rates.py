"""Seed representative PMI rate data for all 6 carriers.

Usage:
    python -m scripts.seed_pmi_rates

Seeds monthly and single premium rate cards with realistic rate grids
and common adjustments for MGIC, Radian, Essent, Arch MI, Enact, and
National MI.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date

from hermes.pmi.rate_card_loader import PMIRateCardLoader

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
logger = logging.getLogger("seed_pmi_rates")

# ---------------------------------------------------------------------------
# Rate data — representative rates by carrier (annual % of loan amount)
# ---------------------------------------------------------------------------

# LTV bands × FICO tiers × coverage % → annual rate %
# These are illustrative rates based on publicly available PMI rate ranges.

FICO_TIERS = [
    (760, 850),
    (740, 759),
    (720, 739),
    (700, 719),
    (680, 699),
    (660, 679),
    (640, 659),
    (620, 639),
]

LTV_BANDS = [
    # (ltv_min, ltv_max, gse_coverage)
    (80.01, 85.00, 6.0),
    (85.01, 90.00, 25.0),
    (90.01, 95.00, 30.0),
    (95.01, 97.00, 35.0),
]


def _build_monthly_grid(base_matrix: list[list[float]]) -> list[dict]:
    """Build rate grid from a matrix [ltv_band_idx][fico_tier_idx] → rate_pct."""
    rates = []
    for li, (ltv_min, ltv_max, coverage) in enumerate(LTV_BANDS):
        for fi, (fico_min, fico_max) in enumerate(FICO_TIERS):
            rates.append({
                "ltv_min": ltv_min,
                "ltv_max": ltv_max,
                "fico_min": fico_min,
                "fico_max": fico_max,
                "coverage_pct": coverage,
                "rate_pct": base_matrix[li][fi],
            })
    return rates


# Carrier rate matrices: [LTV band][FICO tier] → annual rate %
# Rows: 80-85, 85-90, 90-95, 95-97
# Cols: 760+, 740-759, 720-739, 700-719, 680-699, 660-679, 640-659, 620-639

CARRIER_RATES = {
    # MGIC — competitive on high-FICO, moderate elsewhere
    "50501": {
        "monthly": [
            [0.19, 0.24, 0.30, 0.38, 0.52, 0.72, 0.96, 1.25],
            [0.30, 0.36, 0.44, 0.55, 0.74, 1.01, 1.33, 1.72],
            [0.41, 0.49, 0.58, 0.71, 0.95, 1.28, 1.68, 2.15],
            [0.55, 0.66, 0.78, 0.95, 1.24, 1.65, 2.12, 2.68],
        ],
        "single": [
            [0.52, 0.65, 0.82, 1.05, 1.42, 1.97, 2.62, 3.42],
            [0.82, 0.99, 1.20, 1.50, 2.02, 2.76, 3.64, 4.70],
            [1.12, 1.34, 1.59, 1.94, 2.60, 3.50, 4.59, 5.88],
            [1.50, 1.81, 2.13, 2.60, 3.39, 4.51, 5.80, 7.32],
        ],
    },
    # Radian — slightly higher on low-FICO, competitive mid-range
    "50502": {
        "monthly": [
            [0.20, 0.25, 0.31, 0.39, 0.53, 0.74, 0.99, 1.30],
            [0.31, 0.37, 0.45, 0.57, 0.76, 1.04, 1.37, 1.78],
            [0.42, 0.50, 0.60, 0.73, 0.98, 1.32, 1.73, 2.22],
            [0.56, 0.68, 0.80, 0.98, 1.28, 1.70, 2.18, 2.76],
        ],
        "single": [
            [0.55, 0.68, 0.85, 1.07, 1.45, 2.02, 2.71, 3.55],
            [0.85, 1.01, 1.23, 1.56, 2.08, 2.84, 3.74, 4.86],
            [1.15, 1.37, 1.64, 2.00, 2.68, 3.61, 4.73, 6.07],
            [1.53, 1.86, 2.19, 2.68, 3.50, 4.65, 5.96, 7.55],
        ],
    },
    # Essent — aggressive pricing, especially on higher LTV
    "50503": {
        "monthly": [
            [0.18, 0.23, 0.29, 0.37, 0.51, 0.70, 0.93, 1.22],
            [0.29, 0.35, 0.42, 0.53, 0.72, 0.98, 1.29, 1.67],
            [0.39, 0.47, 0.56, 0.69, 0.92, 1.24, 1.63, 2.08],
            [0.52, 0.63, 0.74, 0.91, 1.19, 1.58, 2.04, 2.58],
        ],
        "single": [
            [0.49, 0.63, 0.79, 1.01, 1.39, 1.91, 2.54, 3.34],
            [0.79, 0.96, 1.15, 1.45, 1.97, 2.68, 3.53, 4.57],
            [1.07, 1.29, 1.53, 1.89, 2.52, 3.39, 4.46, 5.69],
            [1.42, 1.72, 2.02, 2.49, 3.26, 4.32, 5.58, 7.05],
        ],
    },
    # Arch MI — strong on mid-FICO borrowers
    "50504": {
        "monthly": [
            [0.20, 0.24, 0.29, 0.36, 0.50, 0.71, 0.95, 1.26],
            [0.31, 0.36, 0.43, 0.54, 0.73, 1.00, 1.32, 1.72],
            [0.42, 0.49, 0.57, 0.70, 0.94, 1.27, 1.67, 2.15],
            [0.56, 0.66, 0.77, 0.94, 1.22, 1.63, 2.10, 2.66],
        ],
        "single": [
            [0.55, 0.66, 0.79, 0.98, 1.37, 1.94, 2.60, 3.44],
            [0.85, 0.98, 1.18, 1.48, 2.00, 2.74, 3.61, 4.70],
            [1.15, 1.34, 1.56, 1.92, 2.57, 3.47, 4.57, 5.88],
            [1.53, 1.81, 2.11, 2.57, 3.34, 4.46, 5.75, 7.27],
        ],
    },
    # Enact (formerly Genworth) — balanced across all tiers
    "50505": {
        "monthly": [
            [0.21, 0.26, 0.32, 0.40, 0.54, 0.75, 1.00, 1.30],
            [0.32, 0.38, 0.46, 0.58, 0.77, 1.05, 1.38, 1.79],
            [0.43, 0.51, 0.61, 0.74, 0.99, 1.34, 1.75, 2.24],
            [0.57, 0.69, 0.81, 0.99, 1.29, 1.72, 2.21, 2.79],
        ],
        "single": [
            [0.57, 0.71, 0.87, 1.09, 1.48, 2.05, 2.74, 3.55],
            [0.87, 1.04, 1.26, 1.59, 2.11, 2.88, 3.77, 4.89],
            [1.18, 1.40, 1.67, 2.03, 2.71, 3.66, 4.79, 6.13],
            [1.56, 1.89, 2.22, 2.71, 3.53, 4.70, 6.05, 7.63],
        ],
    },
    # National MI — competitive on clean files
    "50506": {
        "monthly": [
            [0.18, 0.23, 0.30, 0.38, 0.52, 0.73, 0.97, 1.28],
            [0.29, 0.35, 0.43, 0.55, 0.74, 1.02, 1.34, 1.74],
            [0.40, 0.48, 0.57, 0.70, 0.94, 1.27, 1.66, 2.14],
            [0.53, 0.64, 0.76, 0.93, 1.22, 1.62, 2.08, 2.64],
        ],
        "single": [
            [0.49, 0.63, 0.82, 1.04, 1.42, 2.00, 2.65, 3.50],
            [0.79, 0.96, 1.18, 1.50, 2.02, 2.79, 3.67, 4.76],
            [1.09, 1.31, 1.56, 1.92, 2.57, 3.47, 4.54, 5.85],
            [1.45, 1.75, 2.08, 2.55, 3.34, 4.43, 5.69, 7.22],
        ],
    },
}

# Common adjustments applied to all carriers
COMMON_ADJUSTMENTS = [
    {
        "name": "high_dti",
        "condition": {"dti_min": 43, "dti_max": 50},
        "adjustment_method": "additive",
        "adjustment_value": 0.15,
        "description": "High DTI surcharge (43-50%)",
    },
    {
        "name": "very_high_dti",
        "condition": {"dti_min": 50.01, "dti_max": 65},
        "adjustment_method": "additive",
        "adjustment_value": 0.30,
        "description": "Very high DTI surcharge (>50%)",
    },
    {
        "name": "investment_property",
        "condition": {"occupancy_eq": "investment"},
        "adjustment_method": "multiplicative",
        "adjustment_value": 1.25,
        "description": "Investment property surcharge (+25%)",
    },
    {
        "name": "second_home",
        "condition": {"occupancy_eq": "secondary"},
        "adjustment_method": "multiplicative",
        "adjustment_value": 1.10,
        "description": "Second home surcharge (+10%)",
    },
    {
        "name": "condo",
        "condition": {"property_type_eq": "condo"},
        "adjustment_method": "additive",
        "adjustment_value": 0.05,
        "description": "Condo property adjustment",
    },
    {
        "name": "manufactured_home",
        "condition": {"property_type_eq": "manufactured"},
        "adjustment_method": "additive",
        "adjustment_value": 0.20,
        "description": "Manufactured home surcharge",
    },
    {
        "name": "cash_out_refi",
        "condition": {"loan_purpose_eq": "cash_out_refi"},
        "adjustment_method": "additive",
        "adjustment_value": 0.10,
        "description": "Cash-out refinance surcharge",
    },
    {
        "name": "multi_unit",
        "condition": {"property_type_in": ["2_unit", "3_4_unit"]},
        "adjustment_method": "additive",
        "adjustment_value": 0.10,
        "description": "Multi-unit property surcharge",
    },
]


async def seed_all() -> None:
    """Seed rate cards for all 6 PMI carriers."""
    loader = PMIRateCardLoader()
    effective = date(2025, 1, 1)
    total_cards = 0
    total_rates = 0

    for naic, rate_data in CARRIER_RATES.items():
        for ptype, matrix in rate_data.items():
            grid = _build_monthly_grid(matrix)
            result = await loader.load_rate_card(
                carrier_naic=naic,
                premium_type=ptype,
                effective_date=effective,
                rates=grid,
                adjustments=COMMON_ADJUSTMENTS,
                source="seed",
                notes="Representative rates seeded for development/testing",
            )
            total_cards += 1
            total_rates += result["rates_inserted"]
            logger.info(
                "  %s %s: %d rates, %d adjustments",
                naic, ptype, result["rates_inserted"], result["adjustments_inserted"],
            )

    logger.info(
        "Seeding complete: %d rate cards, %d total rates across %d carriers",
        total_cards, total_rates, len(CARRIER_RATES),
    )


if __name__ == "__main__":
    asyncio.run(seed_all())
