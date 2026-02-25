"""Hermes matching package — carrier-risk matching engine.

Exports the public API for carrier-risk matching:

- :class:`MatchingEngine` — orchestrates the full matching pipeline
- :class:`EligibilityFilter` — checks risk profiles against carrier eligibility criteria
- :class:`AppetiteScorer` — scores carrier appetite for a given risk
- :class:`PremiumEstimator` — estimates premiums using rate tables and rating factors
- :class:`CarrierRanker` — ranks and sorts carrier matches by composite score
"""

from hermes.matching.eligibility import EligibilityFilter
from hermes.matching.appetite import AppetiteScorer
from hermes.matching.premium import PremiumEstimator
from hermes.matching.ranker import CarrierRanker
from hermes.matching.engine import MatchingEngine

__all__ = [
    "MatchingEngine",
    "EligibilityFilter",
    "AppetiteScorer",
    "PremiumEstimator",
    "CarrierRanker",
]
