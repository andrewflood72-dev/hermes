"""Hermes Validation Layer — post-extraction quality checks and confidence scoring.

Provides:
  - ``RateValidator``: cross-validates extracted rates against filing metadata
  - ``ConfidenceScorer``: scores individual table, rule, and form extractions
"""

from hermes.validation.rate_validator import RateValidator, ValidationResult
from hermes.validation.confidence import ConfidenceScorer

__all__ = [
    "ConfidenceScorer",
    "RateValidator",
    "ValidationResult",
]
