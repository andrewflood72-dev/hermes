"""Rate Validator — post-extraction consistency and cross-validation checks.

Compares extracted rate data against the stated rate change percentages from
the filing's actuarial memo and checks for internal anomalies (negative rates,
extreme outliers, duplicate class codes).
"""

from __future__ import annotations

import logging
import statistics
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ValidationResult(BaseModel):
    """Outcome of a rate validation run."""

    is_valid: bool = True
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    cross_validation_notes: list[str] = Field(default_factory=list)
    anomalies_found: int = 0
    duplicate_class_codes: list[str] = Field(default_factory=list)


class RateValidator:
    """Validates extracted rate data for internal consistency and filing accuracy.

    Two primary entry points:
      - ``validate_rate_table``: cross-validates the extracted rate set against
        the stated rate change percentages from the actuarial memo.
      - ``check_rate_consistency``: scans for anomalies within the rate set
        itself (negatives, extreme values, duplicates).
    """

    # ── Thresholds ─────────────────────────────────────────────────────────

    #: Maximum acceptable rate value for any single class code.
    MAX_REASONABLE_RATE = 100_000.0

    #: Minimum acceptable rate value (rates should be positive).
    MIN_REASONABLE_RATE = 0.0001

    #: Relative tolerance when comparing computed vs stated rate change.
    RATE_CHANGE_TOLERANCE = 0.05  # 5 percentage points

    def validate_rate_table(
        self,
        rates: list[dict[str, Any]],
        actuarial_memo: dict[str, Any],
    ) -> ValidationResult:
        """Cross-validate extracted rates against stated rate change percentages.

        Compares the arithmetic mean of extracted base rates against the prior
        rates implied by the stated overall_rate_change_pct from the filing
        metadata / actuarial memo.

        Args:
            rates: List of base rate record dicts (from hermes_base_rates).
                   Each dict must have a ``base_rate`` key with a numeric value.
            actuarial_memo: Dict containing at least ``overall_rate_change_pct``
                            (as a percentage, e.g. 4.5 for 4.5%).

        Returns:
            A ``ValidationResult`` describing findings.
        """
        result = ValidationResult()

        # ── Internal consistency check first ──────────────────────────────
        anomaly_warnings = self.check_rate_consistency(rates)
        result.anomalies_found = len(anomaly_warnings)
        for w in anomaly_warnings:
            result.warnings.append(w)

        # ── Cross-validation against stated rate change ────────────────────
        stated_change_pct = actuarial_memo.get("overall_rate_change_pct")
        prior_rates = actuarial_memo.get("prior_rates", [])

        if stated_change_pct is None:
            result.cross_validation_notes.append(
                "overall_rate_change_pct not provided in actuarial memo; "
                "cross-validation skipped."
            )
            return result

        if not prior_rates:
            result.cross_validation_notes.append(
                "No prior rate data provided; cross-validation against stated "
                "rate change is unavailable."
            )
            return result

        current_values = [
            float(r["base_rate"])
            for r in rates
            if r.get("base_rate") is not None
        ]
        prior_values = [
            float(r["base_rate"])
            for r in prior_rates
            if r.get("base_rate") is not None
        ]

        if not current_values or not prior_values:
            result.warnings.append(
                "Insufficient numeric rate data for cross-validation."
            )
            return result

        avg_current = statistics.mean(current_values)
        avg_prior = statistics.mean(prior_values)

        if avg_prior == 0:
            result.warnings.append(
                "Prior rates average to zero; cross-validation skipped."
            )
            return result

        computed_change_pct = ((avg_current - avg_prior) / avg_prior) * 100.0
        stated_f = float(stated_change_pct)
        delta = abs(computed_change_pct - stated_f)

        note = (
            f"Computed avg rate change: {computed_change_pct:+.2f}% | "
            f"Stated: {stated_f:+.2f}% | Delta: {delta:.2f}pp"
        )
        result.cross_validation_notes.append(note)
        logger.info("Rate cross-validation: %s", note)

        if delta > self.RATE_CHANGE_TOLERANCE * 100:
            result.is_valid = False
            result.errors.append(
                f"Rate change discrepancy: extracted data implies "
                f"{computed_change_pct:+.2f}% change but filing states "
                f"{stated_f:+.2f}%. Tolerance is "
                f"{self.RATE_CHANGE_TOLERANCE * 100:.0f}pp."
            )

        return result

    def check_rate_consistency(
        self, rates: list[dict[str, Any]]
    ) -> list[str]:
        """Scan extracted rates for internal anomalies.

        Checks performed:
          - Negative rate values.
          - Rates above ``MAX_REASONABLE_RATE``.
          - Rates at or below zero.
          - Duplicate class codes within the same territory.
          - Rates that are statistical outliers (> 3 standard deviations from mean).

        Args:
            rates: List of base rate record dicts.

        Returns:
            List of warning strings describing each anomaly found.  Empty list
            means no anomalies detected.
        """
        warnings: list[str] = []

        if not rates:
            warnings.append("No rates provided for consistency check.")
            return warnings

        numeric_rates: list[float] = []
        seen_class_territory: dict[str, int] = {}

        for rec in rates:
            raw_rate = rec.get("base_rate")
            if raw_rate is None:
                continue
            try:
                rate_val = float(raw_rate)
            except (TypeError, ValueError):
                warnings.append(
                    f"Non-numeric base_rate value: {raw_rate!r} "
                    f"(class_code={rec.get('class_code')})"
                )
                continue

            # Negative / zero rates.
            if rate_val <= 0:
                warnings.append(
                    f"Non-positive base_rate={rate_val} for "
                    f"class_code={rec.get('class_code')!r}"
                )

            # Extreme values.
            elif rate_val > self.MAX_REASONABLE_RATE:
                warnings.append(
                    f"Suspiciously large base_rate={rate_val} for "
                    f"class_code={rec.get('class_code')!r} "
                    f"(threshold={self.MAX_REASONABLE_RATE})"
                )
            else:
                numeric_rates.append(rate_val)

            # Duplicate class code + territory combos.
            key = f"{rec.get('class_code')}|{rec.get('territory', '')}"
            seen_class_territory[key] = seen_class_territory.get(key, 0) + 1

        # Report duplicates.
        duplicates = [k for k, count in seen_class_territory.items() if count > 1]
        for dup in duplicates:
            warnings.append(
                f"Duplicate class_code+territory combination: {dup!r} "
                f"appears {seen_class_territory[dup]} times"
            )

        # Statistical outlier detection.
        if len(numeric_rates) >= 5:
            mean = statistics.mean(numeric_rates)
            stdev = statistics.stdev(numeric_rates)
            if stdev > 0:
                for rate_val in numeric_rates:
                    z = abs(rate_val - mean) / stdev
                    if z > 3.0:
                        warnings.append(
                            f"Statistical outlier detected: base_rate={rate_val:.4f} "
                            f"is {z:.1f} standard deviations from mean={mean:.4f}"
                        )

        if not warnings:
            logger.debug(
                "Rate consistency check passed for %d rate records.", len(rates)
            )
        else:
            logger.info(
                "Rate consistency check found %d issue(s) in %d records.",
                len(warnings),
                len(rates),
            )

        return warnings
