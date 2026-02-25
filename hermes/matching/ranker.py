"""Carrier ranker — sorts a list of :class:`CarrierMatchResult` by a composite
score and computes placement probability for each match.

Composite scoring weights:

- 60% — premium competitiveness (lowest premium = 100, highest = 0)
- 30% — appetite alignment (0-100 from :class:`AppetiteScorer`)
- 10% — coverage breadth (number of coverage highlights)

Placement probability is a heuristic combining eligibility status, appetite
score, and premium competitiveness.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hermes.matching.engine import CarrierMatchResult

logger = logging.getLogger("hermes.matching.ranker")

# Composite weights (must sum to 1.0)
_W_PREMIUM = 0.60
_W_APPETITE = 0.30
_W_COVERAGE = 0.10

# Minimum premium required to score competitiveness (avoids div-by-zero)
_MIN_MEANINGFUL_PREMIUM = 1.0


class CarrierRanker:
    """Ranks carrier match results by composite score.

    The ranker is stateless — call :meth:`rank_carriers` with the full list of
    matches from the matching engine and receive a sorted list back with
    ``competitiveness_rank`` and ``composite_score`` populated.
    """

    def rank_carriers(
        self, matches: list["CarrierMatchResult"]
    ) -> list["CarrierMatchResult"]:
        """Sort and rank a list of carrier matches.

        Ranking procedure:

        1. Compute premium competitiveness index: normalise all final estimated
           premiums so the lowest = 100 and the highest = 0.  Carriers with
           zero or unknown premiums receive a 50 (neutral) score.
        2. Compute composite score per carrier using the three-factor weighting.
        3. Sort descending by composite score.
        4. Assign ``competitiveness_rank`` (1 = best).
        5. Compute ``placement_probability`` for each match.

        Parameters
        ----------
        matches:
            Unordered list of :class:`CarrierMatchResult` instances.

        Returns
        -------
        list[CarrierMatchResult]
            Sorted list with ``competitiveness_rank``, ``composite_score``,
            and ``placement_probability`` populated.
        """
        if not matches:
            return []

        # Gather valid premiums for normalisation
        premiums = [
            m.premium.final_estimated
            for m in matches
            if m.premium.final_estimated >= _MIN_MEANINGFUL_PREMIUM
        ]

        min_premium = min(premiums) if premiums else None
        max_premium = max(premiums) if premiums else None

        for match in matches:
            premium_competitiveness = self._normalise_premium(
                match.premium.final_estimated, min_premium, max_premium
            )
            appetite_score = match.appetite.score  # already 0-100
            coverage_score = self._score_coverage_breadth(match.coverage_highlights)

            composite = (
                _W_PREMIUM * premium_competitiveness
                + _W_APPETITE * appetite_score
                + _W_COVERAGE * coverage_score
            )
            match.composite_score = round(composite, 4)

        # Sort descending by composite score, breaking ties alphabetically
        sorted_matches = sorted(
            matches,
            key=lambda m: (-m.composite_score, m.carrier_name),
        )

        for rank, match in enumerate(sorted_matches, start=1):
            match.competitiveness_rank = rank
            match.placement_probability = self.compute_placement_probability(match)
            logger.debug(
                "Ranked carrier=%s rank=%d composite=%.2f placement_prob=%.3f",
                match.carrier_name,
                rank,
                match.composite_score,
                match.placement_probability,
            )

        return sorted_matches

    def compute_placement_probability(
        self, match: "CarrierMatchResult"
    ) -> float:
        """Estimate the probability of successful placement with this carrier.

        The heuristic combines three signals:

        - **Eligibility weight** — ``pass`` = 1.0, ``conditional`` = 0.6,
          ``fail`` = 0.0.
        - **Appetite weight** — normalised appetite score (0-1).
        - **Competitiveness weight** — normalised premium score (0-1), where
          a lower rank (better price) increases the probability.

        The final result is a weighted product that rewards carriers who are
        eligible, motivated, and price-competitive.

        Parameters
        ----------
        match:
            The :class:`CarrierMatchResult` to score.

        Returns
        -------
        float
            Placement probability between 0.0 and 1.0.
        """
        # Eligibility factor
        status = match.eligibility.status
        if status == "pass":
            eligibility_weight = 1.0
        elif status == "conditional":
            eligibility_weight = 0.6
        else:
            eligibility_weight = 0.0

        if eligibility_weight == 0.0:
            return 0.0

        # Appetite factor (0-1)
        appetite_weight = match.appetite.score / 100.0

        # Competitiveness factor — invert rank so rank 1 = 1.0
        rank = max(1, match.competitiveness_rank)
        # Use a simple decay: prob ∝ 1/rank^0.5
        competitiveness_weight = 1.0 / (rank ** 0.5)
        # Normalise to max 1.0 (rank 1 gives exactly 1.0)
        competitiveness_weight = min(1.0, competitiveness_weight)

        probability = (
            eligibility_weight * 0.35
            + appetite_weight * 0.40
            + competitiveness_weight * 0.25
        )
        return round(min(1.0, max(0.0, probability)), 4)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_premium(
        premium: float,
        min_premium: float | None,
        max_premium: float | None,
    ) -> float:
        """Normalise a premium into a 0-100 competitiveness score.

        Lower premium → higher score (more competitive).

        Parameters
        ----------
        premium:
            The carrier's estimated premium.
        min_premium:
            The lowest premium among all carriers (gets score 100).
        max_premium:
            The highest premium among all carriers (gets score 0).

        Returns
        -------
        float
            Score between 0 and 100.
        """
        if premium < _MIN_MEANINGFUL_PREMIUM or min_premium is None or max_premium is None:
            return 50.0  # neutral

        if max_premium == min_premium:
            return 100.0  # all carriers have same premium

        # Invert: lowest premium = 100, highest = 0
        score = 100.0 * (max_premium - premium) / (max_premium - min_premium)
        return round(max(0.0, min(100.0, score)), 2)

    @staticmethod
    def _score_coverage_breadth(coverage_highlights: list[dict]) -> float:
        """Score coverage breadth on a 0-100 scale based on highlight count.

        Parameters
        ----------
        coverage_highlights:
            List of coverage highlight dicts from the carrier match.

        Returns
        -------
        float
            0-100 score; saturates at 10+ highlights.
        """
        count = len(coverage_highlights) if coverage_highlights else 0
        # Each highlight worth 10 points, capped at 100
        return min(100.0, count * 10.0)
