"""Build carrier appetite profiles from parsed filing data.

Usage:
    python -m hermes.scripts.build_appetite
    python -m hermes.scripts.build_appetite --state NY
    python -m hermes.scripts.build_appetite --dry-run

For each carrier+state+line combination with parsed data, computes:
- Appetite score (0-10) based on filing patterns and rate changes
- Eligible/ineligible/preferred class lists from eligibility criteria
- Territory preferences from territory definitions
- Limit and deductible ranges from coverage options
- Rate competitiveness index from base rates
- Filing frequency score from recent activity

Updates hermes_appetite_profiles and hermes_appetite_signals.
"""

import argparse
import asyncio
import logging
import time
import uuid
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import create_async_engine

from hermes.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hermes.build_appetite")


async def get_engine():
    return create_async_engine(settings.database_url, echo=False, pool_size=5)


async def get_carrier_state_lines(engine, state: str | None = None) -> list[dict]:
    """Get distinct carrier+state+line combos that have parsed data."""
    conditions = ["f.filed_date IS NOT NULL"]
    params: dict = {}
    if state:
        conditions.append("f.state = :state")
        params["state"] = state

    where = " AND ".join(conditions)

    query = f"""
        SELECT DISTINCT
            f.carrier_id,
            f.carrier_name_filed,
            f.state,
            f.line_of_business as line,
            COUNT(*) as filing_count,
            MAX(f.filed_date) as latest_filing,
            MIN(f.filed_date) as earliest_filing,
            AVG(f.overall_rate_change_pct) as avg_rate_change,
            COUNT(*) FILTER (WHERE f.overall_rate_change_pct IS NOT NULL) as rate_filings
        FROM hermes_filings f
        WHERE {where}
        AND f.carrier_id IS NOT NULL
        GROUP BY f.carrier_id, f.carrier_name_filed, f.state, f.line_of_business
        HAVING COUNT(*) >= 1
        ORDER BY f.state, f.line_of_business, COUNT(*) DESC
    """

    async with engine.connect() as conn:
        result = await conn.execute(sa_text(query), params)
        return [dict(row._mapping) for row in result.fetchall()]


async def get_eligibility_for_carrier(engine, carrier_id: str, state: str, line: str) -> dict:
    """Get eligibility criteria for a carrier+state+line."""
    query = """
        SELECT
            ec.criterion_type,
            ec.criterion_value,
            ec.criterion_operator,
            ec.is_hard_rule,
            ec.description,
            ec.confidence
        FROM hermes_eligibility_criteria ec
        JOIN hermes_underwriting_rules ur ON ec.rule_id = ur.id
        WHERE ur.carrier_id = CAST(:carrier_id AS uuid)
        AND ur.state = :state
        AND ur.line = :line
        AND ur.is_current = TRUE
    """
    async with engine.connect() as conn:
        result = await conn.execute(
            sa_text(query),
            {"carrier_id": carrier_id, "state": state, "line": line},
        )
        rows = result.fetchall()

    eligible = []
    ineligible = []
    preferred = []

    for row in rows:
        ct = row.criterion_type
        cv = row.criterion_value
        if ct == "eligible_class":
            eligible.append(cv)
        elif ct == "ineligible_class":
            ineligible.append(cv)
        elif ct == "preferred_class":
            preferred.append(cv)

    return {
        "eligible_classes": eligible,
        "ineligible_classes": ineligible,
        "preferred_classes": preferred,
        "criteria_count": len(rows),
    }


async def get_coverage_ranges(engine, carrier_id: str, state: str, line: str) -> dict:
    """Get coverage option ranges for a carrier+state+line."""
    query = """
        SELECT
            MIN(limit_min) as min_limit,
            MAX(limit_max) as max_limit,
            MIN(default_deductible) as min_deductible,
            MAX(default_deductible) as max_deductible
        FROM hermes_coverage_options
        WHERE carrier_id = CAST(:carrier_id AS uuid)
        AND state = :state
        AND line = :line
        AND is_current = TRUE
    """
    async with engine.connect() as conn:
        result = await conn.execute(
            sa_text(query),
            {"carrier_id": carrier_id, "state": state, "line": line},
        )
        row = result.fetchone()

    if not row:
        return {}

    return {
        "limit_range_min": float(row.min_limit) if row.min_limit else None,
        "limit_range_max": float(row.max_limit) if row.max_limit else None,
        "typical_deductible_min": float(row.min_deductible) if row.min_deductible else None,
        "typical_deductible_max": float(row.max_deductible) if row.max_deductible else None,
    }


def compute_appetite_score(combo: dict, eligibility: dict, coverage: dict) -> float:
    """Compute appetite score (0-10) from filing patterns.

    Scoring factors:
    - Filing frequency (more filings = more active = higher appetite)
    - Recency of filings (recent filings = current appetite)
    - Rate changes (moderate increases = healthy, large increases = pulling back)
    - Eligibility breadth (more eligible classes = broader appetite)
    """
    score = 5.0  # Start at neutral

    # Filing frequency: 1 filing = +0, 5+ = +1.5
    count = combo["filing_count"]
    score += min(count / 5.0, 1.5)

    # Recency: filed in last 6 months = +1, last year = +0.5
    latest = combo.get("latest_filing")
    if latest:
        if isinstance(latest, str):
            from datetime import datetime
            latest = datetime.strptime(latest[:10], "%Y-%m-%d").date()
        days_ago = (date.today() - latest).days
        if days_ago < 180:
            score += 1.0
        elif days_ago < 365:
            score += 0.5

    # Rate changes: moderate = stable appetite, large = concerning
    avg_rate = combo.get("avg_rate_change")
    if avg_rate is not None:
        avg_rate = float(avg_rate)
        if -5 <= avg_rate <= 5:
            score += 0.5  # Stable
        elif avg_rate > 15:
            score -= 1.0  # Large increase = pulling back
        elif avg_rate < -10:
            score -= 0.5  # Large decrease = may be aggressive but risky

    # Eligibility breadth
    eligible_count = len(eligibility.get("eligible_classes", []))
    ineligible_count = len(eligibility.get("ineligible_classes", []))
    if eligible_count > 5:
        score += 0.5
    if ineligible_count > eligible_count:
        score -= 0.5

    return max(0.0, min(10.0, round(score, 2)))


async def build_profile(engine, combo: dict, dry_run: bool = False) -> dict | None:
    """Build or update an appetite profile for a carrier+state+line."""
    carrier_id = str(combo["carrier_id"])
    state = combo["state"]
    line = combo["line"]

    eligibility = await get_eligibility_for_carrier(engine, carrier_id, state, line)
    coverage = await get_coverage_ranges(engine, carrier_id, state, line)
    appetite_score = compute_appetite_score(combo, eligibility, coverage)

    profile = {
        "carrier_id": carrier_id,
        "state": state,
        "line": line,
        "appetite_score": appetite_score,
        "eligible_classes": eligibility.get("eligible_classes", []),
        "ineligible_classes": eligibility.get("ineligible_classes", []),
        "preferred_classes": eligibility.get("preferred_classes", []),
        "limit_range_min": coverage.get("limit_range_min"),
        "limit_range_max": coverage.get("limit_range_max"),
        "typical_deductible_min": coverage.get("typical_deductible_min"),
        "typical_deductible_max": coverage.get("typical_deductible_max"),
        "last_rate_change_pct": float(combo["avg_rate_change"]) if combo.get("avg_rate_change") else None,
        "last_rate_change_date": str(combo["latest_filing"]) if combo.get("latest_filing") else None,
        "source_filing_count": combo["filing_count"],
    }

    if dry_run:
        return profile

    # Upsert profile
    import json
    async with engine.begin() as conn:
        # Mark old profiles as not current
        await conn.execute(
            sa_text("""
                UPDATE hermes_appetite_profiles
                SET is_current = FALSE, updated_at = NOW()
                WHERE carrier_id = CAST(:carrier_id AS uuid)
                AND state = :state AND line = :line
                AND is_current = TRUE
            """),
            {"carrier_id": carrier_id, "state": state, "line": line},
        )

        # Insert new profile
        profile_id = str(uuid.uuid4())
        await conn.execute(
            sa_text("""
                INSERT INTO hermes_appetite_profiles (
                    id, carrier_id, state, line, appetite_score,
                    eligible_classes, ineligible_classes, preferred_classes,
                    limit_range_min, limit_range_max,
                    typical_deductible_min, typical_deductible_max,
                    last_rate_change_pct, last_rate_change_date,
                    source_filing_count, is_current
                ) VALUES (
                    CAST(:id AS uuid), CAST(:carrier_id AS uuid),
                    :state, :line, :score,
                    CAST(:eligible AS jsonb), CAST(:ineligible AS jsonb),
                    CAST(:preferred AS jsonb),
                    :limit_min, :limit_max,
                    :ded_min, :ded_max,
                    :rate_change, CAST(:rate_date AS date),
                    :filing_count, TRUE
                )
            """),
            {
                "id": profile_id,
                "carrier_id": carrier_id,
                "state": state,
                "line": line,
                "score": appetite_score,
                "eligible": json.dumps(profile["eligible_classes"]),
                "ineligible": json.dumps(profile["ineligible_classes"]),
                "preferred": json.dumps(profile["preferred_classes"]),
                "limit_min": profile["limit_range_min"],
                "limit_max": profile["limit_range_max"],
                "ded_min": profile["typical_deductible_min"],
                "ded_max": profile["typical_deductible_max"],
                "rate_change": profile["last_rate_change_pct"],
                "rate_date": profile["last_rate_change_date"],
                "filing_count": profile["source_filing_count"],
            },
        )

    profile["profile_id"] = profile_id
    return profile


async def main():
    parser = argparse.ArgumentParser(
        description="Build carrier appetite profiles from parsed filing data"
    )
    parser.add_argument("--state", type=str, help="Filter by state")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute profiles without saving to DB")
    args = parser.parse_args()

    engine = await get_engine()
    start = time.monotonic()

    combos = await get_carrier_state_lines(engine, state=args.state)
    logger.info("Found %d carrier+state+line combinations", len(combos))

    if not combos:
        logger.info("No data to build profiles from")
        return

    built = 0
    for combo in combos:
        try:
            profile = await build_profile(engine, combo, dry_run=args.dry_run)
            if profile:
                built += 1
                logger.info(
                    "%s | %s | %s | score=%.1f | filings=%d%s",
                    combo["carrier_name_filed"][:40] if combo["carrier_name_filed"] else "?",
                    combo["state"],
                    combo["line"][:30],
                    profile["appetite_score"],
                    profile["source_filing_count"],
                    " (dry-run)" if args.dry_run else "",
                )
        except Exception as exc:
            logger.error(
                "Failed for %s/%s/%s: %s",
                combo.get("carrier_name_filed", "?")[:30],
                combo["state"], combo["line"][:20], exc,
            )

    elapsed = round(time.monotonic() - start, 1)
    logger.info("=" * 60)
    logger.info("Built %d appetite profiles in %.1fs", built, elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
