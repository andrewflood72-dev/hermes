"""MGADataCollector — assembles the data package for proposal generation.

Pulls live PMI rate data from the database and merges it with embedded
market intelligence constants to create the full context that Claude uses
to generate each proposal section.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hermes.pmi.engine import HermesPMIEngine
from hermes.pmi.schemas import PMIQuoteRequest

logger = logging.getLogger(__name__)


class MGADataCollector:
    """Collects live DB data and market intelligence for MGA proposals."""

    def __init__(
        self,
        db_engine: AsyncEngine,
        pmi_engine: HermesPMIEngine,
        title_engine: Any = None,
    ) -> None:
        self._engine = db_engine
        self._pmi = pmi_engine
        self._title = title_engine

    # ------------------------------------------------------------------
    # Live DB queries
    # ------------------------------------------------------------------

    async def collect_carrier_profiles(self) -> list[dict[str, Any]]:
        """Query all 6 PMI carriers with rate card counts."""
        query = text("""
            SELECT
                c.id, c.naic_code, c.legal_name, c.am_best_rating,
                COUNT(rc.id) AS rate_card_count
            FROM hermes_carriers c
            LEFT JOIN hermes_pmi_rate_cards rc
                ON rc.carrier_id = c.id AND rc.is_current = TRUE
            WHERE c.naic_code BETWEEN '50501' AND '50506'
              AND c.status = 'active'
            GROUP BY c.id, c.naic_code, c.legal_name, c.am_best_rating
            ORDER BY c.naic_code
        """)
        async with self._engine.connect() as conn:
            rows = (await conn.execute(query)).mappings().all()
        return [
            {
                "id": str(r["id"]),
                "naic_code": r["naic_code"],
                "legal_name": r["legal_name"],
                "am_best_rating": r["am_best_rating"],
                "rate_card_count": int(r["rate_card_count"]),
            }
            for r in rows
        ]

    async def collect_rate_analysis(self) -> dict[str, Any]:
        """Aggregate rate distribution across carriers: min/max/avg per LTV band."""
        query = text("""
            SELECT
                c.legal_name AS carrier,
                CASE
                    WHEN r.ltv_max <= 85    THEN '80-85'
                    WHEN r.ltv_max <= 90    THEN '85-90'
                    WHEN r.ltv_max <= 95    THEN '90-95'
                    ELSE '95-97'
                END AS ltv_band,
                MIN(r.rate_pct) AS min_rate,
                MAX(r.rate_pct) AS max_rate,
                ROUND(AVG(r.rate_pct), 4) AS avg_rate,
                COUNT(*) AS rate_count
            FROM hermes_pmi_rates r
            JOIN hermes_pmi_rate_cards rc ON rc.id = r.rate_card_id AND rc.is_current = TRUE
            JOIN hermes_carriers c ON c.id = rc.carrier_id
            GROUP BY c.legal_name, ltv_band
            ORDER BY c.legal_name, ltv_band
        """)
        async with self._engine.connect() as conn:
            rows = (await conn.execute(query)).mappings().all()

        by_carrier: dict[str, list[dict]] = {}
        for r in rows:
            carrier = r["carrier"]
            by_carrier.setdefault(carrier, []).append({
                "ltv_band": r["ltv_band"],
                "min_rate": float(r["min_rate"]),
                "max_rate": float(r["max_rate"]),
                "avg_rate": float(r["avg_rate"]),
                "rate_count": int(r["rate_count"]),
            })

        # Cheapest carrier per LTV band
        cheapest: dict[str, dict] = {}
        for r in rows:
            band = r["ltv_band"]
            rate = float(r["min_rate"])
            if band not in cheapest or rate < cheapest[band]["rate"]:
                cheapest[band] = {"carrier": r["carrier"], "rate": rate}

        return {
            "by_carrier": by_carrier,
            "cheapest_per_ltv_band": cheapest,
            "total_rates_in_db": sum(int(r["rate_count"]) for r in rows),
        }

    async def collect_sample_quotes(self) -> list[dict[str, Any]]:
        """Run 3 representative quotes through the PMI engine."""
        scenarios = [
            {"loan_amount": 300_000, "property_value": 333_333, "fico_score": 760,
             "label": "$300K / 90% LTV / 760 FICO"},
            {"loan_amount": 400_000, "property_value": 421_053, "fico_score": 720,
             "label": "$400K / 95% LTV / 720 FICO"},
            {"loan_amount": 500_000, "property_value": 588_235, "fico_score": 780,
             "label": "$500K / 85% LTV / 780 FICO"},
        ]
        results = []
        for s in scenarios:
            try:
                req = PMIQuoteRequest(
                    loan_amount=s["loan_amount"],
                    property_value=s["property_value"],
                    fico_score=s["fico_score"],
                )
                resp = await self._pmi.price_loan(req)
                quotes = []
                for q in resp.quotes:
                    quotes.append({
                        "carrier": q.carrier_name,
                        "rate_pct": q.adjusted_rate_pct,
                        "monthly": round(q.monthly_premium, 2),
                        "annual": round(q.annual_premium, 2),
                    })
                results.append({
                    "label": s["label"],
                    "ltv": round(resp.ltv, 2),
                    "coverage_pct": resp.coverage_pct,
                    "carriers_quoted": resp.carriers_quoted,
                    "quotes": quotes,
                    "best_monthly": round(resp.best_monthly.monthly_premium, 2) if resp.best_monthly else None,
                    "best_carrier": resp.best_monthly.carrier_name if resp.best_monthly else None,
                })
            except Exception as exc:
                logger.warning("Sample quote failed for %s: %s", s["label"], exc)
                results.append({"label": s["label"], "error": str(exc)})
        return results

    async def collect_competitive_spread(self) -> list[dict[str, Any]]:
        """Compute cheapest-to-most-expensive spread per LTV/FICO tier."""
        query = text("""
            SELECT
                CONCAT(r.ltv_min, '-', r.ltv_max) AS ltv_range,
                CONCAT(r.fico_min, '-', r.fico_max) AS fico_range,
                MIN(r.rate_pct) AS cheapest,
                MAX(r.rate_pct) AS most_expensive,
                MAX(r.rate_pct) - MIN(r.rate_pct) AS spread,
                COUNT(DISTINCT rc.carrier_id) AS carrier_count
            FROM hermes_pmi_rates r
            JOIN hermes_pmi_rate_cards rc ON rc.id = r.rate_card_id AND rc.is_current = TRUE
            GROUP BY r.ltv_min, r.ltv_max, r.fico_min, r.fico_max
            HAVING COUNT(DISTINCT rc.carrier_id) > 1
            ORDER BY spread DESC
            LIMIT 20
        """)
        async with self._engine.connect() as conn:
            rows = (await conn.execute(query)).mappings().all()
        return [
            {
                "ltv_range": r["ltv_range"],
                "fico_range": r["fico_range"],
                "cheapest": float(r["cheapest"]),
                "most_expensive": float(r["most_expensive"]),
                "spread": float(r["spread"]),
                "carrier_count": int(r["carrier_count"]),
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Full data package
    # ------------------------------------------------------------------

    async def build_data_package(self, request: Any) -> dict[str, Any]:
        """Merge live DB data with embedded market constants into one dict.

        Dispatches to product-specific data collection based on program_type.
        PMI uses live rate data from the DB; other products use embedded intelligence.
        """
        logger.info("Building MGA data package for program_type=%s", request.program_type)

        if request.program_type == "title":
            return await self._build_title_data_package(request)

        # Default: PMI (original flow — unchanged)
        return await self._build_pmi_data_package(request)

    async def _build_pmi_data_package(self, request: Any) -> dict[str, Any]:
        """PMI-specific data package with live rate DB queries."""
        carriers = await self.collect_carrier_profiles()
        rate_analysis = await self.collect_rate_analysis()
        sample_quotes = await self.collect_sample_quotes()
        competitive_spread = await self.collect_competitive_spread()

        from hermes.mga.templates import MARKET_INTELLIGENCE

        return {
            "live_data": {
                "carriers": carriers,
                "rate_analysis": rate_analysis,
                "sample_quotes": sample_quotes,
                "competitive_spread": competitive_spread,
            },
            "market_intelligence": MARKET_INTELLIGENCE,
            "request": {
                "program_type": request.program_type,
                "target_volume": request.target_volume,
                "distribution_partner": request.distribution_partner or "Better Mortgage",
                "target_states": request.target_states,
                "custom_context": request.custom_context,
            },
        }

    async def _build_title_data_package(self, request: Any) -> dict[str, Any]:
        """Title insurance data package with live DB queries via HermesTitleEngine.

        Queries live title rate data from the database (rate cards, simultaneous
        issue schedules, carrier info) and merges with embedded market intelligence.
        Falls back to embedded-only if no title engine is available.
        """
        from hermes.mga.templates import TITLE_INSURANCE_INTELLIGENCE

        live_data: dict[str, Any] = {
            "carriers": [],
            "rate_analysis": {},
            "sample_quotes": [],
            "competitive_spread": [],
            "simultaneous_issue_grid": {},
        }

        # Attempt live DB queries via HermesTitleEngine
        if self._title is not None:
            try:
                logger.info("Building title data package with live DB data")

                # Carrier profiles
                carrier_query = text("""
                    SELECT
                        c.id, c.naic_code, c.legal_name, c.am_best_rating,
                        COUNT(rc.id) AS rate_card_count,
                        ARRAY_AGG(DISTINCT rc.state) FILTER (WHERE rc.state IS NOT NULL) AS states
                    FROM hermes_carriers c
                    LEFT JOIN hermes_title_rate_cards rc
                        ON rc.carrier_id = c.id AND rc.is_current = TRUE
                    WHERE c.naic_code BETWEEN '60001' AND '60008'
                      AND c.status = 'active'
                    GROUP BY c.id, c.naic_code, c.legal_name, c.am_best_rating
                    ORDER BY c.naic_code
                """)
                async with self._engine.connect() as conn:
                    rows = (await conn.execute(carrier_query)).mappings().all()
                live_data["carriers"] = [
                    {
                        "id": str(r["id"]),
                        "naic_code": r["naic_code"],
                        "legal_name": r["legal_name"],
                        "am_best_rating": r["am_best_rating"],
                        "rate_card_count": int(r["rate_card_count"]),
                        "states": r["states"] or [],
                    }
                    for r in rows
                ]

                # Sample quotes (3 representative scenarios)
                from hermes.title.schemas import TitleQuoteRequest

                target_states = request.target_states or ["TX"]
                sample_state = target_states[0] if target_states else "TX"

                scenarios = [
                    {"purchase_price": 300_000, "loan_amount": 285_000, "label": "$300K / $285K loan"},
                    {"purchase_price": 400_000, "loan_amount": 380_000, "label": "$400K / $380K loan"},
                    {"purchase_price": 750_000, "loan_amount": 600_000, "label": "$750K / $600K loan"},
                ]
                for s in scenarios:
                    try:
                        req = TitleQuoteRequest(
                            purchase_price=s["purchase_price"],
                            loan_amount=s["loan_amount"],
                            state=sample_state,
                            policy_type="simultaneous",
                        )
                        resp = await self._title.price_policy(req)
                        quotes = []
                        for q in resp.quotes:
                            quotes.append({
                                "carrier": q.carrier_name,
                                "owner_premium": q.owner_premium,
                                "lender_premium": q.lender_premium,
                                "simultaneous_premium": q.simultaneous_premium,
                                "simultaneous_savings": q.simultaneous_savings,
                                "total": q.total_premium,
                                "is_promulgated": q.is_promulgated,
                            })
                        live_data["sample_quotes"].append({
                            "label": s["label"],
                            "state": sample_state,
                            "carriers_quoted": resp.carriers_quoted,
                            "quotes": quotes,
                            "best_total": resp.best_total.total_premium if resp.best_total else None,
                            "best_carrier": resp.best_total.carrier_name if resp.best_total else None,
                        })
                    except Exception as exc:
                        logger.warning("Title sample quote failed: %s", exc)
                        live_data["sample_quotes"].append({"label": s["label"], "error": str(exc)})

                # Simultaneous issue grid (THE key data product)
                try:
                    grid = await self._title.get_simultaneous_issue_grid(
                        state=sample_state,
                        purchase_price=400_000,
                    )
                    live_data["simultaneous_issue_grid"] = {
                        "carriers": grid.carriers,
                        "loan_amounts": grid.loan_amounts,
                        "max_savings_carrier": grid.max_savings_carrier,
                        "max_savings_amount": grid.max_savings_amount,
                        "entries_count": len(grid.entries),
                    }
                except Exception as exc:
                    logger.warning("Simultaneous issue grid failed: %s", exc)

            except Exception as exc:
                logger.warning("Title live data collection failed, using embedded: %s", exc)
        else:
            logger.info("No title engine available, using embedded intelligence only")

        return {
            "live_data": live_data,
            "market_intelligence": TITLE_INSURANCE_INTELLIGENCE,
            "request": {
                "program_type": request.program_type,
                "target_volume": request.target_volume,
                "distribution_partner": request.distribution_partner or "a leading mortgage originator",
                "target_states": request.target_states,
                "custom_context": request.custom_context,
                "embedded_distribution_context": getattr(request, "embedded_distribution_context", None),
            },
        }
