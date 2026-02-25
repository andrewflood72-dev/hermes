"""Hermes Title Insurance Pricing Engine — multi-carrier title rate comparison.

Queries all 8 title carriers' rate cards, computes tiered premiums, simultaneous
issue discounts, reissue credits, and endorsement fees.  Returns ranked results
with the key data product: cross-carrier simultaneous issue dispersion analysis.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes.config import settings
from hermes.title.schemas import (
    TitleCarrierQuote,
    TitleQuickQuoteRequest,
    TitleQuoteRequest,
    TitleQuoteResponse,
    TitleSimultaneousGridEntry,
    TitleSimultaneousGridResponse,
)

logger = logging.getLogger("hermes.title.engine")


class HermesTitleEngine:
    """Core title insurance pricing engine."""

    def __init__(self, db_engine: AsyncEngine | None = None) -> None:
        self._engine = db_engine
        logger.info("HermesTitleEngine initialised")

    async def _get_engine(self) -> AsyncEngine:
        if self._engine is None:
            self._engine = create_async_engine(
                settings.database_url, pool_size=10, max_overflow=20, echo=False
            )
        return self._engine

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            logger.info("HermesTitleEngine database engine disposed")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def price_policy(self, request: TitleQuoteRequest) -> TitleQuoteResponse:
        """Full pricing pipeline: query carriers → tiered premiums →
        simultaneous discount → reissue credit → endorsements → ranked results.
        """
        t0 = time.monotonic()
        request_id = uuid.uuid4()

        carriers = await self._get_title_carriers(
            state=request.state,
            carrier_ids=request.carrier_ids,
        )

        # Price across all carriers concurrently
        tasks = [
            self._price_carrier(
                carrier=c,
                purchase_price=request.purchase_price,
                loan_amount=request.loan_amount,
                state=request.state,
                policy_type=request.policy_type,
                is_refinance=request.is_refinance,
                years_since=request.years_since_prior_policy,
                endorsement_codes=request.endorsements,
            )
            for c in carriers
        ]
        results: list[TitleCarrierQuote | None] = await asyncio.gather(
            *tasks, return_exceptions=False
        )
        quotes = [q for q in results if q is not None]

        # Sort by total premium (lowest first)
        quotes.sort(key=lambda q: q.total_premium)

        best_total = quotes[0] if quotes else None
        best_savings = max(quotes, key=lambda q: q.simultaneous_savings) if quotes else None

        elapsed_ms = round((time.monotonic() - t0) * 1000, 1)

        response = TitleQuoteResponse(
            quotes=quotes,
            best_total=best_total,
            best_simultaneous_savings=best_savings if best_savings and best_savings.simultaneous_savings > 0 else None,
            purchase_price=request.purchase_price,
            loan_amount=request.loan_amount,
            state=request.state,
            policy_type=request.policy_type,
            carriers_quoted=len(carriers),
            processing_time_ms=elapsed_ms,
            request_id=request_id,
        )

        # Log the quote (fire-and-forget)
        asyncio.create_task(self._log_quote(request, response, elapsed_ms))

        return response

    async def quick_quote(self, request: TitleQuickQuoteRequest) -> TitleQuoteResponse:
        """Minimal-input quote — uses defaults for refinance, endorsements, etc."""
        policy_type = "simultaneous" if request.loan_amount > 0 else "owner"
        full_request = TitleQuoteRequest(
            purchase_price=request.purchase_price,
            loan_amount=request.loan_amount,
            state=request.state,
            policy_type=policy_type,
        )
        return await self.price_policy(full_request)

    async def get_simultaneous_issue_grid(
        self,
        state: str = "TX",
        purchase_price: float = 400_000,
    ) -> TitleSimultaneousGridResponse:
        """Build the cross-carrier simultaneous issue dispersion grid.

        This is THE key data product — shows how much the lender policy costs
        when issued simultaneously with the owner policy, across all carriers
        and loan amounts.
        """
        loan_amounts = [
            200_000, 300_000, 380_000, 400_000,
            500_000, 750_000, 1_000_000,
        ]

        carriers = await self._get_title_carriers(state=state)
        entries: list[TitleSimultaneousGridEntry] = []
        max_savings = 0.0
        max_savings_carrier = None

        for carrier in carriers:
            for loan_amt in loan_amounts:
                try:
                    # Owner premium (standalone)
                    owner = await self._compute_tiered_premium(
                        carrier["id"], state, "owner", purchase_price
                    )
                    # Lender premium (standalone)
                    lender = await self._compute_tiered_premium(
                        carrier["id"], state, "lender", loan_amt
                    )
                    # Simultaneous discount
                    simul_discount = await self._compute_simultaneous_discount(
                        carrier["id"], state, loan_amt
                    )

                    # Simultaneous total = owner + discounted lender
                    simul_lender = max(lender - simul_discount, 0)
                    simul_total = owner + simul_lender
                    standalone_total = owner + lender
                    savings = standalone_total - simul_total
                    discount_pct = round((savings / standalone_total * 100) if standalone_total > 0 else 0, 2)

                    if savings > max_savings:
                        max_savings = savings
                        max_savings_carrier = carrier["legal_name"]

                    entries.append(TitleSimultaneousGridEntry(
                        carrier_name=carrier["legal_name"],
                        carrier_id=carrier["id"],
                        naic_code=carrier["naic_code"],
                        loan_amount=loan_amt,
                        owner_premium=round(owner, 2),
                        lender_standalone=round(lender, 2),
                        simultaneous_premium=round(simul_total, 2),
                        simultaneous_savings=round(savings, 2),
                        discount_pct=discount_pct,
                        is_promulgated=carrier.get("is_promulgated", False),
                    ))
                except Exception as exc:
                    logger.warning(
                        "Grid entry error carrier=%s loan=%s: %s",
                        carrier["legal_name"], loan_amt, exc,
                    )

        return TitleSimultaneousGridResponse(
            entries=entries,
            loan_amounts=loan_amounts,
            carriers=[c["legal_name"] for c in carriers],
            max_savings_carrier=max_savings_carrier,
            max_savings_amount=round(max_savings, 2),
            generated_at=datetime.now(timezone.utc),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_title_carriers(
        self,
        state: str | None = None,
        carrier_ids: list[uuid.UUID] | None = None,
    ) -> list[dict[str, Any]]:
        """Return active title carriers that have current rate cards."""
        engine = await self._get_engine()

        conditions = ["c.status = 'active'", "rc.is_current = TRUE"]
        params: dict[str, Any] = {}

        if state:
            conditions.append("rc.state = :state")
            params["state"] = state

        if carrier_ids:
            conditions.append("c.id = ANY(:cids)")
            params["cids"] = [str(cid) for cid in carrier_ids]

        where = " AND ".join(conditions)

        query = text(f"""
            SELECT DISTINCT
                c.id, c.naic_code, c.legal_name, c.am_best_rating,
                BOOL_OR(rc.is_promulgated) AS is_promulgated
            FROM hermes_carriers c
            JOIN hermes_title_rate_cards rc ON rc.carrier_id = c.id
            WHERE {where}
            GROUP BY c.id, c.naic_code, c.legal_name, c.am_best_rating
            ORDER BY c.legal_name
        """)

        async with engine.connect() as conn:
            result = await conn.execute(query, params)
            rows = result.mappings().all()

        return [dict(row) for row in rows]

    async def _price_carrier(
        self,
        carrier: dict[str, Any],
        purchase_price: float,
        loan_amount: float,
        state: str,
        policy_type: str,
        is_refinance: bool,
        years_since: float | None,
        endorsement_codes: list[str],
    ) -> TitleCarrierQuote | None:
        """Price a single carrier for the requested transaction."""
        carrier_id = carrier["id"]

        try:
            owner_premium = 0.0
            lender_premium = 0.0
            simultaneous_premium = 0.0
            simultaneous_savings = 0.0
            simultaneous_discount_pct = 0.0
            reissue_credit = 0.0

            # Owner policy
            if policy_type in ("owner", "simultaneous"):
                owner_premium = await self._compute_tiered_premium(
                    carrier_id, state, "owner", purchase_price
                )

            # Lender policy
            if policy_type in ("lender", "simultaneous") and loan_amount > 0:
                lender_premium = await self._compute_tiered_premium(
                    carrier_id, state, "lender", loan_amount
                )

            # Simultaneous issue discount
            if policy_type == "simultaneous" and loan_amount > 0:
                discount = await self._compute_simultaneous_discount(
                    carrier_id, state, loan_amount
                )
                standalone_total = owner_premium + lender_premium
                discounted_lender = max(lender_premium - discount, 0)
                simultaneous_premium = owner_premium + discounted_lender
                simultaneous_savings = standalone_total - simultaneous_premium
                if standalone_total > 0:
                    simultaneous_discount_pct = round(
                        simultaneous_savings / standalone_total * 100, 2
                    )

            # Reissue credit (refinance)
            if is_refinance and years_since is not None:
                base = owner_premium if owner_premium > 0 else lender_premium
                reissue_credit = await self._compute_reissue_credit(
                    carrier_id, state, years_since, base
                )

            # Endorsement fees
            endorsement_fees = 0.0
            if endorsement_codes:
                base = owner_premium if owner_premium > 0 else lender_premium
                endorsement_fees = await self._compute_endorsement_fees(
                    carrier_id, state, endorsement_codes, base
                )

            # Total premium
            if policy_type == "simultaneous" and loan_amount > 0:
                total = simultaneous_premium - reissue_credit + endorsement_fees
            elif policy_type == "owner":
                total = owner_premium - reissue_credit + endorsement_fees
            else:
                total = lender_premium - reissue_credit + endorsement_fees
            total = max(total, 0)

            # Rate card metadata
            rc_meta = await self._get_rate_card_meta(carrier_id, state)

            return TitleCarrierQuote(
                carrier_id=carrier_id,
                carrier_name=carrier["legal_name"],
                naic_code=carrier["naic_code"],
                am_best_rating=carrier.get("am_best_rating"),
                owner_premium=round(owner_premium, 2),
                lender_premium=round(lender_premium, 2),
                simultaneous_premium=round(simultaneous_premium, 2),
                simultaneous_savings=round(simultaneous_savings, 2),
                simultaneous_discount_pct=simultaneous_discount_pct,
                reissue_credit=round(reissue_credit, 2),
                endorsement_fees=round(endorsement_fees, 2),
                total_premium=round(total, 2),
                is_promulgated=carrier.get("is_promulgated", False),
                rate_card_source=rc_meta.get("source", "manual"),
                rate_card_effective=rc_meta.get("effective_date"),
            )

        except Exception as exc:
            logger.exception(
                "Error pricing title carrier=%s state=%s: %s",
                carrier["legal_name"], state, exc,
            )
            return None

    async def _compute_tiered_premium(
        self,
        carrier_id: Any,
        state: str,
        policy_type: str,
        insured_amount: float,
    ) -> float:
        """Walk coverage bands and sum band_amount * rate_per_thousand / 1000.

        Title rates are tiered: e.g. first $100K at $5.75/thousand,
        next $100K-$500K at $4.00/thousand, etc.
        """
        engine = await self._get_engine()

        query = text("""
            SELECT r.coverage_min, r.coverage_max,
                   r.rate_per_thousand, r.flat_fee, r.minimum_premium
            FROM hermes_title_rates r
            JOIN hermes_title_rate_cards rc ON rc.id = r.rate_card_id
            WHERE rc.carrier_id = :carrier_id
              AND rc.state = :state
              AND rc.policy_type = :ptype
              AND rc.is_current = TRUE
            ORDER BY r.coverage_min ASC
        """)

        async with engine.connect() as conn:
            result = await conn.execute(query, {
                "carrier_id": carrier_id,
                "state": state,
                "ptype": policy_type,
            })
            bands = result.mappings().all()

        if not bands:
            return 0.0

        total_premium = 0.0
        total_flat = 0.0
        min_premium = 0.0

        for band in bands:
            cov_min = float(band["coverage_min"])
            cov_max = float(band["coverage_max"])
            rpt = float(band["rate_per_thousand"])
            flat = float(band["flat_fee"])
            min_p = float(band["minimum_premium"])

            if min_p > min_premium:
                min_premium = min_p

            if insured_amount <= cov_min:
                break

            band_amount = min(insured_amount, cov_max) - cov_min
            if band_amount > 0:
                total_premium += band_amount * rpt / 1000.0
                total_flat += flat

        total = total_premium + total_flat
        return max(total, min_premium)

    async def _compute_simultaneous_discount(
        self,
        carrier_id: Any,
        state: str,
        loan_amount: float,
    ) -> float:
        """Compute the simultaneous issue discount for a given loan amount.

        Returns the dollar discount to subtract from the lender premium.
        """
        engine = await self._get_engine()

        # Look for a simultaneous-type rate card first
        query = text("""
            SELECT si.loan_min, si.loan_max,
                   si.discount_rate_per_thousand, si.discount_pct, si.flat_fee
            FROM hermes_title_simultaneous_issue si
            JOIN hermes_title_rate_cards rc ON rc.id = si.rate_card_id
            WHERE rc.carrier_id = :carrier_id
              AND rc.state = :state
              AND rc.is_current = TRUE
              AND :loan BETWEEN si.loan_min AND si.loan_max
            ORDER BY si.loan_min ASC
            LIMIT 1
        """)

        async with engine.connect() as conn:
            result = await conn.execute(query, {
                "carrier_id": carrier_id,
                "state": state,
                "loan": Decimal(str(loan_amount)),
            })
            row = result.mappings().first()

        if not row:
            return 0.0

        discount = 0.0
        drpt = float(row["discount_rate_per_thousand"])
        dpct = float(row["discount_pct"])
        flat = float(row["flat_fee"])

        if drpt > 0:
            discount = loan_amount * drpt / 1000.0
        elif dpct > 0:
            # Percentage discount off the lender premium
            lender = await self._compute_tiered_premium(
                carrier_id, state, "lender", loan_amount
            )
            discount = lender * dpct / 100.0

        discount += flat
        return discount

    async def _compute_reissue_credit(
        self,
        carrier_id: Any,
        state: str,
        years_since: float,
        base_premium: float,
    ) -> float:
        """Compute reissue credit for a refinance transaction."""
        engine = await self._get_engine()

        query = text("""
            SELECT rc2.credit_pct
            FROM hermes_title_reissue_credits rc2
            JOIN hermes_title_rate_cards rc ON rc.id = rc2.rate_card_id
            WHERE rc.carrier_id = :carrier_id
              AND rc.state = :state
              AND rc.is_current = TRUE
              AND :years BETWEEN rc2.years_since_min AND rc2.years_since_max
            ORDER BY rc2.credit_pct DESC
            LIMIT 1
        """)

        async with engine.connect() as conn:
            result = await conn.execute(query, {
                "carrier_id": carrier_id,
                "state": state,
                "years": Decimal(str(years_since)),
            })
            row = result.mappings().first()

        if not row:
            return 0.0

        credit_pct = float(row["credit_pct"])
        return base_premium * credit_pct / 100.0

    async def _compute_endorsement_fees(
        self,
        carrier_id: Any,
        state: str,
        codes: list[str],
        base_premium: float,
    ) -> float:
        """Compute total endorsement fees for a list of ALTA codes."""
        if not codes:
            return 0.0

        engine = await self._get_engine()

        query = text("""
            SELECT e.endorsement_code, e.flat_fee,
                   e.rate_per_thousand, e.pct_of_base
            FROM hermes_title_endorsements e
            JOIN hermes_title_rate_cards rc ON rc.id = e.rate_card_id
            WHERE rc.carrier_id = :carrier_id
              AND rc.state = :state
              AND rc.is_current = TRUE
              AND e.endorsement_code = ANY(:codes)
        """)

        async with engine.connect() as conn:
            result = await conn.execute(query, {
                "carrier_id": carrier_id,
                "state": state,
                "codes": codes,
            })
            rows = result.mappings().all()

        total = 0.0
        for row in rows:
            fee = float(row["flat_fee"])
            rpt = float(row["rate_per_thousand"])
            pct = float(row["pct_of_base"])

            if rpt > 0:
                fee += base_premium * rpt / 1000.0
            if pct > 0:
                fee += base_premium * pct

            total += fee

        return total

    async def _get_rate_card_meta(
        self, carrier_id: Any, state: str
    ) -> dict[str, Any]:
        """Return source and effective_date for the current rate card."""
        engine = await self._get_engine()
        query = text("""
            SELECT source, effective_date
            FROM hermes_title_rate_cards
            WHERE carrier_id = :cid AND state = :state AND is_current = TRUE
            ORDER BY effective_date DESC
            LIMIT 1
        """)
        async with engine.connect() as conn:
            result = await conn.execute(query, {"cid": carrier_id, "state": state})
            row = result.mappings().first()
        if row:
            eff = row["effective_date"]
            return {
                "source": row["source"],
                "effective_date": eff.isoformat() if isinstance(eff, (date, datetime)) else eff,
            }
        return {}

    async def _log_quote(
        self,
        request: TitleQuoteRequest,
        response: TitleQuoteResponse,
        elapsed_ms: float,
    ) -> None:
        """Insert an audit trail record into hermes_title_quote_log."""
        try:
            engine = await self._get_engine()
            best_carrier_id = None
            best_premium = None
            if response.quotes:
                best = response.quotes[0]
                best_carrier_id = best.carrier_id
                best_premium = Decimal(str(best.total_premium))

            async with engine.begin() as conn:
                await conn.execute(
                    text("""
                        INSERT INTO hermes_title_quote_log
                            (request_data, response_data, carriers_quoted,
                             best_premium, best_carrier_id, processing_time_ms, source)
                        VALUES
                            (CAST(:req AS jsonb), CAST(:resp AS jsonb), :count,
                             :best_premium, :best_cid, :elapsed, 'api')
                    """),
                    {
                        "req": json.dumps(request.model_dump(), default=str),
                        "resp": json.dumps({
                            "carriers_quoted": response.carriers_quoted,
                            "quotes_returned": len(response.quotes),
                            "best_total": response.quotes[0].total_premium if response.quotes else None,
                        }, default=str),
                        "count": response.carriers_quoted,
                        "best_premium": best_premium,
                        "best_cid": best_carrier_id,
                        "elapsed": Decimal(str(elapsed_ms)),
                    },
                )
        except Exception as exc:
            logger.warning("Failed to log title quote: %s", exc)
