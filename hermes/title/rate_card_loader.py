"""Title insurance rate card data ingestion — bulk load rate cards, rates,
simultaneous issue discounts, reissue credits, and endorsement pricing."""

from __future__ import annotations

import csv
import json
import logging
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes.config import settings

logger = logging.getLogger("hermes.title.rate_card_loader")


class TitleRateCardLoader:
    """Loads title insurance rate card data into the database."""

    def __init__(self, db_engine: AsyncEngine | None = None) -> None:
        self._engine = db_engine

    async def _get_engine(self) -> AsyncEngine:
        if self._engine is None:
            self._engine = create_async_engine(
                settings.database_url, pool_size=5, max_overflow=10, echo=False
            )
        return self._engine

    async def load_rate_card(
        self,
        carrier_naic: str,
        policy_type: str,
        state: str,
        effective_date: date,
        rates: list[dict[str, Any]],
        simultaneous: list[dict[str, Any]] | None = None,
        reissue_credits: list[dict[str, Any]] | None = None,
        endorsements: list[dict[str, Any]] | None = None,
        is_promulgated: bool = False,
        source: str = "manual",
        notes: str | None = None,
    ) -> dict[str, Any]:
        """Bulk insert a title rate card with all sub-tables.

        Parameters
        ----------
        carrier_naic:
            NAIC code of the carrier.
        policy_type:
            One of: owner, lender, simultaneous, reissue, endorsement.
        state:
            Two-letter state code.
        effective_date:
            Date the rate card becomes effective.
        rates:
            List of dicts: coverage_min, coverage_max, rate_per_thousand,
            flat_fee, minimum_premium.
        simultaneous:
            Optional list of dicts: loan_min, loan_max,
            discount_rate_per_thousand, discount_pct, flat_fee, conditions.
        reissue_credits:
            Optional list of dicts: years_since_min, years_since_max,
            credit_pct, conditions.
        endorsements:
            Optional list of dicts: endorsement_code, endorsement_name,
            flat_fee, rate_per_thousand, pct_of_base, description.
        is_promulgated:
            True for states with state-set rates (e.g. TX).
        source:
            Data source identifier.
        notes:
            Optional notes about this rate card.

        Returns
        -------
        dict with rate_card_id and insert counts.
        """
        engine = await self._get_engine()
        rate_card_id = uuid.uuid4()

        async with engine.begin() as conn:
            # Look up carrier ID from NAIC code
            result = await conn.execute(
                text("SELECT id FROM hermes_carriers WHERE naic_code = :naic"),
                {"naic": carrier_naic},
            )
            row = result.mappings().first()
            if not row:
                raise ValueError(f"Carrier with NAIC code {carrier_naic} not found")
            carrier_id = row["id"]

            # Insert new rate card first (before supersede, to satisfy FK)
            await conn.execute(
                text("""
                    INSERT INTO hermes_title_rate_cards
                        (id, carrier_id, policy_type, state, is_promulgated,
                         effective_date, is_current, source, version, notes)
                    VALUES
                        (:id, :carrier_id, :ptype, :state, :promulgated,
                         :eff_date, TRUE, :source, 1, :notes)
                """),
                {
                    "id": rate_card_id,
                    "carrier_id": carrier_id,
                    "ptype": policy_type,
                    "state": state,
                    "promulgated": is_promulgated,
                    "eff_date": effective_date,
                    "source": source,
                    "notes": notes,
                },
            )

            # Supersede existing current cards for same carrier/type/state
            await conn.execute(
                text("""
                    UPDATE hermes_title_rate_cards
                    SET is_current = FALSE, superseded_by = :new_id
                    WHERE carrier_id = :carrier_id
                      AND policy_type = :ptype
                      AND state = :state
                      AND is_current = TRUE
                      AND id != :new_id
                """),
                {
                    "new_id": rate_card_id,
                    "carrier_id": carrier_id,
                    "ptype": policy_type,
                    "state": state,
                },
            )

            # Bulk insert rates
            for r in rates:
                await conn.execute(
                    text("""
                        INSERT INTO hermes_title_rates
                            (rate_card_id, coverage_min, coverage_max,
                             rate_per_thousand, flat_fee, minimum_premium)
                        VALUES
                            (:rc_id, :cov_min, :cov_max,
                             :rpt, :flat, :min_prem)
                    """),
                    {
                        "rc_id": rate_card_id,
                        "cov_min": Decimal(str(r.get("coverage_min", 0))),
                        "cov_max": Decimal(str(r["coverage_max"])),
                        "rpt": Decimal(str(r["rate_per_thousand"])),
                        "flat": Decimal(str(r.get("flat_fee", 0))),
                        "min_prem": Decimal(str(r.get("minimum_premium", 0))),
                    },
                )

            # Bulk insert simultaneous issue discounts
            simul_count = 0
            if simultaneous:
                for s in simultaneous:
                    await conn.execute(
                        text("""
                            INSERT INTO hermes_title_simultaneous_issue
                                (rate_card_id, loan_min, loan_max,
                                 discount_rate_per_thousand, discount_pct,
                                 flat_fee, conditions)
                            VALUES
                                (:rc_id, :loan_min, :loan_max,
                                 :drpt, :dpct, :flat,
                                 CAST(:cond AS jsonb))
                        """),
                        {
                            "rc_id": rate_card_id,
                            "loan_min": Decimal(str(s.get("loan_min", 0))),
                            "loan_max": Decimal(str(s["loan_max"])),
                            "drpt": Decimal(str(s.get("discount_rate_per_thousand", 0))),
                            "dpct": Decimal(str(s.get("discount_pct", 0))),
                            "flat": Decimal(str(s.get("flat_fee", 0))),
                            "cond": json.dumps(s.get("conditions", {})),
                        },
                    )
                    simul_count += 1

            # Bulk insert reissue credits
            reissue_count = 0
            if reissue_credits:
                for rc in reissue_credits:
                    await conn.execute(
                        text("""
                            INSERT INTO hermes_title_reissue_credits
                                (rate_card_id, years_since_min, years_since_max,
                                 credit_pct, conditions)
                            VALUES
                                (:rc_id, :yr_min, :yr_max,
                                 :credit, CAST(:cond AS jsonb))
                        """),
                        {
                            "rc_id": rate_card_id,
                            "yr_min": Decimal(str(rc.get("years_since_min", 0))),
                            "yr_max": Decimal(str(rc["years_since_max"])),
                            "credit": Decimal(str(rc["credit_pct"])),
                            "cond": json.dumps(rc.get("conditions", {})),
                        },
                    )
                    reissue_count += 1

            # Bulk insert endorsements
            endorse_count = 0
            if endorsements:
                for e in endorsements:
                    await conn.execute(
                        text("""
                            INSERT INTO hermes_title_endorsements
                                (rate_card_id, endorsement_code, endorsement_name,
                                 flat_fee, rate_per_thousand, pct_of_base, description)
                            VALUES
                                (:rc_id, :code, :name,
                                 :flat, :rpt, :pct, :desc)
                        """),
                        {
                            "rc_id": rate_card_id,
                            "code": e["endorsement_code"],
                            "name": e["endorsement_name"],
                            "flat": Decimal(str(e.get("flat_fee", 0))),
                            "rpt": Decimal(str(e.get("rate_per_thousand", 0))),
                            "pct": Decimal(str(e.get("pct_of_base", 0))),
                            "desc": e.get("description"),
                        },
                    )
                    endorse_count += 1

        logger.info(
            "Loaded title rate card %s for carrier %s (%s, state=%s): "
            "%d rates, %d simultaneous, %d reissue, %d endorsements",
            rate_card_id, carrier_naic, policy_type, state,
            len(rates), simul_count, reissue_count, endorse_count,
        )
        return {
            "rate_card_id": str(rate_card_id),
            "carrier_naic": carrier_naic,
            "policy_type": policy_type,
            "state": state,
            "is_promulgated": is_promulgated,
            "rates_inserted": len(rates),
            "simultaneous_inserted": simul_count,
            "reissue_inserted": reissue_count,
            "endorsements_inserted": endorse_count,
        }

    async def load_promulgated_rates(
        self,
        state: str,
        effective_date: date,
        rates: list[dict[str, Any]],
        simultaneous: list[dict[str, Any]] | None = None,
        reissue_credits: list[dict[str, Any]] | None = None,
        endorsements: list[dict[str, Any]] | None = None,
        source: str = "tdi",
        notes: str | None = None,
    ) -> list[dict[str, Any]]:
        """Load promulgated (state-set) rates — creates identical rate cards
        for ALL title carriers.  Used for states like TX where rates are
        regulated and uniform across all carriers.

        Returns a list of load results, one per carrier.
        """
        engine = await self._get_engine()

        # Get all title carrier NAIC codes (60001-60008)
        async with engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT naic_code FROM hermes_carriers
                    WHERE naic_code BETWEEN '60001' AND '60008'
                      AND status = 'active'
                    ORDER BY naic_code
                """)
            )
            carriers = [row[0] for row in result.fetchall()]

        if not carriers:
            raise ValueError("No title carriers found in database")

        results = []
        for naic in carriers:
            for policy_type in ["owner", "lender"]:
                r = await self.load_rate_card(
                    carrier_naic=naic,
                    policy_type=policy_type,
                    state=state,
                    effective_date=effective_date,
                    rates=rates,
                    simultaneous=simultaneous if policy_type == "owner" else None,
                    reissue_credits=reissue_credits,
                    endorsements=endorsements if policy_type == "owner" else None,
                    is_promulgated=True,
                    source=source,
                    notes=notes or f"Promulgated {state} rates — identical for all carriers",
                )
                results.append(r)

        logger.info(
            "Loaded promulgated rates for %s: %d carriers × 2 policy types = %d cards",
            state, len(carriers), len(results),
        )
        return results

    async def load_from_csv(
        self,
        csv_path: str | Path,
        carrier_naic: str,
        policy_type: str,
        state: str,
        effective_date: date,
        is_promulgated: bool = False,
    ) -> dict[str, Any]:
        """Load a title rate card from a CSV file.

        Expected columns: coverage_min, coverage_max, rate_per_thousand,
        flat_fee, minimum_premium
        """
        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        rates: list[dict[str, Any]] = []
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rates.append({
                    "coverage_min": float(row.get("coverage_min", 0)),
                    "coverage_max": float(row["coverage_max"]),
                    "rate_per_thousand": float(row["rate_per_thousand"]),
                    "flat_fee": float(row.get("flat_fee", 0)),
                    "minimum_premium": float(row.get("minimum_premium", 0)),
                })

        return await self.load_rate_card(
            carrier_naic=carrier_naic,
            policy_type=policy_type,
            state=state,
            effective_date=effective_date,
            rates=rates,
            is_promulgated=is_promulgated,
            source="csv",
            notes=f"Imported from {path.name}",
        )

    async def supersede_card(self, rate_card_id: uuid.UUID) -> bool:
        """Mark a title rate card as no longer current.

        Returns True if the card was found and updated.
        """
        engine = await self._get_engine()
        async with engine.begin() as conn:
            result = await conn.execute(
                text("""
                    UPDATE hermes_title_rate_cards
                    SET is_current = FALSE
                    WHERE id = :id AND is_current = TRUE
                """),
                {"id": rate_card_id},
            )
            updated = result.rowcount > 0

        if updated:
            logger.info("Superseded title rate card %s", rate_card_id)
        else:
            logger.warning("Title rate card %s not found or already superseded", rate_card_id)
        return updated
