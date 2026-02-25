"""PMI rate card data ingestion — bulk load rate cards, rates, and adjustments."""

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

logger = logging.getLogger("hermes.pmi.rate_card_loader")


class PMIRateCardLoader:
    """Loads PMI rate card data into the database."""

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
        premium_type: str,
        effective_date: date,
        rates: list[dict[str, Any]],
        adjustments: list[dict[str, Any]] | None = None,
        state: str | None = None,
        source: str = "manual",
        notes: str | None = None,
    ) -> dict[str, Any]:
        """Bulk insert a rate card with its rates and adjustments.

        Parameters
        ----------
        carrier_naic:
            NAIC code of the carrier.
        premium_type:
            One of: monthly, single, split, lender_paid.
        effective_date:
            Date the rate card becomes effective.
        rates:
            List of dicts with keys: ltv_min, ltv_max, fico_min, fico_max,
            coverage_pct, rate_pct.
        adjustments:
            Optional list of dicts with keys: name, condition, adjustment_method,
            adjustment_value, description.
        state:
            Two-letter state code, or None for nationwide.
        source:
            Data source identifier.
        notes:
            Optional notes about this rate card.

        Returns
        -------
        dict with rate_card_id, rates_inserted, adjustments_inserted.
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

            # Supersede existing current cards for same carrier/type/state
            await conn.execute(
                text("""
                    UPDATE hermes_pmi_rate_cards
                    SET is_current = FALSE, superseded_by = :new_id
                    WHERE carrier_id = :carrier_id
                      AND premium_type = :ptype
                      AND (state = :state OR (state IS NULL AND :state IS NULL))
                      AND is_current = TRUE
                """),
                {
                    "new_id": rate_card_id,
                    "carrier_id": carrier_id,
                    "ptype": premium_type,
                    "state": state,
                },
            )

            # Insert rate card
            await conn.execute(
                text("""
                    INSERT INTO hermes_pmi_rate_cards
                        (id, carrier_id, premium_type, state, effective_date,
                         is_current, source, version, notes)
                    VALUES
                        (:id, :carrier_id, :ptype, :state, :eff_date,
                         TRUE, :source, 1, :notes)
                """),
                {
                    "id": rate_card_id,
                    "carrier_id": carrier_id,
                    "ptype": premium_type,
                    "state": state,
                    "eff_date": effective_date,
                    "source": source,
                    "notes": notes,
                },
            )

            # Bulk insert rates
            for r in rates:
                await conn.execute(
                    text("""
                        INSERT INTO hermes_pmi_rates
                            (rate_card_id, ltv_min, ltv_max, fico_min, fico_max,
                             coverage_pct, rate_pct)
                        VALUES
                            (:rc_id, :ltv_min, :ltv_max, :fico_min, :fico_max,
                             :cov, :rate)
                    """),
                    {
                        "rc_id": rate_card_id,
                        "ltv_min": Decimal(str(r["ltv_min"])),
                        "ltv_max": Decimal(str(r["ltv_max"])),
                        "fico_min": r["fico_min"],
                        "fico_max": r["fico_max"],
                        "cov": Decimal(str(r["coverage_pct"])),
                        "rate": Decimal(str(r["rate_pct"])),
                    },
                )

            # Bulk insert adjustments
            adj_count = 0
            if adjustments:
                for a in adjustments:
                    await conn.execute(
                        text("""
                            INSERT INTO hermes_pmi_adjustments
                                (rate_card_id, name, condition, adjustment_method,
                                 adjustment_value, description)
                            VALUES
                                (:rc_id, :name, CAST(:cond AS jsonb), :method, :val, :desc)
                        """),
                        {
                            "rc_id": rate_card_id,
                            "name": a["name"],
                            "cond": json.dumps(a.get("condition", {})),
                            "method": a["adjustment_method"],
                            "val": Decimal(str(a["adjustment_value"])),
                            "desc": a.get("description"),
                        },
                    )
                    adj_count += 1

        logger.info(
            "Loaded rate card %s for carrier %s (%s, state=%s): %d rates, %d adjustments",
            rate_card_id, carrier_naic, premium_type, state or "nationwide",
            len(rates), adj_count,
        )
        return {
            "rate_card_id": str(rate_card_id),
            "carrier_naic": carrier_naic,
            "premium_type": premium_type,
            "state": state,
            "rates_inserted": len(rates),
            "adjustments_inserted": adj_count,
        }

    async def load_from_csv(
        self,
        csv_path: str | Path,
        carrier_naic: str,
        premium_type: str,
        effective_date: date,
        state: str | None = None,
    ) -> dict[str, Any]:
        """Load a rate card from a CSV file.

        Expected columns: ltv_min, ltv_max, fico_min, fico_max, coverage_pct, rate_pct
        """
        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        rates: list[dict[str, Any]] = []
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rates.append({
                    "ltv_min": float(row["ltv_min"]),
                    "ltv_max": float(row["ltv_max"]),
                    "fico_min": int(row["fico_min"]),
                    "fico_max": int(row["fico_max"]),
                    "coverage_pct": float(row["coverage_pct"]),
                    "rate_pct": float(row["rate_pct"]),
                })

        return await self.load_rate_card(
            carrier_naic=carrier_naic,
            premium_type=premium_type,
            effective_date=effective_date,
            rates=rates,
            state=state,
            source="csv",
            notes=f"Imported from {path.name}",
        )

    async def supersede_card(self, rate_card_id: uuid.UUID) -> bool:
        """Mark a rate card as no longer current.

        Returns True if the card was found and updated.
        """
        engine = await self._get_engine()
        async with engine.begin() as conn:
            result = await conn.execute(
                text("""
                    UPDATE hermes_pmi_rate_cards
                    SET is_current = FALSE
                    WHERE id = :id AND is_current = TRUE
                """),
                {"id": rate_card_id},
            )
            updated = result.rowcount > 0

        if updated:
            logger.info("Superseded rate card %s", rate_card_id)
        else:
            logger.warning("Rate card %s not found or already superseded", rate_card_id)
        return updated
