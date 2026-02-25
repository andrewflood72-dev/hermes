"""MGAProposalAgent — AI-powered MGA business proposal generator.

Follows the AIExtractor pattern: AsyncAnthropic client with tenacity retry,
token tracking, and sequential section generation via Claude Opus.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from anthropic import AsyncAnthropic, InternalServerError, RateLimitError, APIConnectionError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from hermes.config import settings
from hermes.mga.data_collector import MGADataCollector
from hermes.mga.schemas import (
    FinancialProjection,
    MGAProposalRequest,
    MGAProposalResponse,
    ProposalSection,
)
from hermes.mga.templates import (
    FINANCIAL_PROMPTS_BY_PRODUCT,
    PRODUCT_CATALOG,
    SECTION_ORDER_BY_PRODUCT,
    SECTION_PROMPTS,
    SECTION_PROMPTS_BY_PRODUCT,
    SECTION_TITLES,
    financial_projections_prompt,
)
from hermes.pmi.engine import HermesPMIEngine

logger = logging.getLogger(__name__)

# ── Retry configuration ──────────────────────────────────────────────────

_RETRY_EXCEPTIONS = (RateLimitError, APIConnectionError, InternalServerError)

_retry_policy = dict(
    retry=retry_if_exception_type(_RETRY_EXCEPTIONS),
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=2, min=4, max=120),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)

# ── Cost constants (USD per million tokens — Claude Opus 4.6) ────────────

_INPUT_COST_PER_M = 15.00
_OUTPUT_COST_PER_M = 75.00

# ── Product-specific financial assumptions ───────────────────────────────

PRODUCT_FINANCIALS: dict[str, dict] = {
    "pmi": {
        "ramp_pcts": [0.10, 0.30, 0.60, 0.85, 1.00],
        "loss_ratios": [0.22, 0.21, 0.20, 0.19, 0.18],
        "expense_ratios": [0.35, 0.30, 0.26, 0.24, 0.22],
        "commission_rate": 0.25,
        "default_avg_rate_pct": 0.55,
    },
    "title": {
        "ramp_pcts": [0.08, 0.25, 0.50, 0.75, 1.00],
        "loss_ratios": [0.06, 0.055, 0.05, 0.05, 0.045],
        "expense_ratios": [0.55, 0.48, 0.42, 0.38, 0.35],
        "commission_rate": 0.40,  # higher commission — title has high expense ratio savings
        "default_avg_rate_pct": 0.50,  # ~$500 per $100K loan amount as rough average
    },
}


def _strip_fences(text_val: str) -> str:
    """Remove markdown code fences from a model response."""
    text_val = text_val.strip()
    if text_val.startswith("```"):
        text_val = re.sub(r"^```(?:json)?\s*", "", text_val)
        text_val = re.sub(r"\s*```$", "", text_val)
    return text_val.strip()


class MGAProposalAgent:
    """Generates comprehensive MGA business proposals using Claude AI."""

    def __init__(
        self,
        db_engine: AsyncEngine,
        pmi_engine: HermesPMIEngine,
        title_engine: Any = None,
    ) -> None:
        self._engine = db_engine
        self._pmi = pmi_engine
        self._title = title_engine
        self._client = AsyncAnthropic(api_key=settings.anthropic_api_key)
        self._collector = MGADataCollector(
            db_engine=db_engine, pmi_engine=pmi_engine, title_engine=title_engine
        )

        # Token tracking
        self.total_input_tokens: int = 0
        self.total_output_tokens: int = 0
        self.total_calls: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def generate_proposal(
        self, request: MGAProposalRequest
    ) -> MGAProposalResponse:
        """Full proposal generation pipeline.

        1. Collect data package (live DB + market constants)
        2. Compute financial projections (pure math, no LLM)
        3. Generate each section via Claude sequentially
        4. Assemble and persist proposal
        """
        t0 = time.monotonic()
        proposal_id = uuid.uuid4()

        logger.info("Starting proposal generation id=%s program=%s", proposal_id, request.program_type)

        # 1. Data collection
        data_package = await self._collector.build_data_package(request)
        logger.info("Data package assembled: %d carriers, %d rates",
                     len(data_package["live_data"]["carriers"]),
                     data_package["live_data"]["rate_analysis"].get("total_rates_in_db", 0))

        # 2. Financial projections (computed, not generated)
        projections = self._compute_financial_projections(request, data_package)

        # 3. Generate sections sequentially (product-aware)
        sections: dict[str, ProposalSection] = {}
        product_type = request.program_type

        # Select the right prompt registry and section order for this product
        section_prompts = SECTION_PROMPTS_BY_PRODUCT.get(product_type, SECTION_PROMPTS)
        section_order = SECTION_ORDER_BY_PRODUCT.get(product_type, SECTION_ORDER_BY_PRODUCT["pmi"])
        fp_prompt_fn = FINANCIAL_PROMPTS_BY_PRODUCT.get(product_type, financial_projections_prompt)

        for key in section_order:
            prompt_fn = section_prompts.get(key)
            if not prompt_fn:
                logger.warning("No prompt function for section '%s' in product '%s', skipping", key, product_type)
                continue
            prompt = prompt_fn(data_package)
            content = await self._generate_section(key, prompt)
            sections[key] = ProposalSection(
                title=SECTION_TITLES.get(key, key.replace("_", " ").title()),
                content=content,
            )
            logger.info("Section '%s' generated (%d chars)", key, len(content))

        # Financial projections section (special — includes computed data)
        proj_dicts = [p.model_dump() for p in projections]
        fp_prompt = fp_prompt_fn(data_package, proj_dicts)
        fp_content = await self._generate_section("financial_projections", fp_prompt)
        sections["financial_projections"] = ProposalSection(
            title=SECTION_TITLES["financial_projections"],
            content=fp_content,
            data_tables=[{"projections": proj_dicts}],
            key_metrics={
                "year_5_premium": projections[-1].premium_volume if projections else 0,
                "year_5_net_income": projections[-1].net_income if projections else 0,
                "cumulative_income": projections[-1].cumulative_income if projections else 0,
            },
        )

        # 4. Assemble response
        elapsed_s = time.monotonic() - t0
        token_usage = {
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "total_calls": self.total_calls,
            "estimated_cost_usd": round(self.total_cost_usd, 4),
            "generation_time_s": round(elapsed_s, 1),
        }

        # Dynamic title based on product
        product_name = PRODUCT_CATALOG.get(product_type, {}).get("name", product_type.upper())
        partner = request.distribution_partner or "Strategic"
        title = f"{product_name} MGA Proposal — {partner} Partnership"

        # 5. Persist to DB
        await self._persist_proposal(
            proposal_id=proposal_id,
            request=request,
            title=title,
            sections=sections,
            projections=projections,
            token_usage=token_usage,
        )

        exec_summary_text = sections.get("executive_summary")
        exec_summary = exec_summary_text.content if exec_summary_text else ""

        return MGAProposalResponse(
            id=proposal_id,
            program_type=request.program_type,
            title=title,
            sections=sections,
            financial_projections=projections,
            executive_summary=exec_summary,
            status="complete",
            token_usage=token_usage,
            generated_at=datetime.now(timezone.utc),
        )

    async def close(self) -> None:
        """Cleanup resources."""
        pass

    # ------------------------------------------------------------------
    # Section generation
    # ------------------------------------------------------------------

    @retry(**_retry_policy)
    async def _generate_section(self, section_key: str, prompt: str) -> str:
        """Call Claude to generate a single proposal section."""
        response = await self._client.messages.create(
            model="claude-opus-4-6",
            max_tokens=8192,
            messages=[{"role": "user", "content": prompt}],
        )
        self._track_usage(response)
        return response.content[0].text.strip()

    # ------------------------------------------------------------------
    # Financial projections (pure computation)
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_financial_projections(
        request: MGAProposalRequest,
        data_package: dict[str, Any],
    ) -> list[FinancialProjection]:
        """Compute 5-year P&L from rate data + assumptions.

        Uses product-specific financial assumptions from PRODUCT_FINANCIALS.
        PMI derives average rate from live DB data; other products use defaults.
        """
        product_type = request.program_type
        fin = PRODUCT_FINANCIALS.get(product_type, PRODUCT_FINANCIALS["pmi"])

        target = request.target_volume
        ramp_pcts = fin["ramp_pcts"]
        loss_ratios = fin["loss_ratios"]
        expense_ratios = fin["expense_ratios"]
        commission_rate = fin["commission_rate"]

        # Derive average premium rate from live data (PMI) or use default
        rate_data = data_package["live_data"]["rate_analysis"].get("by_carrier", {})
        all_avg_rates = []
        for carrier_rates in rate_data.values():
            for band in carrier_rates:
                all_avg_rates.append(band["avg_rate"])
        avg_rate_pct = (
            sum(all_avg_rates) / len(all_avg_rates)
            if all_avg_rates
            else fin["default_avg_rate_pct"]
        )

        projections = []
        cumulative = 0.0

        for i in range(5):
            year = i + 1
            # Insured volume × average premium rate = premium
            insured_volume = target * ramp_pcts[i]
            premium = insured_volume * (avg_rate_pct / 100)

            # MGA revenue = ceding commission on premium
            mga_revenue = premium * commission_rate

            # MGA bears its own expenses (% of revenue)
            expenses = mga_revenue * expense_ratios[i]

            # MGA's share of losses (proportional to commission share)
            # In a QSR/MGA model, MGA typically bears loss on its commission share
            loss_share = premium * loss_ratios[i] * commission_rate

            net_income = mga_revenue - expenses - loss_share
            cumulative += net_income

            projections.append(FinancialProjection(
                year=year,
                premium_volume=round(premium, 0),
                loss_ratio=loss_ratios[i],
                expense_ratio=expense_ratios[i],
                commission_rate=commission_rate,
                net_income=round(net_income, 0),
                cumulative_income=round(cumulative, 0),
            ))

        return projections

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def _persist_proposal(
        self,
        proposal_id: uuid.UUID,
        request: MGAProposalRequest,
        title: str,
        sections: dict[str, ProposalSection],
        projections: list[FinancialProjection],
        token_usage: dict,
    ) -> None:
        """Save the completed proposal to hermes_mga_proposals."""
        proposal_data = {
            "sections": {k: v.model_dump() for k, v in sections.items()},
            "financial_projections": [p.model_dump() for p in projections],
        }

        insert = text("""
            INSERT INTO hermes_mga_proposals
                (id, program_type, title, request_data, proposal_data,
                 status, token_usage, generated_by)
            VALUES
                (:id, :program_type, :title, :request_data, :proposal_data,
                 'complete', :token_usage, :generated_by)
        """)

        async with self._engine.begin() as conn:
            await conn.execute(insert, {
                "id": str(proposal_id),
                "program_type": request.program_type,
                "title": title,
                "request_data": json.dumps(request.model_dump(), default=str),
                "proposal_data": json.dumps(proposal_data, default=str),
                "token_usage": json.dumps(token_usage),
                "generated_by": "mga_proposal_agent_v1",
            })

        logger.info("Proposal %s persisted to DB", proposal_id)

    # ------------------------------------------------------------------
    # Token tracking
    # ------------------------------------------------------------------

    def _track_usage(self, response: Any) -> None:
        """Accumulate token counts from a response."""
        self.total_calls += 1
        if not response.usage:
            return
        inp = getattr(response.usage, "input_tokens", 0)
        out = getattr(response.usage, "output_tokens", 0)
        self.total_input_tokens += inp
        self.total_output_tokens += out
        cost = inp * _INPUT_COST_PER_M / 1_000_000 + out * _OUTPUT_COST_PER_M / 1_000_000
        logger.debug("AI call #%d: in=%d out=%d | $%.4f", self.total_calls, inp, out, cost)

    @property
    def total_cost_usd(self) -> float:
        """Estimated total USD cost across all calls."""
        return (
            self.total_input_tokens * _INPUT_COST_PER_M / 1_000_000
            + self.total_output_tokens * _OUTPUT_COST_PER_M / 1_000_000
        )
