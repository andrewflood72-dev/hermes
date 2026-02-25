"""Generate a PMI MGA business proposal using the MGA Proposal Agent.

Usage:
    python scripts/generate_pmi_proposal.py
"""

import asyncio
import logging
import sys

from sqlalchemy.ext.asyncio import create_async_engine

from hermes.config import settings
from hermes.mga.agent import MGAProposalAgent
from hermes.mga.schemas import MGAProposalRequest
from hermes.pmi.engine import HermesPMIEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("generate_proposal")


async def main() -> None:
    engine = create_async_engine(settings.database_url, pool_size=5)
    pmi = HermesPMIEngine(db_engine=engine)
    agent = MGAProposalAgent(db_engine=engine, pmi_engine=pmi)

    request = MGAProposalRequest(
        program_type="pmi",
        target_volume=5_000_000_000,
        distribution_partner="Better Mortgage",
        target_states=["CA", "TX", "FL", "NY", "IL"],
    )

    logger.info("Starting PMI MGA proposal generation...")
    proposal = await agent.generate_proposal(request)

    # Print the full proposal
    print("\n" + "=" * 80)
    print(f"  {proposal.title}")
    print(f"  Status: {proposal.status} | ID: {proposal.id}")
    print(f"  Generated: {proposal.generated_at}")
    print("=" * 80)

    # Print each section
    section_order = [
        "executive_summary",
        "market_analysis",
        "pricing_analysis",
        "carrier_strategy",
        "reinsurance_strategy",
        "tinman_advantage",
        "distribution_strategy",
        "financial_projections",
        "go_to_market",
        "risk_factors",
    ]

    for key in section_order:
        section = proposal.sections.get(key)
        if not section:
            continue
        print(f"\n{'─' * 80}")
        print(f"  {section.title}")
        print(f"{'─' * 80}\n")
        print(section.content)

    # Financial projections table
    if proposal.financial_projections:
        print(f"\n{'─' * 80}")
        print("  5-Year Financial Projections (Computed)")
        print(f"{'─' * 80}\n")
        print(f"  {'Year':<6} {'Premium Volume':>16} {'Loss Ratio':>12} {'Expense Ratio':>15} {'Net Income':>14} {'Cumulative':>14}")
        print(f"  {'─' * 6} {'─' * 16} {'─' * 12} {'─' * 15} {'─' * 14} {'─' * 14}")
        for p in proposal.financial_projections:
            print(f"  {p.year:<6} ${p.premium_volume:>14,.0f} {p.loss_ratio:>11.0%} {p.expense_ratio:>14.0%} ${p.net_income:>12,.0f} ${p.cumulative_income:>12,.0f}")

    # Token usage summary
    print(f"\n{'=' * 80}")
    print("  Token Usage Summary")
    print(f"{'=' * 80}")
    usage = proposal.token_usage
    print(f"  Input tokens:  {usage.get('input_tokens', 0):,}")
    print(f"  Output tokens: {usage.get('output_tokens', 0):,}")
    print(f"  API calls:     {usage.get('total_calls', 0)}")
    print(f"  Est. cost:     ${usage.get('estimated_cost_usd', 0):.4f}")
    print(f"  Gen. time:     {usage.get('generation_time_s', 0):.1f}s")
    print(f"  Proposal ID:   {proposal.id}")
    print("=" * 80)

    await pmi.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
