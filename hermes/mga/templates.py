"""Prompt templates and embedded market intelligence for MGA proposal generation.

Market intelligence constants are research-backed figures that get injected
into section prompts alongside live DB data. They are NOT hardcoded into
the prompts themselves — they are structured dicts rendered at call time.
"""

from __future__ import annotations

import json
from typing import Any

# ═══════════════════════════════════════════════════════════════════════════
# EMBEDDED MARKET INTELLIGENCE — research-backed constants
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# PRODUCT CATALOG — binary product opportunities
# ═══════════════════════════════════════════════════════════════════════════

PRODUCT_CATALOG: dict[str, dict[str, Any]] = {
    "pmi": {
        "name": "Private Mortgage Insurance",
        "line": "Mortgage Guarantee",
        "tam_usd": 6_800_000_000,
        "binary_trigger": "Borrower default on mortgage with LTV >80%",
        "serff_status": "Rate filings required per state",
    },
    "title": {
        "name": "Lender's Title Insurance",
        "line": "Title Insurance",
        "tam_usd": 16_400_000_000,
        "binary_trigger": "Title defect or lien discovered post-closing",
        "serff_status": "Promulgated rates in TX; rate filings in other states",
    },
    "surety": {
        "name": "Surety Bonds",
        "line": "Surety",
        "tam_usd": 7_200_000_000,
        "binary_trigger": "Principal fails to perform contractual obligation",
        "serff_status": "Rate filings required",
    },
    "cyber": {
        "name": "Cyber Liability Insurance",
        "line": "Cyber",
        "tam_usd": 14_800_000_000,
        "binary_trigger": "Data breach, ransomware, or network security failure",
        "serff_status": "Rate and form filings required",
    },
    "gap": {
        "name": "Auto GAP Insurance",
        "line": "Auto GAP",
        "tam_usd": 3_200_000_000,
        "binary_trigger": "Vehicle total loss exceeding loan payoff",
        "serff_status": "Rate filings required per state",
    },
    "crime": {
        "name": "Commercial Crime Insurance",
        "line": "Crime / Fidelity",
        "tam_usd": 2_100_000_000,
        "binary_trigger": "Employee theft, fraud, or forgery event",
        "serff_status": "Rate and form filings",
    },
    "eo": {
        "name": "Errors & Omissions Insurance",
        "line": "Professional Liability",
        "tam_usd": 8_500_000_000,
        "binary_trigger": "Professional negligence claim from client",
        "serff_status": "Rate and form filings required",
    },
    "trade_credit": {
        "name": "Trade Credit Insurance",
        "line": "Trade Credit",
        "tam_usd": 11_000_000_000,
        "binary_trigger": "Buyer insolvency or protracted default on trade receivable",
        "serff_status": "Surplus lines in most states",
    },
    "inland_marine": {
        "name": "Inland Marine Insurance",
        "line": "Inland Marine",
        "tam_usd": 4_600_000_000,
        "binary_trigger": "Physical loss/damage to property in transit or at temporary location",
        "serff_status": "Rate filings required; non-filed in some states",
    },
    "builders_risk": {
        "name": "Builders Risk Insurance",
        "line": "Builders Risk",
        "tam_usd": 3_800_000_000,
        "binary_trigger": "Physical loss to structure under construction",
        "serff_status": "Rate and form filings required",
    },
    "student_loan": {
        "name": "Student Loan Default Insurance",
        "line": "Credit Insurance",
        "tam_usd": 1_700_000_000,
        "binary_trigger": "Borrower default on student loan obligation",
        "serff_status": "Rate filings required per state",
    },
    "travel": {
        "name": "Travel Insurance",
        "line": "Travel",
        "tam_usd": 5_300_000_000,
        "binary_trigger": "Trip cancellation, medical emergency, or baggage loss",
        "serff_status": "Rate and form filings required",
    },
    "home_warranty": {
        "name": "Home Warranty / Home Service Contract",
        "line": "Home Warranty",
        "tam_usd": 6_100_000_000,
        "binary_trigger": "Mechanical breakdown of covered home system or appliance",
        "serff_status": "Regulated as service contracts in most states",
    },
}


PMI_MARKET_INTELLIGENCE: dict[str, Any] = {
    "pmi_market": {
        "total_market_size_usd": 6_800_000_000,
        "total_niw_usd": 299_000_000_000,
        "annual_niw_label": "$299B new insurance written (2024)",
        "growth_rate_pct": 8.5,
        "penetration_note": "PMI required on conventional loans with LTV >80%",
    },
    "carrier_landscape": {
        "total_carriers": 6,
        "description": "Oligopoly — 6 carriers control 100% of the market",
        "carriers": {
            "MGIC": {"market_share_pct": 25.2, "ticker": "MTG", "am_best": "A-"},
            "Radian": {"market_share_pct": 20.1, "ticker": "RDN", "am_best": "A-"},
            "Essent": {"market_share_pct": 18.7, "ticker": "ESNT", "am_best": "A"},
            "Enact (Genworth)": {"market_share_pct": 16.4, "ticker": "ACT", "am_best": "A-"},
            "Arch MI": {"market_share_pct": 12.8, "ticker": "ACGL", "am_best": "A+"},
            "NMI": {"market_share_pct": 6.8, "ticker": "NMIH", "am_best": "A-"},
        },
    },
    "loss_history": {
        "post_crisis_combined_ratio": 28,
        "current_loss_ratio_range": "15-25%",
        "historical_peak_loss_ratio": "Over 200% during 2008-2011 crisis",
        "note": "PMI is among the most profitable P&C lines post-2012 reforms",
        "pmiers_impact": "PMIERS capital requirements ($400M+ minimum) raised barriers to entry",
    },
    "capital_markets": {
        "iln_annual_volume_usd": 4_500_000_000,
        "iln_label": "Insurance-linked notes (ILN) — $4.5B annual issuance",
        "qsr_typical_cede_pct": "25-50%",
        "note": "All 6 carriers use ILN + QSR to optimize capital; an MGA would too",
    },
    "better_mortgage": {
        "current_volume_usd": 3_600_000_000,
        "projected_volume_usd": 5_000_000_000,
        "projected_year": 2027,
        "growth_trajectory": "$3.6B → $5B annual origination volume",
        "digital_first": True,
        "note": "Better.com is the #1 digital-first lender, ideal for embedded PMI",
    },
    "tinman_ai": {
        "ai_underwritten_pct": 40,
        "cost_savings_per_loan": 1_400,
        "description": "Tinman AI-underwriting engine: 40% of loans fully AI-underwritten",
        "thesis": "Better risk selection → lower loss ratios → carriers incentivized to partner with MGA that delivers better-performing pools",
    },
    "regulatory": {
        "state_licensing": "MGA must be licensed in each state of operation",
        "pmiers": "Private Mortgage Insurer Eligibility Requirements — $400M minimum capital",
        "gse_requirements": "Fannie Mae / Freddie Mac approve MI providers; MGA operates under carrier's approval",
        "note": "MGA delegates authority from licensed carrier — avoids direct PMIERS requirement",
    },
    "precedent_transactions": {
        "examples": [
            "Arch MI delegated underwriting programs with top-10 lenders",
            "Essent's master policy partnerships with digital lenders",
            "NMI's rate API integrations for embedded PMI pricing",
        ],
        "note": "No pure MGA model exists yet in PMI — this would be the first",
    },
}

# Backward-compat alias (data_collector.py imports this name)
MARKET_INTELLIGENCE = PMI_MARKET_INTELLIGENCE


TITLE_INSURANCE_INTELLIGENCE: dict[str, Any] = {
    "title_market": {
        "total_market_size_usd": 16_400_000_000,
        "annual_premium_label": "$16.4B annual premiums (largest product in catalog)",
        "growth_rate_pct": 4.2,
        "penetration_note": "Lender's title insurance required on virtually all mortgage originations",
        "one_time_premium": True,
        "note": "Title insurance is a one-time premium paid at closing, not recurring like PMI",
    },
    "carrier_landscape": {
        "total_carriers": 4,
        "description": "Oligopoly — 4 carriers control ~85% of the market",
        "carriers": {
            "Fidelity National Financial": {"market_share_pct": 33.0, "ticker": "FNF", "am_best": "A"},
            "First American Financial": {"market_share_pct": 26.0, "ticker": "FAF", "am_best": "A"},
            "Old Republic International": {"market_share_pct": 15.0, "ticker": "ORI", "am_best": "A+"},
            "Stewart Information Services": {"market_share_pct": 11.0, "ticker": "STC", "am_best": "A-"},
        },
    },
    "loss_history": {
        "historical_loss_ratio": "~5%",
        "current_loss_ratio_range": "3-7%",
        "note": "Title insurance is among the most profitable P&C lines — ~5% loss ratio vs 60%+ for most lines",
        "claims_nature": "Many title claims are 'cured' (defect resolved) rather than paid — unique to title",
        "expense_ratio_note": "High expense ratio (~85%) due to title search/examination costs; MGA AI automation reduces this dramatically",
    },
    "distribution": {
        "primary_channel": "Embedded at mortgage closing via lender partnerships",
        "secondary_channels": ["Real estate attorney referrals", "Real estate agent referrals", "Direct-to-consumer (refinance)"],
        "embedded_thesis": "Title insurance is a natural embedded product — ordered by lender, paid by borrower at closing",
        "note": "Same data flow as mortgage underwriting — property data, lien searches, public records",
    },
    "regulatory": {
        "state_licensing": "Title insurance agent/underwriter licensing required per state",
        "serff_filing": "Promulgated rates in TX (state-set); rate filings in most other states",
        "unique_regulation": "Title insurance is regulated differently from P&C — separate title insurance code in most states",
        "escrow_requirements": "Title agents must maintain escrow/trust accounts for closing funds",
        "note": "MGA operates under carrier's underwriting authority — avoids direct underwriter licensing requirement",
    },
    "ai_opportunity": {
        "ai_automated_pct": 85,
        "description": "85%+ of standard residential titles can be AI-cleared using public records and prior title history",
        "data_signals": ["County recorder filings", "Tax lien databases", "UCC filings", "Court records", "Prior title searches"],
        "cost_savings_per_policy": 800,
        "thesis": "AI-powered title search replaces manual abstracting — faster, cheaper, more accurate defect detection",
    },
    "capital_markets": {
        "reinsurance_market": "Limited but growing — title reinsurance is niche",
        "note": "Low loss ratios mean less reinsurance need; excess-of-loss for large commercial policies",
    },
    "precedent_transactions": {
        "examples": [
            "States Title (AI-powered title) acquired by Blend for embedded title",
            "Doma Holdings (formerly North American Title) — AI title underwriting platform",
            "Endpoint (Fidelity subsidiary) — digital title and closing platform",
            "Qualia — title and closing platform used by 500+ title companies",
        ],
        "note": "Multiple successful AI-title plays validate the embedded MGA model",
    },
}


# ═══════════════════════════════════════════════════════════════════════════
# PROMPT TEMPLATES — one function per proposal section
# ═══════════════════════════════════════════════════════════════════════════


def _fmt(data: dict) -> str:
    """Compact JSON representation for prompt injection."""
    return json.dumps(data, indent=2, default=str)


def executive_summary_prompt(data: dict[str, Any]) -> str:
    return f"""\
You are a senior insurance strategy consultant writing a PMI MGA business proposal.

Generate a compelling executive summary (2-3 paragraphs) for the first-ever Private Mortgage Insurance MGA.

KEY DATA:
- Distribution partner: {data['request']['distribution_partner']} with {data['market_intelligence']['better_mortgage']['growth_trajectory']} volume
- Target volume: ${data['request']['target_volume']:,.0f}
- PMI market: ${data['market_intelligence']['pmi_market']['total_market_size_usd']/1e9:.1f}B annual premiums, {data['market_intelligence']['pmi_market']['total_niw_usd']/1e9:.0f}B NIW
- Carrier landscape: {data['market_intelligence']['carrier_landscape']['total_carriers']}-carrier oligopoly
- AI edge: {data['market_intelligence']['tinman_ai']['description']}
- Carriers in DB: {len(data['live_data']['carriers'])} carriers with {data['live_data']['rate_analysis']['total_rates_in_db']} rate data points

LIVE RATE DATA (sample quotes):
{_fmt(data['live_data']['sample_quotes'])}

Write a professional, data-driven executive summary that:
1. Opens with the opportunity (first PMI MGA + digital distribution + AI underwriting)
2. Quantifies the market opportunity with specific numbers
3. Closes with the competitive moat (embedded distribution + better risk selection)

Return ONLY the executive summary text (no headers, no JSON). Write in a persuasive but factual tone."""


def market_analysis_prompt(data: dict[str, Any]) -> str:
    return f"""\
You are a senior insurance analyst writing the Market Analysis section of a PMI MGA proposal.

MARKET DATA:
{_fmt(data['market_intelligence']['pmi_market'])}

CARRIER LANDSCAPE:
{_fmt(data['market_intelligence']['carrier_landscape'])}

LOSS HISTORY:
{_fmt(data['market_intelligence']['loss_history'])}

LIVE CARRIER DATA FROM OUR DATABASE:
{_fmt(data['live_data']['carriers'])}

RATE ANALYSIS (rates per LTV band across all carriers):
{_fmt(data['live_data']['rate_analysis'])}

Write a comprehensive market analysis section covering:
1. Market size and growth ($6.8B premiums, $299B NIW, growth drivers)
2. The 6-carrier oligopoly structure with market shares
3. Post-crisis profitability (28% combined ratio — among best in P&C)
4. Why the market structure creates an MGA opportunity
5. Rate variation across carriers (use the live rate data)

Use specific numbers from the data above. Format with clear subheadings.
Return ONLY the section text (no JSON)."""


def pricing_analysis_prompt(data: dict[str, Any]) -> str:
    return f"""\
You are a PMI pricing actuary writing the Pricing Analysis section.

LIVE RATE DATA BY CARRIER:
{_fmt(data['live_data']['rate_analysis'])}

SAMPLE MULTI-CARRIER QUOTES:
{_fmt(data['live_data']['sample_quotes'])}

COMPETITIVE SPREAD ANALYSIS (largest spreads between cheapest and most expensive):
{_fmt(data['live_data']['competitive_spread'][:10])}

Write a pricing analysis section that:
1. Summarizes the rate landscape across all 6 carriers
2. Shows where the biggest pricing spreads exist (LTV/FICO tiers)
3. Presents the sample quote comparisons with specific dollar amounts
4. Identifies pricing arbitrage opportunities for an MGA
5. Explains how an MGA earns margin (ceding commission from carrier)

Use specific rates and dollar amounts from the live data.
Return ONLY the section text (no JSON)."""


def carrier_strategy_prompt(data: dict[str, Any]) -> str:
    return f"""\
You are an insurance M&A advisor writing the Carrier Partnership Strategy section.

CARRIER PROFILES:
{_fmt(data['live_data']['carriers'])}

CARRIER MARKET SHARES:
{_fmt(data['market_intelligence']['carrier_landscape']['carriers'])}

RATE DATA:
{_fmt(data['live_data']['rate_analysis'])}

PRECEDENT TRANSACTIONS:
{_fmt(data['market_intelligence']['precedent_transactions'])}

TARGET VOLUME: ${data['request']['target_volume']:,.0f} via {data['request']['distribution_partner']}

Write a carrier partnership strategy that:
1. Recommends 2-3 carriers to approach first and explains why (consider market share, rating, pricing competitiveness, growth appetite)
2. Outlines the negotiation approach (what we offer: volume, AI-selected risk, digital integration)
3. Describes the delegated authority structure (MGA binds coverage under carrier paper)
4. Addresses potential carrier objections and counter-arguments
5. Proposes a phased carrier expansion plan

Return ONLY the section text (no JSON)."""


def reinsurance_strategy_prompt(data: dict[str, Any]) -> str:
    return f"""\
You are a reinsurance strategist writing the Reinsurance & Capital Strategy section.

CAPITAL MARKETS DATA:
{_fmt(data['market_intelligence']['capital_markets'])}

REGULATORY CONTEXT:
{_fmt(data['market_intelligence']['regulatory'])}

LOSS HISTORY:
{_fmt(data['market_intelligence']['loss_history'])}

TARGET VOLUME: ${data['request']['target_volume']:,.0f}

Write a reinsurance strategy section covering:
1. Proposed quota share reinsurance (QSR) structure — 25-40% cession
2. Insurance-linked notes (ILN) path for capital efficiency
3. How the MGA layers its capital stack vs. traditional carriers
4. Capital relief benefits for the fronting carrier
5. Precedent ILN/QSR transactions in PMI

Return ONLY the section text (no JSON)."""


def tinman_advantage_prompt(data: dict[str, Any]) -> str:
    return f"""\
You are a technology strategist writing the AI Underwriting Advantage section.

TINMAN AI DATA:
{_fmt(data['market_intelligence']['tinman_ai'])}

LOSS HISTORY (baseline):
{_fmt(data['market_intelligence']['loss_history'])}

LIVE RATE DATA:
{_fmt(data['live_data']['rate_analysis'])}

Write a section on the Tinman AI underwriting advantage:
1. How AI-powered risk selection (40% of loans fully AI-underwritten) creates an MGA moat
2. The thesis: better selection → lower loss ratios → more attractive to carriers
3. Quantify the potential: if AI reduces loss ratio by even 5 points on a ${data['request']['target_volume']:,.0f} book, the value creation
4. How this differentiates from other MGA/MGU models
5. Data flywheel: more loans → better models → lower losses → more carrier appetite

Return ONLY the section text (no JSON)."""


def distribution_strategy_prompt(data: dict[str, Any]) -> str:
    return f"""\
You are a distribution strategy consultant writing the Distribution Strategy section.

BETTER MORTGAGE DATA:
{_fmt(data['market_intelligence']['better_mortgage'])}

TINMAN AI:
{_fmt(data['market_intelligence']['tinman_ai'])}

TARGET STATES: {', '.join(data['request']['target_states'])}
TARGET VOLUME: ${data['request']['target_volume']:,.0f}

Write a distribution strategy covering:
1. Phase 1: Embedded PMI within {data['request']['distribution_partner']} origination flow (captive volume)
2. Phase 2: Tinman AI licensing to other digital lenders (platform expansion)
3. Phase 3: Broker and correspondent channels
4. 5-year channel expansion roadmap with volume targets
5. Technology integration model (API-first, real-time pricing)

Return ONLY the section text (no JSON)."""


def financial_projections_prompt(data: dict[str, Any], projections: list[dict]) -> str:
    return f"""\
You are a financial analyst writing the Financial Projections narrative section.

5-YEAR FINANCIAL MODEL:
{_fmt(projections)}

MARKET CONTEXT:
- PMI market: ${data['market_intelligence']['pmi_market']['total_market_size_usd']/1e9:.1f}B
- Average loss ratio: {data['market_intelligence']['loss_history']['current_loss_ratio_range']}
- Target volume: ${data['request']['target_volume']:,.0f}

Write a financial projections narrative that:
1. Walks through the 5-year P&L summary with key numbers
2. Explains the assumptions (loss ratio, expense ratio, commission rates)
3. Identifies the breakeven point and path to profitability
4. Discusses sensitivity analysis (what if loss ratios are 5 points higher?)
5. Compares unit economics to traditional MI carriers

Use the specific numbers from the financial model above.
Return ONLY the section text (no JSON)."""


def go_to_market_prompt(data: dict[str, Any]) -> str:
    return f"""\
You are a program manager writing the Go-to-Market Plan section.

REGULATORY CONTEXT:
{_fmt(data['market_intelligence']['regulatory'])}

TARGET STATES: {', '.join(data['request']['target_states'])}
DISTRIBUTION PARTNER: {data['request']['distribution_partner']}

Write a 4-phase go-to-market plan:

Phase 1 (Months 1-3): Foundation
- Carrier LOI / delegated authority agreement
- State licensing in target states
- Technology integration planning

Phase 2 (Months 4-6): Pilot
- Limited launch in 1-2 states
- Rate API integration with {data['request']['distribution_partner']}
- Performance monitoring framework

Phase 3 (Months 7-12): Scale
- Expand to all target states
- Full volume ramp
- Second carrier partnership

Phase 4 (Year 2+): Platform
- Third-party lender distribution
- Additional carrier panels
- Product expansion

Include specific milestones and success criteria for each phase.
Return ONLY the section text (no JSON)."""


def risk_factors_prompt(data: dict[str, Any]) -> str:
    return f"""\
You are a risk management consultant writing the Risk Factors section.

REGULATORY:
{_fmt(data['market_intelligence']['regulatory'])}

MARKET:
{_fmt(data['market_intelligence']['pmi_market'])}

Write a balanced risk factors section covering:
1. Regulatory risk — state licensing, PMIERS evolution, GSE requirement changes
2. Market risk — housing downturn, interest rate changes, NIW volume decline
3. Concentration risk — single-carrier dependency, single-distribution-partner risk
4. Technology risk — AI model degradation, data quality, integration complexity
5. Competitive risk — carriers building direct digital channels, other MGA entrants

For EACH risk, include:
- Risk description
- Likelihood (Low/Medium/High)
- Impact (Low/Medium/High)
- Mitigation strategy

Return ONLY the section text (no JSON)."""


# ═══════════════════════════════════════════════════════════════════════════
# TITLE INSURANCE — section prompt overrides
# ═══════════════════════════════════════════════════════════════════════════


def title_executive_summary_prompt(data: dict[str, Any]) -> str:
    mi = data["market_intelligence"]
    partner = data["request"].get("distribution_partner") or "a leading mortgage originator"
    return f"""\
You are a senior insurance strategy consultant writing an investor-grade MGA business proposal.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

Generate a compelling executive summary (3-4 paragraphs) for a Lender's Title Insurance MGA.

KEY DATA:
- Distribution partner: {partner}
- Target volume: ${data['request']['target_volume']:,.0f} annual origination
- Title insurance market: ${mi['title_market']['total_market_size_usd']/1e9:.1f}B annual premiums — largest addressable market in the product catalog
- Carrier landscape: {mi['carrier_landscape']['total_carriers']}-carrier oligopoly ({mi['carrier_landscape']['description']})
- Binary trigger: {PRODUCT_CATALOG['title']['binary_trigger']}
- Loss ratio: {mi['loss_history']['historical_loss_ratio']} — among the most profitable P&C lines
- AI edge: {mi['ai_opportunity']['description']}
- Distribution: {mi['distribution']['primary_channel']}

CARRIER LANDSCAPE:
{_fmt(mi['carrier_landscape']['carriers'])}

Write a professional, data-driven executive summary that:
1. Opens with the massive market opportunity ($16.4B TAM, binary product, embedded distribution)
2. Explains the MGA model: delegated underwriting under carrier paper with AI-powered title search
3. Quantifies unit economics: ~5% loss ratio + AI automation = dramatically lower expense ratios
4. Closes with the competitive moat: embedded at point of mortgage closing + AI title clearance

Return ONLY the executive summary text (no headers, no JSON). Write in a persuasive but factual tone."""


def title_market_analysis_prompt(data: dict[str, Any]) -> str:
    mi = data["market_intelligence"]
    return f"""\
You are a senior insurance analyst writing the Market Analysis section of a title insurance MGA proposal.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

MARKET DATA:
{_fmt(mi['title_market'])}

CARRIER LANDSCAPE:
{_fmt(mi['carrier_landscape'])}

LOSS HISTORY:
{_fmt(mi['loss_history'])}

DISTRIBUTION CHANNELS:
{_fmt(mi['distribution'])}

Write a comprehensive market analysis section covering:
1. Market size and growth ($16.4B premiums, one-time premium model, growth drivers tied to mortgage origination)
2. The 4-carrier oligopoly structure with market shares (Fidelity 33%, First American 26%, Old Republic 15%, Stewart 11%)
3. Profitability: ~5% loss ratio makes title insurance among the most profitable P&C lines
4. Expense ratio problem: ~85% expense ratio from manual title search — this is the AI disruption opportunity
5. Why the market structure creates an MGA opportunity (concentrated carriers + high expense ratios + embedded distribution)

Use specific numbers from the data above. Format with clear subheadings.
Return ONLY the section text (no JSON)."""


def title_pricing_analysis_prompt(data: dict[str, Any]) -> str:
    mi = data["market_intelligence"]
    return f"""\
You are a title insurance pricing analyst writing the Pricing Analysis section.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

MARKET DATA:
{_fmt(mi['title_market'])}

REGULATORY CONTEXT (rate filing):
{_fmt(mi['regulatory'])}

AI COST REDUCTION:
{_fmt(mi['ai_opportunity'])}

TARGET STATES: {', '.join(data['request']['target_states'])}

Write a pricing analysis section that:
1. Explains the title insurance premium structure (one-time premium at closing, based on loan amount)
2. Covers state-by-state rate variation — promulgated rates in TX vs filed rates elsewhere
3. Analyzes the expense ratio opportunity: traditional ~85% expense ratio → AI-powered ~45% target
4. Shows the MGA margin model: carrier keeps loss reserves (~5%), MGA earns commission on expense savings
5. Compares per-policy economics: traditional title search ($400-800) vs AI-powered ($50-150)
6. Identifies the highest-margin states based on premium rates and regulatory structure

Note: Live SERFF rate data not yet available for title — use market intelligence figures.
Return ONLY the section text (no JSON)."""


def underwriting_strategy_prompt(data: dict[str, Any]) -> str:
    """Section 4 — Underwriting Strategy (replaces tinman_advantage for non-PMI products)."""
    mi = data["market_intelligence"]
    product_key = data["request"]["program_type"]
    product = PRODUCT_CATALOG.get(product_key, PRODUCT_CATALOG["title"])
    return f"""\
You are a chief underwriting officer writing the Underwriting Strategy section of a {product['name']} MGA proposal.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

PRODUCT:
- Name: {product['name']}
- Binary trigger: {product['binary_trigger']}
- TAM: ${product['tam_usd']/1e9:.1f}B

AI UNDERWRITING CAPABILITY:
{_fmt(mi['ai_opportunity'])}

LOSS HISTORY:
{_fmt(mi['loss_history'])}

Write a comprehensive underwriting strategy section covering:
1. Binary trigger analysis: {product['binary_trigger']} — what constitutes a covered event
2. Data signals for risk assessment: {_fmt(mi['ai_opportunity']['data_signals'])}
3. AI-powered underwriting: {mi['ai_opportunity']['description']}
4. Automation rate target: {mi['ai_opportunity']['ai_automated_pct']}%+ of standard cases fully AI-cleared
5. Risk tiering: how AI classifies applications into risk tiers with different pricing
6. Human-in-the-loop: complex cases flagged for expert review (the remaining {100 - mi['ai_opportunity']['ai_automated_pct']}%)
7. How superior risk selection creates a moat: better data → lower losses → more carrier appetite

Return ONLY the section text (no JSON)."""


def claims_philosophy_prompt(data: dict[str, Any]) -> str:
    """Section 7 — Claims Philosophy & Management (new section)."""
    mi = data["market_intelligence"]
    product_key = data["request"]["program_type"]
    product = PRODUCT_CATALOG.get(product_key, PRODUCT_CATALOG["title"])
    return f"""\
You are a claims management executive writing the Claims Philosophy & Management section of a {product['name']} MGA proposal.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

PRODUCT:
- Name: {product['name']}
- Binary trigger: {product['binary_trigger']}

LOSS HISTORY:
{_fmt(mi['loss_history'])}

Write a comprehensive claims philosophy section covering:
1. Triggering event: {product['binary_trigger']} — precise definition of what constitutes a valid claim
2. Claims triage: AI-powered initial classification and severity assessment
3. For title insurance specifically: curative action vs. payment — many title claims are "cured" (defect resolved legally) rather than paid out, which is unique to title
4. Frequency/severity analysis by risk tier
5. Subrogation opportunities: recovery from prior title agents, abstractors, or responsible parties
6. Claims cost management: target handling cost per claim and automation of routine claims
7. Reserving philosophy: conservative IBNR (incurred but not reported) reserves given ~5% loss ratio
8. Dispute resolution: escalation pathway and litigation management

Return ONLY the section text (no JSON)."""


def title_carrier_strategy_prompt(data: dict[str, Any]) -> str:
    mi = data["market_intelligence"]
    partner = data["request"].get("distribution_partner") or "a leading mortgage originator"
    return f"""\
You are an insurance M&A advisor writing the Carrier Partnership Strategy section for a title insurance MGA.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

CARRIER LANDSCAPE:
{_fmt(mi['carrier_landscape'])}

PRECEDENT TRANSACTIONS:
{_fmt(mi['precedent_transactions'])}

TARGET VOLUME: ${data['request']['target_volume']:,.0f} via {partner}

Write a carrier partnership strategy that:
1. Recommends 2-3 title carriers to approach first and explains why (consider market share, AM Best rating, technology appetite, growth strategy)
2. Outlines the value proposition to carriers: high-volume embedded distribution + AI-cleared titles with lower defect rates
3. Describes the delegated authority structure (MGA issues title commitments and policies under carrier's underwriting authority)
4. Addresses carrier objections: why would Fidelity/First American let an MGA into their oligopoly?
5. Proposes phased carrier expansion plan with volume milestones
6. Explains the reinsurance/excess arrangement for large commercial policies

Return ONLY the section text (no JSON)."""


def title_reinsurance_strategy_prompt(data: dict[str, Any]) -> str:
    mi = data["market_intelligence"]
    return f"""\
You are a reinsurance strategist writing the Reinsurance & Capital Strategy section for a title insurance MGA.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

CAPITAL MARKETS:
{_fmt(mi['capital_markets'])}

REGULATORY:
{_fmt(mi['regulatory'])}

LOSS HISTORY:
{_fmt(mi['loss_history'])}

TARGET VOLUME: ${data['request']['target_volume']:,.0f}

Write a reinsurance and capital strategy section covering:
1. Title insurance capital requirements: lower than P&C due to ~5% loss ratios, but statutory reserves required
2. Reinsurance structure: excess-of-loss for large commercial transactions (>$5M policy limits)
3. Quota share potential: limited need given low loss ratios, but useful for capital efficiency
4. MGA capital-light model: carrier bears underwriting risk, MGA earns commission — minimal capital at risk
5. Reserve management: IBNR reserves for title, statutory premium reserves per state requirements
6. Capital efficiency comparison vs. traditional title underwriters

Return ONLY the section text (no JSON)."""


def title_distribution_strategy_prompt(data: dict[str, Any]) -> str:
    mi = data["market_intelligence"]
    partner = data["request"].get("distribution_partner") or "a leading mortgage originator"
    return f"""\
You are a distribution strategy consultant writing the Distribution Strategy section for a title insurance MGA.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

DISTRIBUTION DATA:
{_fmt(mi['distribution'])}

AI CAPABILITY:
{_fmt(mi['ai_opportunity'])}

TARGET STATES: {', '.join(data['request']['target_states'])}
TARGET VOLUME: ${data['request']['target_volume']:,.0f}

Write a distribution strategy covering:
1. Phase 1: Embedded title insurance within {partner} mortgage origination flow — title ordered automatically at application, AI-cleared, policy issued at closing
2. Phase 2: Expand to additional mortgage originators and correspondent lenders
3. Phase 3: Real estate attorney and agent referral networks
4. Phase 4: Direct-to-consumer for refinance transactions
5. Technology integration: API-first title ordering, automated title search, digital closing integration
6. State-by-state rollout strategy based on regulatory environment (TX promulgated rates first vs. competitive-rate states)

Return ONLY the section text (no JSON)."""


def product_market_fit_prompt(data: dict[str, Any]) -> str:
    """Section 9 — Product-Market Fit (new section)."""
    mi = data["market_intelligence"]
    product_key = data["request"]["program_type"]
    product = PRODUCT_CATALOG.get(product_key, PRODUCT_CATALOG["title"])
    partner = data["request"].get("distribution_partner") or "a leading mortgage originator"
    return f"""\
You are a venture strategist writing the Product-Market Fit section of a {product['name']} MGA proposal.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

PRODUCT: {product['name']}
TAM: ${product['tam_usd']/1e9:.1f}B
BINARY TRIGGER: {product['binary_trigger']}
DISTRIBUTION PARTNER: {partner}

DISTRIBUTION DATA:
{_fmt(mi['distribution'])}

PRECEDENT TRANSACTIONS:
{_fmt(mi['precedent_transactions'])}

Write a product-market fit analysis covering:
1. Why {product['name']} belongs embedded in the mortgage origination workflow — same data, same closing, same customer
2. Customer pain points: title search delays (avg 5-10 business days), cost opacity, closing timeline risk
3. Natural extension thesis: title insurance uses the same property data flow as mortgage underwriting — property records, lien searches, tax data
4. Comparable embedded deployments and exits: States Title (acquired by Blend), Doma (AI title platform), Endpoint (Fidelity digital subsidiary)
5. TAM capture strategy: ${product['tam_usd']/1e9:.1f}B market, target X% in 5 years through embedded distribution
6. Flywheel effect: more transactions → better AI models → faster clearance → more lender partnerships → more transactions

Return ONLY the section text (no JSON)."""


def title_financial_projections_prompt(data: dict[str, Any], projections: list[dict]) -> str:
    mi = data["market_intelligence"]
    return f"""\
You are a financial analyst writing the Financial Projections narrative section for a title insurance MGA.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

5-YEAR FINANCIAL MODEL:
{_fmt(projections)}

MARKET CONTEXT:
- Title insurance market: ${mi['title_market']['total_market_size_usd']/1e9:.1f}B
- Loss ratio: {mi['loss_history']['historical_loss_ratio']}
- Target volume: ${data['request']['target_volume']:,.0f}
- AI cost savings per policy: ${mi['ai_opportunity']['cost_savings_per_policy']:,}

Write a financial projections narrative that:
1. Walks through the 5-year P&L summary with key numbers from the model
2. Explains the title-specific assumptions: ~5% loss ratio, one-time premium, AI-reduced expense ratio
3. Identifies breakeven point and path to profitability
4. Sensitivity analysis: what if loss ratios double to 10%? Still dramatically profitable vs other P&C lines
5. Compares unit economics to traditional title underwriters (our expense ratio advantage)
6. Revenue model: commission on premium + fee income from title search/closing services

Use the specific numbers from the financial model above.
Return ONLY the section text (no JSON)."""


def title_go_to_market_prompt(data: dict[str, Any]) -> str:
    mi = data["market_intelligence"]
    partner = data["request"].get("distribution_partner") or "a leading mortgage originator"
    return f"""\
You are a program manager writing the Go-to-Market Plan for a title insurance MGA.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

REGULATORY:
{_fmt(mi['regulatory'])}

TARGET STATES: {', '.join(data['request']['target_states'])}
DISTRIBUTION PARTNER: {partner}

Write a 4-phase go-to-market plan:

Phase 1 (Months 1-3): Foundation
- Title insurance agent licensing in target states
- Carrier delegated authority agreement (approach Old Republic or Stewart first — more open to MGA model)
- AI title search engine build: county recorder API integrations, lien database feeds
- Escrow/trust account setup per state requirements

Phase 2 (Months 4-6): Pilot
- Limited launch in 1-2 states (recommend TX for promulgated rates + FL for volume)
- Integration with {partner} loan origination system
- AI title clearance on first 500-1,000 transactions
- Performance monitoring: clearance time, defect detection accuracy, customer satisfaction

Phase 3 (Months 7-12): Scale
- Expand to all target states: {', '.join(data['request']['target_states'])}
- Full volume ramp with primary carrier partner
- Second carrier partnership for redundancy
- Direct title agent licensing in high-volume states

Phase 4 (Year 2+): Platform
- Additional mortgage originator partnerships
- Real estate attorney/agent referral channel
- Commercial title insurance product launch
- Title search API licensing to third-party title companies

Include specific milestones and success criteria for each phase.
Return ONLY the section text (no JSON)."""


def title_risk_factors_prompt(data: dict[str, Any]) -> str:
    mi = data["market_intelligence"]
    return f"""\
You are a risk management consultant writing the Risk Factors section for a title insurance MGA.
Quality bar: Goldman Sachs memo quality — precise, data-driven, no filler.

REGULATORY:
{_fmt(mi['regulatory'])}

MARKET:
{_fmt(mi['title_market'])}

Write a balanced risk factors section covering:
1. Regulatory risk — state-specific title insurance licensing, promulgated rate changes (TX), escrow account requirements
2. Market risk — housing downturn reducing origination volume, interest rate changes, refinance wave sensitivity
3. Concentration risk — single-carrier dependency, single-distribution-partner risk, geographic concentration
4. Technology risk — AI title search accuracy, county recorder data quality variations by jurisdiction, edge cases in complex titles
5. Competitive risk — incumbent carriers (Fidelity, First American) building direct digital channels, other AI title startups
6. Operational risk — escrow fund management, closing delays, E&O liability on title searches

For EACH risk, include:
- Risk description
- Likelihood (Low/Medium/High)
- Impact (Low/Medium/High)
- Mitigation strategy

Return ONLY the section text (no JSON)."""


# ═══════════════════════════════════════════════════════════════════════════
# Section registries — maps section keys to their prompt functions
# ═══════════════════════════════════════════════════════════════════════════

# PMI section prompts (original — unchanged)
SECTION_PROMPTS: dict[str, Any] = {
    "executive_summary": executive_summary_prompt,
    "market_analysis": market_analysis_prompt,
    "pricing_analysis": pricing_analysis_prompt,
    "carrier_strategy": carrier_strategy_prompt,
    "reinsurance_strategy": reinsurance_strategy_prompt,
    "tinman_advantage": tinman_advantage_prompt,
    "distribution_strategy": distribution_strategy_prompt,
    # financial_projections uses a special prompt with projections data
    "go_to_market": go_to_market_prompt,
    "risk_factors": risk_factors_prompt,
}

# Title Insurance section prompts (12-section format)
TITLE_SECTION_PROMPTS: dict[str, Any] = {
    "executive_summary": title_executive_summary_prompt,
    "market_analysis": title_market_analysis_prompt,
    "pricing_analysis": title_pricing_analysis_prompt,
    "underwriting_strategy": underwriting_strategy_prompt,
    "carrier_strategy": title_carrier_strategy_prompt,
    "reinsurance_strategy": title_reinsurance_strategy_prompt,
    "claims_philosophy": claims_philosophy_prompt,
    "distribution_strategy": title_distribution_strategy_prompt,
    "product_market_fit": product_market_fit_prompt,
    # financial_projections uses a special prompt with projections data
    "go_to_market": title_go_to_market_prompt,
    "risk_factors": title_risk_factors_prompt,
}

# Product → prompts dispatcher
SECTION_PROMPTS_BY_PRODUCT: dict[str, dict[str, Any]] = {
    "pmi": SECTION_PROMPTS,
    "title": TITLE_SECTION_PROMPTS,
}

# Product → financial_projections prompt dispatcher
FINANCIAL_PROMPTS_BY_PRODUCT: dict[str, Any] = {
    "pmi": financial_projections_prompt,
    "title": title_financial_projections_prompt,
}

# PMI section order (10 sections — unchanged)
PMI_SECTION_ORDER: list[str] = [
    "executive_summary",
    "market_analysis",
    "pricing_analysis",
    "carrier_strategy",
    "reinsurance_strategy",
    "tinman_advantage",
    "distribution_strategy",
    "go_to_market",
    "risk_factors",
]

# Title Insurance section order (12 sections)
TITLE_SECTION_ORDER: list[str] = [
    "executive_summary",
    "market_analysis",
    "pricing_analysis",
    "underwriting_strategy",
    "carrier_strategy",
    "reinsurance_strategy",
    "claims_philosophy",
    "distribution_strategy",
    "product_market_fit",
    "go_to_market",
    "risk_factors",
]

# Product → section order dispatcher
SECTION_ORDER_BY_PRODUCT: dict[str, list[str]] = {
    "pmi": PMI_SECTION_ORDER,
    "title": TITLE_SECTION_ORDER,
}

SECTION_TITLES: dict[str, str] = {
    "executive_summary": "Executive Summary",
    "market_analysis": "Market Analysis",
    "pricing_analysis": "Pricing & Rate Analysis",
    "underwriting_strategy": "Underwriting Strategy",
    "carrier_strategy": "Carrier Partnership Strategy",
    "reinsurance_strategy": "Reinsurance & Capital Strategy",
    "claims_philosophy": "Claims Philosophy & Management",
    "tinman_advantage": "AI Underwriting Advantage (Tinman)",
    "distribution_strategy": "Distribution Strategy",
    "product_market_fit": "Product-Market Fit",
    "financial_projections": "Financial Projections",
    "go_to_market": "Go-to-Market Plan",
    "risk_factors": "Risk Factors & Mitigations",
}
