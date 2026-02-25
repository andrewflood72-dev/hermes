"""Hermes FastAPI application — carrier-risk matching and market intelligence API.

Endpoints
---------
POST  /v1/match                          — run carrier-risk matching
GET   /v1/carriers/{naic_code}/appetite  — carrier appetite for a state/line
GET   /v1/rates/{state}/{line}/{class_code} — rate comparison across carriers
GET   /v1/filings                        — search SERFF filings
GET   /v1/market-intelligence            — market trend statistics
GET   /v1/health                         — system health check

Authentication is via the ``X-API-Key`` header.  Rate limiting enforces a
maximum of 100 requests per minute per API key using an in-memory sliding
window counter.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hermes import __version__
from hermes.api.schemas import (
    AppetiteResponse,
    CarrierMatchResponse,
    FilingEntry,
    FilingSearchResponse,
    HealthResponse,
    MarketIntelResponse,
    MatchResponse,
    RateComparisonEntry,
    RateComparisonResponse,
    RiskProfileInput,
)
from hermes.config import settings
from hermes.matching.engine import CarrierMatchResult, MatchingEngine

logger = logging.getLogger("hermes.api")

# ---------------------------------------------------------------------------
# In-memory rate limiter
# ---------------------------------------------------------------------------

# Maps api_key → list of request timestamps (Unix seconds, float)
_rate_limit_windows: dict[str, list[float]] = defaultdict(list)

_RATE_LIMIT_MAX = 100       # requests
_RATE_LIMIT_WINDOW = 60.0   # seconds


def _check_rate_limit(api_key: str) -> None:
    """Enforce 100 requests / 60-second sliding window per API key.

    Raises HTTP 429 when the limit is exceeded.
    """
    now = time.monotonic()
    window = _rate_limit_windows[api_key]
    # Evict timestamps outside the window
    cutoff = now - _RATE_LIMIT_WINDOW
    _rate_limit_windows[api_key] = [t for t in window if t > cutoff]
    if len(_rate_limit_windows[api_key]) >= _RATE_LIMIT_MAX:
        logger.warning("Rate limit exceeded for key=%s", api_key[:8])
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded: 100 requests per 60 seconds.",
        )
    _rate_limit_windows[api_key].append(now)


# ---------------------------------------------------------------------------
# Application state
# ---------------------------------------------------------------------------

_engine: AsyncEngine | None = None
_matching_engine: MatchingEngine | None = None


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup and shutdown lifecycle handler.

    On startup: creates the database engine and initialises the
    :class:`MatchingEngine`.  On shutdown: disposes the connection pool.
    """
    global _engine, _matching_engine

    logger.info("Hermes API starting up (version=%s)", __version__)

    _engine = create_async_engine(
        settings.database_url,
        pool_size=10,
        max_overflow=20,
        echo=False,
    )
    _matching_engine = MatchingEngine(db_engine=_engine)
    logger.info("MatchingEngine ready")

    yield  # ← application runs here

    logger.info("Hermes API shutting down")
    if _matching_engine is not None:
        await _matching_engine.close()
    if _engine is not None:
        await _engine.dispose()
    logger.info("Database pool disposed")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Hermes Carrier-Risk Matching API",
    description=(
        "SERFF-powered carrier matching, appetite scoring, and market intelligence "
        "for commercial lines insurance placement."
    ),
    version=__version__,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS — allow any origin for internal service-to-service usage; tighten in prod
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Middleware — request logging
# ---------------------------------------------------------------------------


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log every inbound request with timing."""
    start = time.monotonic()
    response = await call_next(request)
    elapsed_ms = (time.monotonic() - start) * 1000
    logger.info(
        "%s %s → %d (%.1fms)",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------


async def require_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> str:
    """Validate the ``X-API-Key`` header and enforce rate limiting.

    Parameters
    ----------
    x_api_key:
        Value of the ``X-API-Key`` request header.

    Returns
    -------
    str
        The validated API key (passed through to the endpoint if needed).

    Raises
    ------
    HTTPException
        403 if the key is invalid; 429 if rate limit is exceeded.
    """
    if x_api_key != settings.hermes_api_key:
        logger.warning("Invalid API key attempt: %s...", x_api_key[:6])
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key.",
        )
    _check_rate_limit(x_api_key)
    return x_api_key


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_matching_engine() -> MatchingEngine:
    """Return the application-level MatchingEngine or raise 503."""
    if _matching_engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Matching engine not initialised.",
        )
    return _matching_engine


def _get_engine() -> AsyncEngine:
    """Return the application-level AsyncEngine or raise 503."""
    if _engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database engine not initialised.",
        )
    return _engine


def _match_to_response(match: CarrierMatchResult) -> CarrierMatchResponse:
    """Convert an internal :class:`CarrierMatchResult` to an API response model."""
    notes = list(match.eligibility.failed_criteria) + list(match.eligibility.conditional_notes)
    return CarrierMatchResponse(
        carrier_id=match.carrier_id,
        carrier_name=match.carrier_name,
        naic_code=match.naic_code,
        am_best_rating=None,  # populated below when available
        eligibility_status=match.eligibility.status,
        eligibility_notes=notes,
        appetite_score=match.appetite.score,
        estimated_premium=match.premium.model_dump(),
        competitiveness_rank=match.competitiveness_rank,
        coverage_highlights=match.coverage_highlights,
        recent_signals=match.recent_signals,
        filing_references=match.filing_references,
        placement_probability=match.placement_probability,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.post(
    "/v1/match",
    response_model=MatchResponse,
    summary="Run carrier-risk matching",
    tags=["Matching"],
)
async def match_risk(
    body: RiskProfileInput,
    _key: str = Depends(require_api_key),
) -> MatchResponse:
    """Match a risk profile against all eligible carriers for the requested state
    and lines of business.

    Returns a ranked list of carriers with eligibility status, appetite score,
    estimated premium, and placement probability.
    """
    engine = _get_matching_engine()
    t0 = time.monotonic()

    risk_dict = body.model_dump()
    # Merge atlas_risk_scores into the top-level risk_dict for convenience
    if body.atlas_risk_scores:
        risk_dict.update(body.atlas_risk_scores)

    all_matches = await engine.match(
        risk_profile=risk_dict,
        state=body.state,
        lines=body.coverage_lines,
    )

    elapsed_ms = round((time.monotonic() - t0) * 1000, 1)

    match_responses = [_match_to_response(m) for m in all_matches]

    return MatchResponse(
        matches=match_responses,
        state=body.state,
        lines_matched=body.coverage_lines,
        carriers_evaluated=len(all_matches),
        carriers_eligible=sum(1 for m in all_matches if m.eligibility.status != "fail"),
        match_time_ms=elapsed_ms,
    )


@app.get(
    "/v1/carriers/{naic_code}/appetite",
    response_model=AppetiteResponse,
    summary="Get carrier appetite for a state/line",
    tags=["Carriers"],
)
async def get_carrier_appetite(
    naic_code: str,
    state: str = Query(..., min_length=2, max_length=2, description="Two-letter state code"),
    line: str = Query(..., description="Line of business"),
    _key: str = Depends(require_api_key),
) -> AppetiteResponse:
    """Return appetite profile and recent signals for a specific carrier,
    state, and line of business combination.
    """
    db_engine = _get_engine()
    matching_engine = _get_matching_engine()

    # Look up carrier by NAIC code
    carrier_query = text(
        """
        SELECT id, legal_name, am_best_rating
        FROM hermes_carriers
        WHERE naic_code = :naic_code AND status = 'active'
        LIMIT 1
        """
    )
    async with db_engine.connect() as conn:
        row = (await conn.execute(carrier_query, {"naic_code": naic_code})).mappings().first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Carrier with NAIC code {naic_code!r} not found.",
        )

    carrier_id = UUID(str(row["id"]))
    carrier_name: str = row["legal_name"]

    # Load appetite profile
    profile_query = text(
        """
        SELECT
            eligible_classes, ineligible_classes, preferred_classes,
            limit_range_min, limit_range_max,
            rate_competitiveness_index, last_rate_change_pct
        FROM hermes_appetite_profiles
        WHERE carrier_id = :carrier_id AND state = :state AND line = :line
          AND is_current = TRUE
        LIMIT 1
        """
    )
    async with db_engine.connect() as conn:
        profile_row = (
            await conn.execute(
                profile_query,
                {"carrier_id": str(carrier_id), "state": state, "line": line},
            )
        ).mappings().first()

    profile = dict(profile_row) if profile_row else {}

    # Score appetite
    appetite_result = await matching_engine.appetite_scorer.score_appetite(
        carrier_id=carrier_id,
        state=state,
        line=line,
        risk_profile={"state": state},
    )
    signals = await matching_engine.appetite_scorer.get_recent_signals(carrier_id, state, line)

    return AppetiteResponse(
        carrier_id=carrier_id,
        carrier_name=carrier_name,
        state=state,
        line=line,
        appetite_score=appetite_result.score,
        eligible_classes=list(profile.get("eligible_classes") or []),
        ineligible_classes=list(profile.get("ineligible_classes") or []),
        preferred_classes=list(profile.get("preferred_classes") or []),
        limit_range={
            "min": float(profile["limit_range_min"]) if profile.get("limit_range_min") else None,
            "max": float(profile["limit_range_max"]) if profile.get("limit_range_max") else None,
        },
        rate_competitiveness=(
            float(profile["rate_competitiveness_index"])
            if profile.get("rate_competitiveness_index")
            else None
        ),
        last_rate_change=(
            float(profile["last_rate_change_pct"])
            if profile.get("last_rate_change_pct")
            else None
        ),
        recent_signals=signals,
    )


@app.get(
    "/v1/rates/{state}/{line}/{class_code}",
    response_model=RateComparisonResponse,
    summary="Compare rates across carriers",
    tags=["Rates"],
)
async def get_rate_comparison(
    state: str,
    line: str,
    class_code: str,
    _key: str = Depends(require_api_key),
) -> RateComparisonResponse:
    """Return base rates and estimated premiums from all carriers for a given
    state, line, and class code.  Results are sorted by estimated premium
    ascending (most competitive first).
    """
    db_engine = _get_engine()

    query = text(
        """
        SELECT
            c.legal_name       AS carrier_name,
            c.naic_code,
            rt.id              AS rate_table_id,
            rt.effective_date,
            ap.last_rate_change_pct AS rate_change_pct,
            br.base_rate,
            br.territory
        FROM hermes_carriers c
        JOIN hermes_rate_tables rt
            ON rt.carrier_id = c.id
            AND rt.state     = :state
            AND rt.line      = :line
            AND rt.is_current = TRUE
        LEFT JOIN hermes_base_rates br
            ON br.rate_table_id = rt.id
            AND (br.class_code = :class_code
                 OR :class_code LIKE br.class_code || '%'
                 OR br.class_code LIKE :class_code || '%')
        LEFT JOIN hermes_appetite_profiles ap
            ON ap.carrier_id = c.id
            AND ap.state     = :state
            AND ap.line      = :line
            AND ap.is_current = TRUE
        WHERE c.status = 'active'
        ORDER BY br.base_rate ASC NULLS LAST
        LIMIT 50
        """
    )

    async with db_engine.connect() as conn:
        result = await conn.execute(
            query,
            {"state": state, "line": line, "class_code": class_code},
        )
        rows = result.mappings().all()

    entries: list[RateComparisonEntry] = []
    for row in rows:
        base_rate = float(row["base_rate"]) if row["base_rate"] is not None else None
        # Rough premium estimate: base_rate × $1M revenue / 100
        estimated = round(base_rate * 10_000, 2) if base_rate is not None else None
        entries.append(
            RateComparisonEntry(
                carrier_name=row["carrier_name"],
                naic_code=row["naic_code"],
                base_rate=base_rate,
                territory_factor=1.0,
                estimated_premium=estimated,
                rate_change_pct=(
                    float(row["rate_change_pct"])
                    if row["rate_change_pct"] is not None
                    else None
                ),
                confidence=0.5 if base_rate is not None else 0.0,
            )
        )

    return RateComparisonResponse(
        state=state,
        line=line,
        class_code=class_code,
        carriers=entries,
    )


@app.get(
    "/v1/filings",
    response_model=FilingSearchResponse,
    summary="Search SERFF filings",
    tags=["Filings"],
)
async def search_filings(
    state: str | None = Query(default=None, description="Filter by state"),
    carrier_naic: str | None = Query(default=None, description="Filter by carrier NAIC"),
    line: str | None = Query(default=None, description="Filter by line of business"),
    filing_status: str | None = Query(
        default=None,
        alias="status",
        description="Filter by filing status (approved, pending, withdrawn, etc.)",
    ),
    limit: int = Query(default=50, ge=1, le=500, description="Maximum results"),
    _key: str = Depends(require_api_key),
) -> FilingSearchResponse:
    """Search SERFF filings with optional filters.

    All filter parameters are optional; omitting them returns the most recent
    filings across all carriers and states.
    """
    db_engine = _get_engine()

    conditions = []
    params: dict = {}

    if state:
        conditions.append("f.state = :state")
        params["state"] = state
    if carrier_naic:
        conditions.append("c.naic_code = :carrier_naic")
        params["carrier_naic"] = carrier_naic
    if line:
        conditions.append("f.line_of_business = :line")
        params["line"] = line
    if filing_status:
        conditions.append("f.status = :status")
        params["status"] = filing_status

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    params["limit"] = limit

    query = text(
        f"""
        SELECT
            f.serff_tracking_number,
            c.legal_name       AS carrier_name,
            f.filing_type,
            f.line_of_business AS line,
            f.status,
            f.effective_date,
            f.overall_rate_change_pct AS rate_change_pct
        FROM hermes_filings f
        LEFT JOIN hermes_carriers c ON c.id = f.carrier_id
        {where_clause}
        ORDER BY f.filed_date DESC NULLS LAST
        LIMIT :limit
        """
    )

    async with db_engine.connect() as conn:
        result = await conn.execute(query, params)
        rows = result.mappings().all()

    filings = []
    for row in rows:
        effective = row["effective_date"]
        filings.append(
            FilingEntry(
                serff_tracking=row["serff_tracking_number"],
                carrier_name=row["carrier_name"] or "Unknown",
                filing_type=row["filing_type"],
                line=row["line"],
                status=row["status"],
                effective_date=effective.isoformat() if effective else None,
                rate_change_pct=(
                    float(row["rate_change_pct"])
                    if row["rate_change_pct"] is not None
                    else None
                ),
            )
        )

    return FilingSearchResponse(filings=filings, total_found=len(filings))


@app.get(
    "/v1/market-intelligence",
    response_model=MarketIntelResponse,
    summary="Get market intelligence",
    tags=["Market Intelligence"],
)
async def get_market_intelligence(
    state: str = Query(..., min_length=2, max_length=2, description="Two-letter state code"),
    line: str = Query(..., description="Line of business"),
    period_days: int = Query(default=90, ge=7, le=730, description="Look-back window in days"),
    _key: str = Depends(require_api_key),
) -> MarketIntelResponse:
    """Return aggregate market intelligence for a state and line of business
    over the specified look-back period.
    """
    engine = _get_matching_engine()
    overview = await engine.get_market_overview(state=state, line=line)

    if not overview or overview.get("data") is None and "avg_rate_change_pct" not in overview:
        return MarketIntelResponse(
            state=state,
            line=line,
            period=f"{period_days}d",
            market_trend="unknown",
        )

    period_start = overview.get("period_start", "")
    period_end = overview.get("period_end", "")
    period_str = f"{period_start}/{period_end}" if period_start and period_end else f"{period_days}d"

    return MarketIntelResponse(
        state=state,
        line=line,
        period=period_str,
        avg_rate_change=(
            float(overview["avg_rate_change_pct"])
            if overview.get("avg_rate_change_pct") is not None
            else None
        ),
        filing_count=int(overview.get("filing_count") or 0),
        market_trend=overview.get("market_trend"),
        new_entrants=list(overview.get("new_entrants") or []),
        withdrawals=list(overview.get("withdrawals") or []),
        top_signals=list(overview.get("top_appetite_shifts") or []),
    )


@app.get(
    "/v1/health",
    response_model=HealthResponse,
    summary="System health check",
    tags=["System"],
)
async def health_check(
    _key: str = Depends(require_api_key),
) -> HealthResponse:
    """Return system health statistics including carrier and filing counts,
    active states, and most recent scrape/parse timestamps.
    """
    db_engine = _get_engine()

    try:
        async with db_engine.connect() as conn:
            carriers_row = await conn.execute(
                text("SELECT COUNT(*) FROM hermes_carriers WHERE status = 'active'")
            )
            carriers_loaded = carriers_row.scalar() or 0

            filings_row = await conn.execute(text("SELECT COUNT(*) FROM hermes_filings"))
            filings_indexed = filings_row.scalar() or 0

            states_row = await conn.execute(
                text(
                    """
                    SELECT COUNT(DISTINCT state)
                    FROM hermes_appetite_profiles
                    WHERE is_current = TRUE
                    """
                )
            )
            states_active = states_row.scalar() or 0

            last_scrape_row = await conn.execute(
                text(
                    """
                    SELECT completed_at
                    FROM hermes_scrape_log
                    WHERE status = 'completed'
                    ORDER BY completed_at DESC NULLS LAST
                    LIMIT 1
                    """
                )
            )
            last_scrape_ts = last_scrape_row.scalar()

            last_parse_row = await conn.execute(
                text(
                    """
                    SELECT completed_at
                    FROM hermes_parse_log
                    WHERE status = 'completed'
                    ORDER BY completed_at DESC NULLS LAST
                    LIMIT 1
                    """
                )
            )
            last_parse_ts = last_parse_row.scalar()

        return HealthResponse(
            status="ok",
            version=__version__,
            carriers_loaded=int(carriers_loaded),
            filings_indexed=int(filings_indexed),
            states_active=int(states_active),
            last_scrape=last_scrape_ts.isoformat() if last_scrape_ts else None,
            last_parse=last_parse_ts.isoformat() if last_parse_ts else None,
        )

    except Exception as exc:
        logger.exception("Health check database error: %s", exc)
        return HealthResponse(
            status="error",
            version=__version__,
        )
