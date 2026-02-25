"""FastAPI routes for the MGA Proposal Agent.

All endpoints are prefixed with ``/v1/mga``.
"""

from __future__ import annotations

import json
import logging
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hermes.mga.agent import MGAProposalAgent
from hermes.mga.renderer import render_proposal_html, render_proposal_pdf
from hermes.mga.schemas import (
    MGAProposalListItem,
    MGAProposalRequest,
    MGAProposalResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/mga", tags=["MGA"])

# ---------------------------------------------------------------------------
# Module-level state — set by the main app lifespan handler
# ---------------------------------------------------------------------------

_mga_agent: MGAProposalAgent | None = None
_db_engine: AsyncEngine | None = None


def set_mga_agent(agent: MGAProposalAgent) -> None:
    """Called by the main app to inject the initialised MGA agent."""
    global _mga_agent
    _mga_agent = agent


def set_db_engine(engine: AsyncEngine) -> None:
    """Called by the main app to inject the database engine."""
    global _db_engine
    _db_engine = engine


def _get_mga_agent() -> MGAProposalAgent:
    if _mga_agent is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="MGA proposal agent not initialised.",
        )
    return _mga_agent


def _get_db_engine() -> AsyncEngine:
    if _db_engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database engine not initialised.",
        )
    return _db_engine


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_PROPOSAL_QUERY = text("""
    SELECT id, program_type, title, request_data, proposal_data,
           status, token_usage, generated_by, created_at
    FROM hermes_mga_proposals
    WHERE id = :id
""")


async def _fetch_proposal_row(proposal_id: UUID) -> dict:
    """Fetch a single proposal row and deserialise JSON columns.

    Returns a plain dict with ``proposal_data`` and ``token_usage``
    guaranteed to be Python dicts.

    Raises ``HTTPException(404)`` when not found.
    """
    engine = _get_db_engine()
    async with engine.connect() as conn:
        row = (
            await conn.execute(_PROPOSAL_QUERY, {"id": str(proposal_id)})
        ).mappings().first()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Proposal {proposal_id} not found.",
        )

    data = dict(row)

    if isinstance(data["proposal_data"], str):
        data["proposal_data"] = json.loads(data["proposal_data"])
    if isinstance(data["token_usage"], str):
        data["token_usage"] = json.loads(data["token_usage"])

    return data


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/proposal", response_model=MGAProposalResponse)
async def generate_proposal(body: MGAProposalRequest) -> MGAProposalResponse:
    """Generate a new MGA business proposal."""
    agent = _get_mga_agent()
    logger.info("Generating MGA proposal: program=%s partner=%s",
                body.program_type, body.distribution_partner)
    return await agent.generate_proposal(body)


@router.get("/proposals", response_model=list[MGAProposalListItem])
async def list_proposals() -> list[MGAProposalListItem]:
    """List all generated proposals."""
    engine = _get_db_engine()
    query = text("""
        SELECT id, program_type, title, status, generated_by, created_at
        FROM hermes_mga_proposals
        ORDER BY created_at DESC
        LIMIT 50
    """)
    async with engine.connect() as conn:
        rows = (await conn.execute(query)).mappings().all()
    return [
        MGAProposalListItem(
            id=row["id"],
            program_type=row["program_type"],
            title=row["title"],
            status=row["status"],
            generated_by=row["generated_by"],
            created_at=row["created_at"],
        )
        for row in rows
    ]


@router.get("/proposals/{proposal_id}", response_model=MGAProposalResponse)
async def get_proposal(proposal_id: UUID) -> MGAProposalResponse:
    """Retrieve a specific proposal by ID."""
    from hermes.mga.schemas import FinancialProjection, ProposalSection

    row = await _fetch_proposal_row(proposal_id)
    proposal_data = row["proposal_data"]

    sections = {}
    for key, sec_data in proposal_data.get("sections", {}).items():
        sections[key] = ProposalSection(**sec_data)

    projections = [
        FinancialProjection(**p)
        for p in proposal_data.get("financial_projections", [])
    ]

    exec_summary = sections.get("executive_summary")

    return MGAProposalResponse(
        id=row["id"],
        program_type=row["program_type"],
        title=row["title"],
        sections=sections,
        financial_projections=projections,
        executive_summary=exec_summary.content if exec_summary else "",
        status=row["status"],
        token_usage=row["token_usage"],
        generated_at=row["created_at"],
    )


@router.get("/proposals/{proposal_id}/view", response_class=HTMLResponse)
async def view_proposal(proposal_id: UUID) -> HTMLResponse:
    """Render the proposal as a formatted HTML page."""
    row = await _fetch_proposal_row(proposal_id)

    html = render_proposal_html(
        proposal_data=row["proposal_data"],
        title=row["title"],
        created_at=row["created_at"],
        token_usage=row["token_usage"],
        proposal_id=str(row["id"]),
        status=row["status"],
    )
    return HTMLResponse(content=html)


@router.get("/proposals/{proposal_id}/pdf")
async def download_proposal_pdf(proposal_id: UUID) -> StreamingResponse:
    """Download the proposal as a PDF document."""
    row = await _fetch_proposal_row(proposal_id)

    html = render_proposal_html(
        proposal_data=row["proposal_data"],
        title=row["title"],
        created_at=row["created_at"],
        token_usage=row["token_usage"],
        proposal_id="",  # no download button in PDF source
        status=row["status"],
    )
    pdf_bytes = await render_proposal_pdf(html)

    # Sanitise filename to ASCII (em dashes, accents, etc. break latin-1 headers)
    title_ascii = row["title"].encode("ascii", "ignore").decode("ascii")
    filename = title_ascii.replace(" ", "_")[:80] + ".pdf"
    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
