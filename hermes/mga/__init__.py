"""MGA Proposal Agent — generates AI-powered MGA business proposals."""

from hermes.mga.agent import MGAProposalAgent
from hermes.mga.models import MGAProposal
from hermes.mga.schemas import MGAProposalRequest, MGAProposalResponse

__all__ = [
    "MGAProposalAgent",
    "MGAProposal",
    "MGAProposalRequest",
    "MGAProposalResponse",
]
