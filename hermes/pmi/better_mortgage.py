"""Better Mortgage embedded PMI gateway — Phase 3 stub.

Will handle the integration with Better Mortgage's loan origination
system for real-time PMI pricing and bind requests.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("hermes.pmi.better_mortgage")


class BetterMortgageGateway:
    """Gateway for Better Mortgage embedded PMI MGA partnership.

    Phase 3 implementation will:
    - Accept loan submissions from Better's LOS via webhook
    - Return real-time PMI quotes with carrier binding
    - Handle MI certificate generation
    - Process cancellation and renewal events
    - Sync loan status updates
    """

    def __init__(self) -> None:
        self._configured = False

    async def handle_submission(self, payload: dict[str, Any]) -> dict:
        """Process a loan submission from Better Mortgage. (Phase 3 stub)"""
        logger.info("Better Mortgage gateway not yet implemented")
        return {"status": "not_implemented"}

    async def handle_webhook(self, event_type: str, payload: dict[str, Any]) -> dict:
        """Process a webhook event from Better Mortgage. (Phase 3 stub)"""
        logger.info("Better Mortgage webhook not yet implemented — event=%s", event_type)
        return {"status": "not_implemented", "event_type": event_type}
