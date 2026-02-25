"""SERFF filing loader for PMI rate data — Phase 2 stub.

Will extract approved rate ranges, state rules, and actuarial data
from SERFF filings for PMI carriers.
"""

from __future__ import annotations

import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger("hermes.pmi.serff_loader")


class PMISERFFLoader:
    """Loads PMI-relevant data from SERFF filings into hermes_pmi_serff_data.

    Phase 2 implementation will:
    - Parse PMI-specific SERFF filings for approved rate ranges
    - Extract state-level rules and restrictions
    - Pull actuarial data (loss ratios, reserves)
    - Link to existing hermes_filings records
    """

    def __init__(self, db_engine: AsyncEngine | None = None) -> None:
        self._engine = db_engine

    async def load_from_filing(self, filing_id: UUID) -> dict:
        """Extract PMI data from a SERFF filing. (Phase 2 stub)"""
        logger.info("PMI SERFF loader not yet implemented — filing_id=%s", filing_id)
        return {"status": "not_implemented", "filing_id": str(filing_id)}

    async def sync_state_rules(self, state: str) -> dict:
        """Sync all PMI SERFF data for a state. (Phase 2 stub)"""
        logger.info("PMI SERFF state sync not yet implemented — state=%s", state)
        return {"status": "not_implemented", "state": state}

    async def refresh_all(self) -> dict:
        """Refresh all PMI SERFF data from latest filings. (Phase 2 stub)"""
        logger.info("PMI SERFF refresh not yet implemented")
        return {"status": "not_implemented"}
