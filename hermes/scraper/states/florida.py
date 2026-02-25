"""Florida SERFF Filing Access scraper.

Uses the generic SFA scraper — the SERFF portal is identical across states.
"""

from hermes.config import settings
from hermes.scraper.states.generic_sfa import GenericSFAScraper


class FloridaScraper(GenericSFAScraper):
    """SERFF Filing Access scraper for Florida."""

    def __init__(self, config=settings) -> None:
        super().__init__(state="FL", config=config)
