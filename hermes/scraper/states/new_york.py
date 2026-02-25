"""New York SERFF Filing Access scraper.

Uses the generic SFA scraper — the SERFF portal is identical across states.
"""

from hermes.config import settings
from hermes.scraper.states.generic_sfa import GenericSFAScraper


class NewYorkScraper(GenericSFAScraper):
    """SERFF Filing Access scraper for New York."""

    def __init__(self, config=settings) -> None:
        super().__init__(state="NY", config=config)
