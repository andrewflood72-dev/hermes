"""California SERFF Filing Access scraper.

Uses the generic SFA scraper — the SERFF portal is identical across states.
The original CA scraper pioneered the PrimeFaces interaction pattern that
is now in GenericSFAScraper.
"""

from hermes.config import settings
from hermes.scraper.states.generic_sfa import GenericSFAScraper


class CaliforniaScraper(GenericSFAScraper):
    """SERFF Filing Access scraper for California."""

    def __init__(self, config=settings) -> None:
        super().__init__(state="CA", config=config)
