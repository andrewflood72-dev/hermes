"""Pennsylvania SERFF Filing Access scraper."""

from hermes.config import settings
from hermes.scraper.states.generic_sfa import GenericSFAScraper


class PennsylvaniaScraper(GenericSFAScraper):
    """SERFF Filing Access scraper for Pennsylvania."""

    def __init__(self, config=settings) -> None:
        super().__init__(state="PA", config=config)
