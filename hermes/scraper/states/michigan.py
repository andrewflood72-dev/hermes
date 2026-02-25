"""Michigan SERFF Filing Access scraper."""

from hermes.config import settings
from hermes.scraper.states.generic_sfa import GenericSFAScraper


class MichiganScraper(GenericSFAScraper):
    """SERFF Filing Access scraper for Michigan."""

    def __init__(self, config=settings) -> None:
        super().__init__(state="MI", config=config)
