"""Virginia SERFF Filing Access scraper."""

from hermes.config import settings
from hermes.scraper.states.generic_sfa import GenericSFAScraper


class VirginiaScraper(GenericSFAScraper):
    """SERFF Filing Access scraper for Virginia."""

    def __init__(self, config=settings) -> None:
        super().__init__(state="VA", config=config)
