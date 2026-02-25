"""Georgia SERFF Filing Access scraper."""

from hermes.config import settings
from hermes.scraper.states.generic_sfa import GenericSFAScraper


class GeorgiaScraper(GenericSFAScraper):
    """SERFF Filing Access scraper for Georgia."""

    def __init__(self, config=settings) -> None:
        super().__init__(state="GA", config=config)
