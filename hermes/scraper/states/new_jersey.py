"""New Jersey SERFF Filing Access scraper."""

from hermes.config import settings
from hermes.scraper.states.generic_sfa import GenericSFAScraper


class NewJerseyScraper(GenericSFAScraper):
    """SERFF Filing Access scraper for New Jersey."""

    def __init__(self, config=settings) -> None:
        super().__init__(state="NJ", config=config)
