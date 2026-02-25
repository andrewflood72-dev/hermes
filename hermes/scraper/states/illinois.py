"""Illinois SERFF Filing Access scraper."""

from hermes.config import settings
from hermes.scraper.states.generic_sfa import GenericSFAScraper


class IllinoisScraper(GenericSFAScraper):
    """SERFF Filing Access scraper for Illinois."""

    def __init__(self, config=settings) -> None:
        super().__init__(state="IL", config=config)
