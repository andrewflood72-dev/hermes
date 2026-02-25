"""Massachusetts SERFF Filing Access scraper."""

from hermes.config import settings
from hermes.scraper.states.generic_sfa import GenericSFAScraper


class MassachusettsScraper(GenericSFAScraper):
    """SERFF Filing Access scraper for Massachusetts."""

    def __init__(self, config=settings) -> None:
        super().__init__(state="MA", config=config)
