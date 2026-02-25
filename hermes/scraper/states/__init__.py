"""State-specific SERFF scraper implementations.

Each sub-module contains a scraper class that extends
:class:`~hermes.scraper.base.BaseSERFFScraper` with portal selectors and
pagination patterns specific to that state's SFA portal.
"""

from hermes.scraper.states.california import CaliforniaScraper
from hermes.scraper.states.florida import FloridaScraper
from hermes.scraper.states.georgia import GeorgiaScraper
from hermes.scraper.states.illinois import IllinoisScraper
from hermes.scraper.states.massachusetts import MassachusettsScraper
from hermes.scraper.states.michigan import MichiganScraper
from hermes.scraper.states.new_jersey import NewJerseyScraper
from hermes.scraper.states.new_york import NewYorkScraper
from hermes.scraper.states.north_carolina import NorthCarolinaScraper
from hermes.scraper.states.ohio import OhioScraper
from hermes.scraper.states.pennsylvania import PennsylvaniaScraper
from hermes.scraper.states.texas import TexasScraper
from hermes.scraper.states.virginia import VirginiaScraper

__all__ = [
    "CaliforniaScraper",
    "FloridaScraper",
    "GeorgiaScraper",
    "IllinoisScraper",
    "MassachusettsScraper",
    "MichiganScraper",
    "NewJerseyScraper",
    "NewYorkScraper",
    "NorthCarolinaScraper",
    "OhioScraper",
    "PennsylvaniaScraper",
    "TexasScraper",
    "VirginiaScraper",
]
