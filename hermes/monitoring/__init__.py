"""Hermes Monitoring Package — Change detection, alerting, and market intelligence.

Exports the three core monitoring components:

- :class:`ChangeDetector` — detects appetite and rate shifts from new SERFF filings
- :class:`AlertManager` — generates and manages actionable alerts for downstream systems
- :class:`MarketReportGenerator` — computes structured market intelligence reports

These components are designed to run periodically via Celery tasks and can also be
invoked directly from the CLI.
"""

from hermes.monitoring.change_detector import ChangeDetector, AppetiteShift
from hermes.monitoring.alerts import AlertManager, Alert
from hermes.monitoring.market_report import MarketReportGenerator, MarketReport

__all__ = [
    "ChangeDetector",
    "AppetiteShift",
    "AlertManager",
    "Alert",
    "MarketReportGenerator",
    "MarketReport",
]
