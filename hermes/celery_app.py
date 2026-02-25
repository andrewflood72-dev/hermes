"""Hermes Celery application — task broker, beat scheduler, and configuration.

Initialises the Celery app with Redis as both broker and result backend,
registers the periodic beat schedule for all Hermes background tasks, and
exposes the app instance for use by workers (``celery -A hermes.celery_app worker``).

Beat schedule overview:
    - daily_scrape_incremental  : 02:00 UTC daily
    - parse_new_filings         : every 4 hours
    - detect_appetite_shifts    : every 6 hours
    - recompute_appetite_profiles: 04:00 UTC daily
    - generate_market_report    : 06:00 UTC every Monday
    - stale_data_check          : 05:00 UTC daily
    - health_check              : every hour
"""

from __future__ import annotations

import logging

from celery import Celery
from celery.schedules import crontab

from hermes.config import settings

logger = logging.getLogger("hermes.celery")

# ---------------------------------------------------------------------------
# App initialisation
# ---------------------------------------------------------------------------

app = Celery(
    "hermes",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["hermes.tasks"],
)

# ---------------------------------------------------------------------------
# Serialisation & transport settings
# ---------------------------------------------------------------------------

app.conf.update(
    # Serialisation
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Timezone
    timezone="UTC",
    enable_utc=True,
    # Task behaviour
    task_track_started=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    # Result expiry — keep results for 24 hours
    result_expires=86400,
    # Retry defaults
    task_max_retries=3,
    task_default_retry_delay=60,
    # Beat scheduler persistence
    beat_schedule_filename="celerybeat-schedule",
)

# ---------------------------------------------------------------------------
# Beat schedule
# ---------------------------------------------------------------------------

app.conf.beat_schedule = {
    # 1. Incremental SERFF scrape — runs at 02:00 UTC daily
    "daily-scrape-incremental": {
        "task": "hermes.tasks.daily_scrape_incremental",
        "schedule": crontab(hour=2, minute=0),
        "options": {"queue": "scraper"},
    },
    # 2. Parse newly downloaded filing documents — every 4 hours
    "parse-new-filings": {
        "task": "hermes.tasks.parse_new_filings",
        "schedule": crontab(minute=0, hour="*/4"),
        "options": {"queue": "parser"},
    },
    # 3. Detect appetite/rate shifts — every 6 hours
    "detect-appetite-shifts": {
        "task": "hermes.tasks.detect_appetite_shifts",
        "schedule": crontab(minute=30, hour="*/6"),
        "options": {"queue": "monitoring"},
    },
    # 4. Full appetite profile recompute — 04:00 UTC daily
    "recompute-appetite-profiles": {
        "task": "hermes.tasks.recompute_appetite_profiles",
        "schedule": crontab(hour=4, minute=0),
        "options": {"queue": "monitoring"},
    },
    # 5. Weekly market report generation — Monday 06:00 UTC
    "generate-market-report": {
        "task": "hermes.tasks.generate_market_report",
        "schedule": crontab(hour=6, minute=0, day_of_week="monday"),
        "options": {"queue": "monitoring"},
    },
    # 6. Flag stale appetite profiles (90+ days) — 05:00 UTC daily
    "stale-data-check": {
        "task": "hermes.tasks.stale_data_check",
        "schedule": crontab(hour=5, minute=0),
        "options": {"queue": "monitoring"},
    },
    # 7. System health check — every hour
    "health-check": {
        "task": "hermes.tasks.health_check",
        "schedule": crontab(minute=0),
        "options": {"queue": "default"},
    },
}

logger.info(
    "Celery app configured: broker=%s tasks=%d",
    settings.redis_url,
    len(app.conf.beat_schedule),
)
