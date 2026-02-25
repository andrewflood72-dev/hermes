"""Hermes Celery Tasks — all periodic background jobs for the intelligence pipeline.

Each task wraps an async function using ``asyncio.get_event_loop().run_until_complete()``
via the shared ``_run_async`` helper.  Tasks are registered with the Celery app defined
in :mod:`hermes.celery_app` and scheduled via the beat configuration in the same module.

Task inventory:
    1. daily_scrape_incremental   — incremental SERFF scrape for enabled states
    2. parse_new_filings          — parse all unparsed filing documents
    3. detect_appetite_shifts     — run ChangeDetector for last 24 hours
    4. recompute_appetite_profiles— full profile recompute for recently updated carriers
    5. generate_market_report     — weekly market reports for all active state/line combos
    6. stale_data_check           — flag appetite profiles not refreshed in 90+ days
    7. health_check               — verify connectivity and log system health
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Coroutine, TypeVar

from celery import Task
from sqlalchemy import text

from hermes.celery_app import app
from hermes.config import settings
from hermes.db import async_session, engine

logger = logging.getLogger("hermes.tasks")

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Async bridge
# ---------------------------------------------------------------------------


def _run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Execute an async coroutine from a synchronous Celery task.

    Uses ``asyncio.get_event_loop().run_until_complete()`` so that Celery
    worker threads (which have no running event loop) can drive async code.

    Args:
        coro: The coroutine to execute.

    Returns:
        Whatever the coroutine returns.
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


# ---------------------------------------------------------------------------
# Task 1: daily_scrape_incremental
# ---------------------------------------------------------------------------


@app.task(
    name="hermes.tasks.daily_scrape_incremental",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    queue="scraper",
)
def daily_scrape_incremental(self: Task) -> dict:
    """Incremental SERFF scrape for all states where scrape_enabled=True.

    For each enabled state in ``hermes_state_config``:
      1. Reads ``last_scraped_at`` to determine the incremental window.
      2. Instantiates the appropriate state scraper.
      3. Runs the scrape for filings since that date.
      4. Updates ``last_scraped_at`` on success.

    Returns:
        Dict with ``states_scraped``, ``total_filings_found``,
        ``total_filings_new``, and ``errors`` keys.
    """
    logger.info("Task: daily_scrape_incremental started")
    return _run_async(_daily_scrape_incremental_async())


async def _daily_scrape_incremental_async() -> dict:
    """Async implementation of the incremental scrape task."""
    from hermes.scraper import SearchParams

    summary = {
        "states_scraped": 0,
        "total_filings_found": 0,
        "total_filings_new": 0,
        "errors": [],
    }

    async with async_session() as session:
        # Fetch all scrape-enabled states
        stmt = text(
            "SELECT state, lines_available, last_scraped_at "
            "FROM hermes_state_config "
            "WHERE scrape_enabled = TRUE "
            "ORDER BY tier ASC, state ASC"
        )
        result = await session.execute(stmt)
        state_configs = result.fetchall()

    logger.info("Found %d scrape-enabled states", len(state_configs))

    for sc in state_configs:
        state = sc.state
        last_scraped = sc.last_scraped_at

        # Default: scrape the last 7 days if never scraped before
        since_date = (
            last_scraped.date()
            if last_scraped
            else date.today() - timedelta(days=7)
        )

        lines = sc.lines_available or [None]

        for line in lines:
            try:
                scraper = _get_scraper_for_state(state)
                if scraper is None:
                    logger.debug("No scraper implementation for state=%s", state)
                    continue

                params = SearchParams(
                    state=state,
                    line_of_business=line,
                    date_from=since_date.strftime("%m/%d/%Y"),
                )
                scrape_result = await scraper.scrape(params)

                summary["total_filings_found"] += scrape_result.filings_found
                summary["total_filings_new"] += scrape_result.filings_new
                if scrape_result.errors:
                    summary["errors"].extend(scrape_result.errors[:3])

            except Exception as exc:
                msg = f"Scrape error state={state} line={line}: {exc}"
                logger.error(msg)
                summary["errors"].append(msg)

        # Update last_scraped_at
        async with async_session() as session:
            await session.execute(
                text(
                    "UPDATE hermes_state_config "
                    "SET last_scraped_at = NOW() "
                    "WHERE state = :state"
                ),
                {"state": state},
            )
            await session.commit()

        summary["states_scraped"] += 1

    logger.info(
        "daily_scrape_incremental complete: states=%d filings_new=%d errors=%d",
        summary["states_scraped"],
        summary["total_filings_new"],
        len(summary["errors"]),
    )
    return summary


def _get_scraper_for_state(state: str):
    """Return the appropriate scraper class instance for a given state code.

    Returns None if no state-specific implementation exists yet.

    Args:
        state: Two-letter state code.

    Returns:
        An initialised scraper instance, or None.
    """
    from hermes.scraper.states import (
        CaliforniaScraper,
        FloridaScraper,
        GeorgiaScraper,
        IllinoisScraper,
        MassachusettsScraper,
        MichiganScraper,
        NewJerseyScraper,
        NewYorkScraper,
        NorthCarolinaScraper,
        OhioScraper,
        PennsylvaniaScraper,
        TexasScraper,
        VirginiaScraper,
    )

    scrapers = {
        "CA": CaliforniaScraper,
        "TX": TexasScraper,
        "NY": NewYorkScraper,
        "FL": FloridaScraper,
        "OH": OhioScraper,
        "GA": GeorgiaScraper,
        "IL": IllinoisScraper,
        "MA": MassachusettsScraper,
        "MI": MichiganScraper,
        "NC": NorthCarolinaScraper,
        "NJ": NewJerseyScraper,
        "PA": PennsylvaniaScraper,
        "VA": VirginiaScraper,
    }
    cls = scrapers.get(state.upper())
    if cls is None:
        return None
    # State scrapers hardcode their state in __init__, so don't pass state=
    try:
        return cls(state=state)
    except TypeError:
        return cls()


# ---------------------------------------------------------------------------
# Task 2: parse_new_filings
# ---------------------------------------------------------------------------


@app.task(
    name="hermes.tasks.parse_new_filings",
    bind=True,
    max_retries=3,
    default_retry_delay=120,
    queue="parser",
)
def parse_new_filings(self: Task) -> dict:
    """Parse all unparsed filing documents in ``hermes_filing_documents``.

    For each document where ``parsed_flag = FALSE``:
      1. Classifies the document type via :class:`DocumentClassifier`.
      2. Routes to the appropriate parser (RateParser, RuleParser, FormParser).
      3. Marks the document as parsed on success.

    Returns:
        Dict with ``documents_parsed``, ``documents_failed``, and ``errors``.
    """
    logger.info("Task: parse_new_filings started")
    return _run_async(_parse_new_filings_async())


async def _parse_new_filings_async() -> dict:
    """Async implementation of the parse_new_filings task."""
    from hermes.parsers import DocumentClassifier, RateParser, RuleParser, FormParser

    summary = {"documents_parsed": 0, "documents_failed": 0, "errors": []}

    # Fetch unparsed documents (batch of 100)
    async with async_session() as session:
        stmt = text(
            """
            SELECT id, file_path, document_type
            FROM hermes_filing_documents
            WHERE parsed_flag = FALSE
              AND file_path IS NOT NULL
            ORDER BY created_at ASC
            LIMIT 100
            """
        )
        result = await session.execute(stmt)
        docs = result.fetchall()

    logger.info("Found %d unparsed documents", len(docs))

    classifier = DocumentClassifier()
    rate_parser = RateParser()
    rule_parser = RuleParser()
    form_parser = FormParser()

    for doc in docs:
        try:
            # Classify document type if not already set
            doc_type = doc.document_type
            if not doc_type and doc.file_path:
                doc_type = await classifier.classify(doc.file_path)

            # Route to appropriate parser
            if doc_type == "rate":
                parse_result = await rate_parser.parse(doc.id, doc.file_path)
            elif doc_type == "rule":
                parse_result = await rule_parser.parse(doc.id, doc.file_path)
            elif doc_type == "form":
                parse_result = await form_parser.parse(doc.id, doc.file_path)
            else:
                logger.debug("Unknown document type '%s' for doc=%s", doc_type, doc.id)
                continue

            if parse_result.status in ("completed", "partial"):
                # Mark document as parsed
                async with async_session() as session:
                    await session.execute(
                        text(
                            "UPDATE hermes_filing_documents "
                            "SET parsed_flag = TRUE, "
                            "    parse_confidence = :confidence, "
                            "    parse_version = :version, "
                            "    updated_at = NOW() "
                            "WHERE id = :doc_id"
                        ),
                        {
                            "doc_id": str(doc.id),
                            "confidence": parse_result.confidence_avg,
                            "version": "1.0",
                        },
                    )
                    await session.commit()
                summary["documents_parsed"] += 1
            else:
                summary["documents_failed"] += 1

        except Exception as exc:
            msg = f"Parse error doc={doc.id}: {exc}"
            logger.error(msg)
            summary["errors"].append(msg)
            summary["documents_failed"] += 1

    logger.info(
        "parse_new_filings complete: parsed=%d failed=%d",
        summary["documents_parsed"],
        summary["documents_failed"],
    )
    return summary


# ---------------------------------------------------------------------------
# Task 3: detect_appetite_shifts
# ---------------------------------------------------------------------------


@app.task(
    name="hermes.tasks.detect_appetite_shifts",
    bind=True,
    max_retries=2,
    default_retry_delay=180,
    queue="monitoring",
)
def detect_appetite_shifts(self: Task) -> dict:
    """Run ChangeDetector across all filings parsed in the last 24 hours.

    Calls :meth:`ChangeDetector.detect_all_shifts` with ``since_date`` set
    to 24 hours ago, then passes results to :meth:`AlertManager.check_submission_impacts`.

    Returns:
        Dict with ``shifts_detected`` and ``alerts_generated``.
    """
    logger.info("Task: detect_appetite_shifts started")
    return _run_async(_detect_appetite_shifts_async())


async def _detect_appetite_shifts_async() -> dict:
    """Async implementation of the detect_appetite_shifts task."""
    from hermes.monitoring import ChangeDetector, AlertManager

    since_date = date.today() - timedelta(days=1)

    detector = ChangeDetector()
    alert_manager = AlertManager()

    shifts = await detector.detect_all_shifts(since_date=since_date)
    alerts = await alert_manager.check_submission_impacts(shifts)

    logger.info(
        "detect_appetite_shifts complete: shifts=%d alerts=%d",
        len(shifts),
        len(alerts),
    )
    return {"shifts_detected": len(shifts), "alerts_generated": len(alerts)}


# ---------------------------------------------------------------------------
# Task 4: recompute_appetite_profiles
# ---------------------------------------------------------------------------


@app.task(
    name="hermes.tasks.recompute_appetite_profiles",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    queue="monitoring",
)
def recompute_appetite_profiles(self: Task) -> dict:
    """Recompute appetite profiles for carriers with new filings since yesterday.

    Queries for distinct carrier/state/line combinations where a filing was
    parsed in the last 24 hours, then calls
    :meth:`ChangeDetector.recompute_appetite` for each.

    Returns:
        Dict with ``profiles_recomputed`` and ``errors``.
    """
    logger.info("Task: recompute_appetite_profiles started")
    return _run_async(_recompute_appetite_profiles_async())


async def _recompute_appetite_profiles_async() -> dict:
    """Async implementation of the recompute_appetite_profiles task."""
    from hermes.monitoring import ChangeDetector

    summary = {"profiles_recomputed": 0, "errors": []}
    since_date = date.today() - timedelta(days=1)

    # Find distinct combos with recently parsed documents
    async with async_session() as session:
        stmt = text(
            """
            SELECT DISTINCT
                f.carrier_id,
                f.state,
                f.line_of_business AS line
            FROM hermes_filings f
            JOIN hermes_filing_documents fd ON fd.filing_id = f.id
            WHERE fd.parsed_flag = TRUE
              AND f.carrier_id IS NOT NULL
              AND fd.updated_at::date >= :since_date
            """
        )
        result = await session.execute(stmt, {"since_date": since_date})
        combos = result.fetchall()

    logger.info("Recomputing %d appetite profiles", len(combos))
    detector = ChangeDetector()

    for row in combos:
        try:
            await detector.recompute_appetite(
                carrier_id=row.carrier_id,
                state=row.state,
                line=row.line,
            )
            summary["profiles_recomputed"] += 1
        except Exception as exc:
            msg = f"Recompute error carrier={row.carrier_id} state={row.state} line={row.line}: {exc}"
            logger.error(msg)
            summary["errors"].append(msg)

    logger.info(
        "recompute_appetite_profiles complete: recomputed=%d errors=%d",
        summary["profiles_recomputed"],
        len(summary["errors"]),
    )
    return summary


# ---------------------------------------------------------------------------
# Task 5: generate_market_report
# ---------------------------------------------------------------------------


@app.task(
    name="hermes.tasks.generate_market_report",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    queue="monitoring",
)
def generate_market_report(self: Task) -> dict:
    """Generate weekly market intelligence reports for all active state/line combos.

    Queries for all distinct state/line combinations that have had filing
    activity in the last 90 days, then runs
    :meth:`MarketReportGenerator.generate_report` for each.

    Returns:
        Dict with ``reports_generated`` and ``errors``.
    """
    logger.info("Task: generate_market_report started")
    return _run_async(_generate_market_report_async())


async def _generate_market_report_async() -> dict:
    """Async implementation of the generate_market_report task."""
    from hermes.monitoring import MarketReportGenerator

    summary = {"reports_generated": 0, "errors": []}

    # Find all active state/line combinations
    since_date = date.today() - timedelta(days=90)
    async with async_session() as session:
        stmt = text(
            """
            SELECT DISTINCT state, line_of_business AS line
            FROM hermes_filings
            WHERE filed_date >= :since_date
              AND status NOT IN ('withdrawn', 'disapproved')
            ORDER BY state, line
            """
        )
        result = await session.execute(stmt, {"since_date": since_date})
        combos = result.fetchall()

    logger.info("Generating market reports for %d state/line combinations", len(combos))
    generator = MarketReportGenerator()

    for row in combos:
        try:
            await generator.generate_report(
                state=row.state,
                line=row.line,
                period_days=30,
            )
            summary["reports_generated"] += 1
        except Exception as exc:
            msg = f"Report error state={row.state} line={row.line}: {exc}"
            logger.error(msg)
            summary["errors"].append(msg)

    logger.info(
        "generate_market_report complete: reports=%d errors=%d",
        summary["reports_generated"],
        len(summary["errors"]),
    )
    return summary


# ---------------------------------------------------------------------------
# Task 6: stale_data_check
# ---------------------------------------------------------------------------


@app.task(
    name="hermes.tasks.stale_data_check",
    bind=True,
    max_retries=2,
    default_retry_delay=120,
    queue="monitoring",
)
def stale_data_check(self: Task) -> dict:
    """Flag appetite profiles that have not been refreshed in 90+ days.

    Sets ``is_current = FALSE`` on stale profiles and logs a warning for each
    affected carrier/state/line combination.

    Returns:
        Dict with ``stale_profiles_flagged`` count.
    """
    logger.info("Task: stale_data_check started")
    return _run_async(_stale_data_check_async())


async def _stale_data_check_async() -> dict:
    """Async implementation of the stale_data_check task."""
    summary = {"stale_profiles_flagged": 0}
    stale_cutoff = date.today() - timedelta(days=90)

    async with async_session() as session:
        # Find stale profiles
        find_stmt = text(
            """
            SELECT ap.id, c.legal_name, ap.state, ap.line, ap.computed_at
            FROM hermes_appetite_profiles ap
            JOIN hermes_carriers c ON c.id = ap.carrier_id
            WHERE ap.is_current = TRUE
              AND ap.computed_at::date < :stale_cutoff
            """
        )
        result = await session.execute(find_stmt, {"stale_cutoff": stale_cutoff})
        stale = result.fetchall()

        if stale:
            logger.warning("Found %d stale appetite profiles (>90 days)", len(stale))
            for row in stale:
                logger.warning(
                    "Stale profile: carrier=%s state=%s line=%s last_computed=%s",
                    row.legal_name,
                    row.state,
                    row.line,
                    row.computed_at,
                )

            # Flag as no longer current
            flag_stmt = text(
                """
                UPDATE hermes_appetite_profiles
                SET is_current = FALSE, updated_at = NOW()
                WHERE is_current = TRUE
                  AND computed_at::date < :stale_cutoff
                """
            )
            await session.execute(flag_stmt, {"stale_cutoff": stale_cutoff})
            await session.commit()
            summary["stale_profiles_flagged"] = len(stale)

    logger.info(
        "stale_data_check complete: flagged=%d", summary["stale_profiles_flagged"]
    )
    return summary


# ---------------------------------------------------------------------------
# Task 7: health_check
# ---------------------------------------------------------------------------


@app.task(
    name="hermes.tasks.health_check",
    bind=True,
    max_retries=1,
    default_retry_delay=30,
    queue="default",
)
def health_check(self: Task) -> dict:
    """Verify database connectivity, scraper status, and log overall system health.

    Checks:
      - PostgreSQL connectivity (simple SELECT 1)
      - Count of unparsed documents (backlog indicator)
      - Count of pending scrape runs (stuck jobs)
      - Count of unacknowledged high-severity alerts

    Returns:
        Dict with ``status`` (healthy/degraded/unhealthy) and detail fields.
    """
    logger.info("Task: health_check started")
    return _run_async(_health_check_async())


async def _health_check_async() -> dict:
    """Async implementation of the health_check task."""
    report: dict = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database": "unknown",
        "unparsed_documents": 0,
        "stuck_scrapes": 0,
        "unacknowledged_high_alerts": 0,
        "issues": [],
    }

    # --- Database connectivity ---
    try:
        async with async_session() as session:
            await session.execute(text("SELECT 1"))
        report["database"] = "ok"
    except Exception as exc:
        report["database"] = f"error: {exc}"
        report["issues"].append(f"Database connection failed: {exc}")
        report["status"] = "unhealthy"
        logger.error("Health check: DB connectivity failed: %s", exc)
        return report

    # --- Unparsed document backlog ---
    try:
        async with async_session() as session:
            result = await session.execute(
                text(
                    "SELECT COUNT(*) AS cnt FROM hermes_filing_documents "
                    "WHERE parsed_flag = FALSE AND file_path IS NOT NULL"
                )
            )
            row = result.fetchone()
            backlog = int(row.cnt) if row else 0
            report["unparsed_documents"] = backlog
            if backlog > 500:
                report["issues"].append(
                    f"Large parse backlog: {backlog} unparsed documents"
                )
                if report["status"] == "healthy":
                    report["status"] = "degraded"
    except Exception as exc:
        logger.warning("Health check: could not count unparsed docs: %s", exc)

    # --- Stuck scrape runs ---
    try:
        stuck_cutoff = datetime.now(timezone.utc) - timedelta(hours=6)
        async with async_session() as session:
            result = await session.execute(
                text(
                    "SELECT COUNT(*) AS cnt FROM hermes_scrape_log "
                    "WHERE status = 'running' AND started_at < :cutoff"
                ),
                {"cutoff": stuck_cutoff},
            )
            row = result.fetchone()
            stuck = int(row.cnt) if row else 0
            report["stuck_scrapes"] = stuck
            if stuck > 0:
                report["issues"].append(f"{stuck} scrape run(s) appear stuck (>6h running)")
                if report["status"] == "healthy":
                    report["status"] = "degraded"
    except Exception as exc:
        logger.warning("Health check: could not count stuck scrapes: %s", exc)

    # --- Unacknowledged high-severity alerts ---
    try:
        async with async_session() as session:
            result = await session.execute(
                text(
                    "SELECT COUNT(*) AS cnt FROM hermes_appetite_signals "
                    "WHERE acknowledged = FALSE AND signal_strength >= 7"
                )
            )
            row = result.fetchone()
            high_alerts = int(row.cnt) if row else 0
            report["unacknowledged_high_alerts"] = high_alerts
            if high_alerts > 10:
                report["issues"].append(
                    f"{high_alerts} unacknowledged high-severity appetite signals"
                )
    except Exception as exc:
        logger.warning("Health check: could not count high alerts: %s", exc)

    if report["status"] == "healthy":
        logger.info("Health check: system healthy")
    else:
        logger.warning(
            "Health check: status=%s issues=%s",
            report["status"],
            report["issues"],
        )

    return report


# ---------------------------------------------------------------------------
# Task 8: scrape_title_filings
# ---------------------------------------------------------------------------


@app.task(
    name="hermes.tasks.scrape_title_filings",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    queue="scraper",
)
def scrape_title_filings(self: Task) -> dict:
    """Scrape title insurance filings from SERFF and state DOI portals.

    For promulgated states (TX, NM), loads state-set rates directly.
    For other states, runs SERFF title-specific search.

    Returns:
        Dict with ``states_scraped``, ``rate_cards_loaded``, and ``errors``.
    """
    logger.info("Task: scrape_title_filings started")
    return _run_async(_scrape_title_filings_async())


async def _scrape_title_filings_async() -> dict:
    """Async implementation of the title filing scrape task."""
    from hermes.scraper.title_search import (
        build_title_search_params,
        is_promulgated_state,
    )

    summary = {
        "states_scraped": 0,
        "rate_cards_loaded": 0,
        "filings_found": 0,
        "errors": [],
    }

    # Get scrape-enabled states
    async with async_session() as session:
        stmt = text(
            "SELECT state FROM hermes_state_config "
            "WHERE scrape_enabled = TRUE "
            "ORDER BY tier ASC, state ASC"
        )
        result = await session.execute(stmt)
        state_configs = result.fetchall()

    for sc in state_configs:
        state = sc.state
        try:
            if is_promulgated_state(state):
                if state == "TX":
                    from hermes.scraper.tdi_scraper import load_tx_promulgated_rates
                    results = await load_tx_promulgated_rates()
                    summary["rate_cards_loaded"] += len(results)
                # Add other promulgated states as implemented
            else:
                params = build_title_search_params(state)
                if params is None:
                    continue

                scraper = _get_scraper_for_state(state)
                if scraper is None:
                    continue

                scrape_result = await scraper.scrape(params)
                summary["filings_found"] += scrape_result.filings_found
                if scrape_result.errors:
                    summary["errors"].extend(scrape_result.errors[:3])

            summary["states_scraped"] += 1

        except Exception as exc:
            msg = f"Title scrape error state={state}: {exc}"
            logger.error(msg)
            summary["errors"].append(msg)

    logger.info(
        "scrape_title_filings complete: states=%d rate_cards=%d errors=%d",
        summary["states_scraped"],
        summary["rate_cards_loaded"],
        len(summary["errors"]),
    )
    return summary


# ---------------------------------------------------------------------------
# Task 9: parse_title_filings
# ---------------------------------------------------------------------------


@app.task(
    name="hermes.tasks.parse_title_filings",
    bind=True,
    max_retries=3,
    default_retry_delay=120,
    queue="parser",
)
def parse_title_filings(self: Task) -> dict:
    """Parse all unparsed title insurance filing documents.

    Finds documents from title insurance filings that haven't been parsed,
    runs them through the TitleParser, and stores structured rate data.

    Returns:
        Dict with ``documents_parsed``, ``documents_failed``, and ``errors``.
    """
    logger.info("Task: parse_title_filings started")
    return _run_async(_parse_title_filings_async())


async def _parse_title_filings_async() -> dict:
    """Async implementation of the title filing parse task."""
    from hermes.parsers.title_parser import TitleParser

    summary = {"documents_parsed": 0, "documents_failed": 0, "errors": []}

    # Fetch unparsed title filing documents
    async with async_session() as session:
        stmt = text(
            """
            SELECT fd.id, fd.file_path, fd.document_type
            FROM hermes_filing_documents fd
            JOIN hermes_filings f ON f.id = fd.filing_id
            WHERE fd.parsed_flag = FALSE
              AND fd.file_path IS NOT NULL
              AND f.line_of_business ILIKE '%title%'
            ORDER BY fd.created_at ASC
            LIMIT 100
            """
        )
        result = await session.execute(stmt)
        docs = result.fetchall()

    logger.info("Found %d unparsed title documents", len(docs))

    title_parser = TitleParser()

    for doc in docs:
        try:
            parse_result = await title_parser.parse(doc.id, doc.file_path)

            if parse_result.status in ("completed", "partial"):
                async with async_session() as session:
                    await session.execute(
                        text(
                            "UPDATE hermes_filing_documents "
                            "SET parsed_flag = TRUE, "
                            "    parse_confidence = :confidence, "
                            "    parse_version = :version, "
                            "    updated_at = NOW() "
                            "WHERE id = :doc_id"
                        ),
                        {
                            "doc_id": str(doc.id),
                            "confidence": parse_result.confidence_avg,
                            "version": "1.0",
                        },
                    )
                    await session.commit()
                summary["documents_parsed"] += 1
            else:
                summary["documents_failed"] += 1

        except Exception as exc:
            msg = f"Title parse error doc={doc.id}: {exc}"
            logger.error(msg)
            summary["errors"].append(msg)
            summary["documents_failed"] += 1

    logger.info(
        "parse_title_filings complete: parsed=%d failed=%d",
        summary["documents_parsed"],
        summary["documents_failed"],
    )
    return summary
