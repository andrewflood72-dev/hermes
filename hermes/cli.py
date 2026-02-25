"""Hermes CLI — Command-line interface for the Hermes regulatory intelligence engine.

Provides commands for scraping, parsing, matching, monitoring, alerts, market
reports, and system health.  Uses Typer for argument parsing and Rich for
formatted terminal output.

Usage::

    python -m hermes.cli --help
    python -m hermes.cli scrape --state TX --line "Commercial Auto"
    python -m hermes.cli monitor --state TX --since 2025-01-01
    python -m hermes.cli market-report --state TX --line "Commercial Auto"
    python -m hermes.cli health
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date, datetime, timedelta
from typing import Optional
from uuid import UUID

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from hermes.config import settings

# ---------------------------------------------------------------------------
# App & console setup
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="hermes",
    help="Hermes Regulatory Intelligence CLI — SERFF filing scraper, parser, and market monitor.",
    add_completion=False,
    rich_markup_mode="rich",
)

console = Console()
err_console = Console(stderr=True, style="bold red")

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("hermes.cli")

# ---------------------------------------------------------------------------
# Async bridge
# ---------------------------------------------------------------------------


def _run(coro):
    """Execute a coroutine from synchronous CLI context."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Command: scrape
# ---------------------------------------------------------------------------


@app.command("scrape")
def scrape(
    state: str = typer.Option(..., "--state", "-s", help="State code (e.g. TX, CA)"),
    line: Optional[str] = typer.Option(
        None, "--line", "-l", help="Line of business (default: all configured lines)"
    ),
    carrier_naic: Optional[str] = typer.Option(
        None, "--carrier-naic", help="Specific carrier NAIC code to scrape"
    ),
    since: Optional[str] = typer.Option(
        None, "--since", help="Incremental start date (YYYY-MM-DD)"
    ),
    top_carriers: int = typer.Option(
        25, "--top-carriers", help="Scrape top N carriers by premium"
    ),
) -> None:
    """Run the SERFF scraper for a given state.

    Examples:

      hermes scrape --state TX

      hermes scrape --state CA --line "Commercial Auto" --since 2025-01-01

      hermes scrape --state TX --carrier-naic 12345
    """
    console.print(
        Panel(
            f"[bold cyan]Hermes SERFF Scraper[/bold cyan]\n"
            f"State: [yellow]{state.upper()}[/yellow]  "
            f"Line: [yellow]{line or 'all'}[/yellow]  "
            f"Carrier NAIC: [yellow]{carrier_naic or 'all'}[/yellow]",
            title="Scrape",
            expand=False,
        )
    )

    try:
        from hermes.scraper import SearchParams

        state_upper = state.upper()

        # Parse since date
        date_from: Optional[str] = None
        if since:
            try:
                since_date = date.fromisoformat(since)
                date_from = since_date.strftime("%m/%d/%Y")
            except ValueError:
                err_console.print(f"Invalid date format for --since: {since}. Use YYYY-MM-DD.")
                raise typer.Exit(1)

        params = SearchParams(
            state=state_upper,
            line_of_business=line,
            carrier_naic=carrier_naic,
            date_from=date_from,
            max_pages=top_carriers * 2,
        )

        from hermes.tasks import _get_scraper_for_state

        scraper = _get_scraper_for_state(state_upper)
        if scraper is None:
            err_console.print(
                f"No scraper implementation found for state '{state_upper}'. "
                "Supported: TX, CA"
            )
            raise typer.Exit(1)

        with console.status(f"[bold green]Scraping {state_upper}...[/bold green]"):
            result = _run(scraper.scrape(params))

        # Display results
        table = Table(title="Scrape Results", box=box.ROUNDED)
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="green", justify="right")
        table.add_row("State", state_upper)
        table.add_row("Filings Found", str(result.filings_found))
        table.add_row("Filings New", str(result.filings_new))
        table.add_row("Documents Downloaded", str(result.documents_downloaded))
        table.add_row("Duration", f"{result.duration_seconds:.1f}s")
        table.add_row("Errors", str(len(result.errors)))
        console.print(table)

        if result.errors:
            console.print("\n[bold red]Errors:[/bold red]")
            for err in result.errors[:10]:
                console.print(f"  [red]- {err}[/red]")

    except typer.Exit:
        raise
    except Exception as exc:
        err_console.print(f"Scrape failed: {exc}")
        logger.exception("CLI scrape command failed")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Command: parse
# ---------------------------------------------------------------------------


@app.command("parse")
def parse(
    filing_id: Optional[str] = typer.Option(
        None, "--filing-id", help="Parse a specific filing by UUID"
    ),
    state: Optional[str] = typer.Option(
        None, "--state", help="Parse all unparsed filings for a state"
    ),
    unparsed_only: bool = typer.Option(
        True, "--unparsed-only/--all", help="Only parse unparsed documents"
    ),
) -> None:
    """Run the filing document parser.

    Examples:

      hermes parse --filing-id 550e8400-e29b-41d4-a716-446655440000

      hermes parse --state TX

      hermes parse --state CA --all
    """
    console.print(
        Panel(
            f"[bold cyan]Hermes Filing Parser[/bold cyan]\n"
            f"Filing ID: [yellow]{filing_id or 'all'}[/yellow]  "
            f"State: [yellow]{state or 'all'}[/yellow]  "
            f"Unparsed only: [yellow]{unparsed_only}[/yellow]",
            title="Parse",
            expand=False,
        )
    )

    try:
        result = _run(_parse_async(filing_id, state, unparsed_only))

        table = Table(title="Parse Results", box=box.ROUNDED)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        table.add_row("Documents Parsed", str(result["documents_parsed"]))
        table.add_row("Documents Failed", str(result["documents_failed"]))
        table.add_row("Errors", str(len(result.get("errors", []))))
        console.print(table)

        if result.get("errors"):
            console.print("\n[bold red]Errors:[/bold red]")
            for err in result["errors"][:5]:
                console.print(f"  [red]- {err}[/red]")

    except Exception as exc:
        err_console.print(f"Parse failed: {exc}")
        logger.exception("CLI parse command failed")
        raise typer.Exit(1)


async def _parse_async(
    filing_id: Optional[str],
    state: Optional[str],
    unparsed_only: bool,
) -> dict:
    """Async backend for the parse CLI command."""
    from hermes.parsers import DocumentClassifier, RateParser, RuleParser, FormParser
    from hermes.db import async_session
    from sqlalchemy import text

    summary = {"documents_parsed": 0, "documents_failed": 0, "errors": []}

    async with async_session() as session:
        conditions = []
        params_dict: dict = {}

        if filing_id:
            conditions.append("fd.filing_id = :filing_id")
            params_dict["filing_id"] = filing_id
        if state:
            conditions.append("f.state = :state")
            params_dict["state"] = state.upper()
        if unparsed_only:
            conditions.append("fd.parsed_flag = FALSE")

        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        join_clause = "JOIN hermes_filings f ON f.id = fd.filing_id" if state or filing_id else ""

        stmt = text(
            f"""
            SELECT fd.id, fd.file_path, fd.document_type
            FROM hermes_filing_documents fd
            {join_clause}
            {where_clause}
            AND fd.file_path IS NOT NULL
            ORDER BY fd.created_at ASC
            LIMIT 200
            """
        )
        result = await session.execute(stmt, params_dict)
        docs = result.fetchall()

    classifier = DocumentClassifier()
    rate_parser = RateParser()
    rule_parser = RuleParser()
    form_parser = FormParser()

    for doc in docs:
        try:
            doc_type = doc.document_type
            if not doc_type:
                doc_type = await classifier.classify(doc.file_path)

            if doc_type == "rate":
                parse_result = await rate_parser.parse(doc.id, doc.file_path)
            elif doc_type == "rule":
                parse_result = await rule_parser.parse(doc.id, doc.file_path)
            elif doc_type == "form":
                parse_result = await form_parser.parse(doc.id, doc.file_path)
            else:
                continue

            if parse_result.status in ("completed", "partial"):
                async with async_session() as s:
                    await s.execute(
                        text(
                            "UPDATE hermes_filing_documents SET parsed_flag=TRUE, "
                            "parse_confidence=:conf, updated_at=NOW() WHERE id=:id"
                        ),
                        {"id": str(doc.id), "conf": parse_result.confidence_avg},
                    )
                    await s.commit()
                summary["documents_parsed"] += 1
            else:
                summary["documents_failed"] += 1

        except Exception as exc:
            summary["errors"].append(f"doc={doc.id}: {exc}")
            summary["documents_failed"] += 1

    return summary


# ---------------------------------------------------------------------------
# Command: match
# ---------------------------------------------------------------------------


@app.command("match")
def match(
    naics: str = typer.Option(..., "--naics", help="NAICS code"),
    state: str = typer.Option(..., "--state", help="State code"),
    line: str = typer.Option(..., "--line", help="Coverage line(s), comma-separated"),
    zip_code: Optional[str] = typer.Option(None, "--zip", help="ZIP code"),
    years: Optional[int] = typer.Option(None, "--years", help="Years in business"),
    revenue: Optional[float] = typer.Option(None, "--revenue", help="Annual revenue"),
) -> None:
    """Run carrier matching for a risk profile.

    Examples:

      hermes match --naics 5411 --state TX --line "Commercial Auto,GL"

      hermes match --naics 7389 --state CA --line "GL" --zip 90210 --years 5 --revenue 2000000
    """
    console.print(
        Panel(
            f"[bold cyan]Hermes Carrier Matcher[/bold cyan]\n"
            f"NAICS: [yellow]{naics}[/yellow]  "
            f"State: [yellow]{state.upper()}[/yellow]  "
            f"Lines: [yellow]{line}[/yellow]",
            title="Match",
            expand=False,
        )
    )

    try:
        from hermes.matching import MatchingEngine

        lines = [l.strip() for l in line.split(",")]

        risk_profile = {
            "naics_code": naics,
            "state": state.upper(),
            "lines": lines,
            "zip_code": zip_code,
            "years_in_business": years,
            "annual_revenue": revenue,
        }

        with console.status("[bold green]Running carrier matching...[/bold green]"):
            matches = _run(
                MatchingEngine().match(risk_profile)  # type: ignore[attr-defined]
            )

        if not matches:
            console.print("[yellow]No carriers found for this risk profile.[/yellow]")
            return

        table = Table(title=f"Carrier Matches — {state.upper()} | {line}", box=box.ROUNDED)
        table.add_column("Rank", style="dim", width=6)
        table.add_column("Carrier", style="cyan")
        table.add_column("Appetite Score", justify="right")
        table.add_column("Competitiveness", justify="right")
        table.add_column("Eligible Classes", justify="right")

        for i, m in enumerate(matches[:20], 1):
            table.add_row(
                str(i),
                str(getattr(m, "carrier_name", m.get("carrier_name", ""))),
                str(getattr(m, "appetite_score", m.get("appetite_score", "N/A"))),
                str(getattr(m, "competitiveness_index", m.get("competitiveness_index", "N/A"))),
                str(getattr(m, "eligible_class_count", m.get("eligible_class_count", "N/A"))),
            )

        console.print(table)

    except Exception as exc:
        err_console.print(f"Match failed: {exc}")
        logger.exception("CLI match command failed")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Command: monitor
# ---------------------------------------------------------------------------


@app.command("monitor")
def monitor(
    state: Optional[str] = typer.Option(None, "--state", help="Filter by state code"),
    since: Optional[str] = typer.Option(
        None, "--since", help="Check from date (YYYY-MM-DD, default: yesterday)"
    ),
) -> None:
    """Check for appetite shifts in recently parsed filings.

    Examples:

      hermes monitor

      hermes monitor --state TX --since 2025-01-01
    """
    since_date = date.today() - timedelta(days=1)
    if since:
        try:
            since_date = date.fromisoformat(since)
        except ValueError:
            err_console.print(f"Invalid date: {since}. Use YYYY-MM-DD.")
            raise typer.Exit(1)

    console.print(
        Panel(
            f"[bold cyan]Appetite Shift Monitor[/bold cyan]\n"
            f"State filter: [yellow]{state or 'all'}[/yellow]  "
            f"Since: [yellow]{since_date}[/yellow]",
            title="Monitor",
            expand=False,
        )
    )

    try:
        from hermes.monitoring import ChangeDetector

        with console.status("[bold green]Detecting appetite shifts...[/bold green]"):
            shifts = _run(_monitor_async(state, since_date))

        if not shifts:
            console.print("[green]No appetite shifts detected for the specified filters.[/green]")
            return

        table = Table(
            title=f"Appetite Shifts Detected ({len(shifts)} total)", box=box.ROUNDED
        )
        table.add_column("Carrier", style="cyan")
        table.add_column("State", width=6)
        table.add_column("Line", style="dim")
        table.add_column("Signal Type", style="magenta")
        table.add_column("Strength", justify="right", width=8)
        table.add_column("Description")

        for shift in sorted(shifts, key=lambda s: s.signal_strength, reverse=True):
            strength_color = (
                "red" if shift.signal_strength >= 7
                else "yellow" if shift.signal_strength >= 4
                else "green"
            )
            table.add_row(
                shift.carrier_name[:30],
                shift.state,
                shift.line[:25],
                shift.signal_type,
                f"[{strength_color}]{shift.signal_strength}[/{strength_color}]",
                shift.description[:60] + ("..." if len(shift.description) > 60 else ""),
            )

        console.print(table)

    except Exception as exc:
        err_console.print(f"Monitor failed: {exc}")
        logger.exception("CLI monitor command failed")
        raise typer.Exit(1)


async def _monitor_async(state: Optional[str], since_date: date):
    """Async backend for the monitor CLI command."""
    from hermes.monitoring import ChangeDetector
    from hermes.db import async_session
    from sqlalchemy import text

    detector = ChangeDetector()

    if state:
        # Only run for the specified state
        async with async_session() as session:
            stmt = text(
                """
                SELECT DISTINCT f.carrier_id, f.state, f.line_of_business AS line
                FROM hermes_filings f
                JOIN hermes_filing_documents fd ON fd.filing_id = f.id
                WHERE fd.parsed_flag = TRUE
                  AND f.state = :state
                  AND f.carrier_id IS NOT NULL
                  AND f.updated_at >= :since_date
                """
            )
            result = await session.execute(stmt, {"state": state.upper(), "since_date": since_date})
            combos = result.fetchall()

        all_shifts = []
        for row in combos:
            try:
                shifts = await detector.detect_shifts(row.carrier_id, row.state, row.line)
                all_shifts.extend(shifts)
            except Exception as exc:
                logger.error("Shift detection error: %s", exc)
        return all_shifts
    else:
        return await detector.detect_all_shifts(since_date=since_date)


# ---------------------------------------------------------------------------
# Command: alerts
# ---------------------------------------------------------------------------


@app.command("alerts")
def alerts_cmd(
    unread: bool = typer.Option(False, "--unread", is_flag=True, help="Show only unread alerts"),
    acknowledge: Optional[str] = typer.Option(
        None, "--acknowledge", help="Alert UUID to acknowledge"
    ),
) -> None:
    """View and acknowledge appetite shift alerts.

    Examples:

      hermes alerts --unread

      hermes alerts --acknowledge 550e8400-e29b-41d4-a716-446655440000
    """
    try:
        from hermes.monitoring import AlertManager

        manager = AlertManager()

        if acknowledge:
            try:
                alert_uuid = UUID(acknowledge)
            except ValueError:
                err_console.print(f"Invalid UUID: {acknowledge}")
                raise typer.Exit(1)

            with console.status("[bold green]Acknowledging alert...[/bold green]"):
                _run(manager.acknowledge_alert(alert_uuid))
            console.print(f"[green]Alert {acknowledge} acknowledged.[/green]")
            return

        with console.status("[bold green]Loading alerts...[/bold green]"):
            all_alerts = _run(manager.get_unread_alerts()) if unread else _run(manager.get_unread_alerts())

        if not all_alerts:
            console.print("[green]No unread alerts.[/green]")
            return

        table = Table(
            title=f"Alerts — {'Unread only' if unread else 'All'} ({len(all_alerts)} total)",
            box=box.ROUNDED,
        )
        table.add_column("ID", style="dim", width=36)
        table.add_column("Severity", width=8)
        table.add_column("Type", width=15)
        table.add_column("Carrier", style="cyan")
        table.add_column("State", width=6)
        table.add_column("Line", style="dim", width=20)
        table.add_column("Description")

        severity_colors = {"high": "red", "medium": "yellow", "low": "green"}

        for alert in all_alerts[:50]:
            color = severity_colors.get(alert.severity, "white")
            table.add_row(
                str(alert.id),
                f"[{color}]{alert.severity.upper()}[/{color}]",
                alert.alert_type,
                alert.carrier_name[:25],
                alert.state,
                alert.line[:20],
                alert.description[:60] + ("..." if len(alert.description) > 60 else ""),
            )

        console.print(table)
        console.print(
            "\n[dim]Use --acknowledge <ID> to mark an alert as read.[/dim]"
        )

    except typer.Exit:
        raise
    except Exception as exc:
        err_console.print(f"Alerts command failed: {exc}")
        logger.exception("CLI alerts command failed")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Command: market-report
# ---------------------------------------------------------------------------


@app.command("market-report")
def market_report(
    state: str = typer.Option(..., "--state", help="State code (required)"),
    line: str = typer.Option(..., "--line", help="Line of business (required)"),
    period: int = typer.Option(30, "--period", help="Period in days (default: 30)"),
) -> None:
    """Generate or view a market intelligence report for a state/line.

    Examples:

      hermes market-report --state TX --line "Commercial Auto"

      hermes market-report --state CA --line "GL" --period 90
    """
    console.print(
        Panel(
            f"[bold cyan]Market Intelligence Report[/bold cyan]\n"
            f"State: [yellow]{state.upper()}[/yellow]  "
            f"Line: [yellow]{line}[/yellow]  "
            f"Period: [yellow]{period} days[/yellow]",
            title="Market Report",
            expand=False,
        )
    )

    try:
        from hermes.monitoring import MarketReportGenerator

        generator = MarketReportGenerator()

        with console.status("[bold green]Generating market report...[/bold green]"):
            report = _run(generator.generate_report(state=state.upper(), line=line, period_days=period))

        # Trend badge colours
        trend_colors = {
            "hardening": "red",
            "softening": "blue",
            "stable": "green",
            "mixed": "yellow",
        }
        trend_color = trend_colors.get(report.market_trend, "white")

        # Summary panel
        console.print(
            Panel(
                report.summary,
                title=f"[bold]Market Summary — {state.upper()} {line}[/bold]",
                expand=False,
            )
        )

        # Stats table
        stats = Table(title="Key Metrics", box=box.SIMPLE)
        stats.add_column("Metric", style="cyan")
        stats.add_column("Value", style="bold", justify="right")
        stats.add_row("Period", f"{report.period_start} to {report.period_end}")
        stats.add_row("Market Trend", f"[{trend_color}]{report.market_trend.upper()}[/{trend_color}]")
        stats.add_row("Total Filings", str(report.filing_count))
        stats.add_row(
            "Avg Rate Change",
            f"{report.avg_rate_change:+.2f}%" if report.avg_rate_change is not None else "N/A",
        )
        stats.add_row(
            "Median Rate Change",
            f"{report.median_rate_change:+.2f}%" if report.median_rate_change is not None else "N/A",
        )
        stats.add_row("Rate Increases", str(report.rate_increases))
        stats.add_row("Rate Decreases", str(report.rate_decreases))
        stats.add_row("New Entrants", str(len(report.new_entrants)))
        stats.add_row("Withdrawals", str(len(report.withdrawals)))
        console.print(stats)

        if report.new_entrants:
            console.print("\n[bold green]New Entrants:[/bold green]")
            for name in report.new_entrants:
                console.print(f"  + {name}")

        if report.withdrawals:
            console.print("\n[bold red]Withdrawals:[/bold red]")
            for name in report.withdrawals:
                console.print(f"  - {name}")

        if report.top_signals:
            sig_table = Table(title="Top Appetite Signals", box=box.SIMPLE)
            sig_table.add_column("Carrier", style="cyan")
            sig_table.add_column("Signal", style="magenta")
            sig_table.add_column("Strength", justify="right")
            sig_table.add_column("Date", width=12)
            sig_table.add_column("Description")
            for sig in report.top_signals[:10]:
                sig_table.add_row(
                    sig.get("carrier_name", "")[:25],
                    sig.get("signal_type", ""),
                    str(sig.get("signal_strength", "")),
                    sig.get("signal_date", ""),
                    sig.get("description", "")[:60],
                )
            console.print(sig_table)

    except Exception as exc:
        err_console.print(f"Market report failed: {exc}")
        logger.exception("CLI market-report command failed")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Command: scrape-status
# ---------------------------------------------------------------------------


@app.command("scrape-status")
def scrape_status() -> None:
    """Show a summary table of recent scrape log entries."""
    try:
        from hermes.db import async_session
        from sqlalchemy import text

        async def _fetch():
            async with async_session() as session:
                result = await session.execute(
                    text(
                        """
                        SELECT state, scrape_type, status,
                               filings_found, filings_new, documents_downloaded,
                               duration_seconds, started_at, error_message
                        FROM hermes_scrape_log
                        ORDER BY started_at DESC
                        LIMIT 50
                        """
                    )
                )
                return result.fetchall()

        rows = _run(_fetch())

        if not rows:
            console.print("[yellow]No scrape log entries found.[/yellow]")
            return

        table = Table(title="Scrape Log Summary", box=box.ROUNDED)
        table.add_column("State", width=6)
        table.add_column("Type", width=12)
        table.add_column("Status", width=12)
        table.add_column("Found", justify="right", width=8)
        table.add_column("New", justify="right", width=8)
        table.add_column("Docs", justify="right", width=8)
        table.add_column("Duration", justify="right", width=10)
        table.add_column("Started At", width=20)

        status_colors = {
            "completed": "green",
            "failed": "red",
            "running": "yellow",
            "partial": "orange3",
        }

        for row in rows:
            color = status_colors.get(row.status, "white")
            duration_str = f"{row.duration_seconds:.1f}s" if row.duration_seconds else "N/A"
            started_str = (
                row.started_at.strftime("%Y-%m-%d %H:%M")
                if row.started_at else "N/A"
            )
            table.add_row(
                row.state,
                row.scrape_type,
                f"[{color}]{row.status}[/{color}]",
                str(row.filings_found),
                str(row.filings_new),
                str(row.documents_downloaded),
                duration_str,
                started_str,
            )

        console.print(table)

    except Exception as exc:
        err_console.print(f"scrape-status failed: {exc}")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Command: parse-status
# ---------------------------------------------------------------------------


@app.command("parse-status")
def parse_status() -> None:
    """Show a summary table of recent parse log entries."""
    try:
        from hermes.db import async_session
        from sqlalchemy import text

        async def _fetch():
            async with async_session() as session:
                result = await session.execute(
                    text(
                        """
                        SELECT parser_type, status,
                               tables_extracted, rules_extracted,
                               forms_extracted, confidence_avg,
                               ai_calls_made, duration_seconds,
                               started_at, error_message
                        FROM hermes_parse_log
                        ORDER BY started_at DESC
                        LIMIT 50
                        """
                    )
                )
                return result.fetchall()

        rows = _run(_fetch())

        if not rows:
            console.print("[yellow]No parse log entries found.[/yellow]")
            return

        table = Table(title="Parse Log Summary", box=box.ROUNDED)
        table.add_column("Parser", width=12)
        table.add_column("Status", width=10)
        table.add_column("Tables", justify="right", width=7)
        table.add_column("Rules", justify="right", width=7)
        table.add_column("Forms", justify="right", width=7)
        table.add_column("Confidence", justify="right", width=10)
        table.add_column("AI Calls", justify="right", width=9)
        table.add_column("Duration", justify="right", width=10)
        table.add_column("Started At", width=20)

        status_colors = {
            "completed": "green",
            "failed": "red",
            "running": "yellow",
            "partial": "orange3",
        }

        for row in rows:
            color = status_colors.get(row.status, "white")
            conf_str = f"{float(row.confidence_avg):.3f}" if row.confidence_avg else "N/A"
            dur_str = f"{float(row.duration_seconds):.2f}s" if row.duration_seconds else "N/A"
            started_str = (
                row.started_at.strftime("%Y-%m-%d %H:%M") if row.started_at else "N/A"
            )
            table.add_row(
                row.parser_type or "N/A",
                f"[{color}]{row.status}[/{color}]",
                str(row.tables_extracted),
                str(row.rules_extracted),
                str(row.forms_extracted),
                conf_str,
                str(row.ai_calls_made),
                dur_str,
                started_str,
            )

        console.print(table)

    except Exception as exc:
        err_console.print(f"parse-status failed: {exc}")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Command: sources
# ---------------------------------------------------------------------------


@app.command("sources")
def sources(
    state: Optional[str] = typer.Option(None, "--state", help="Filter by state code"),
) -> None:
    """List all carriers with their filing counts.

    Examples:

      hermes sources

      hermes sources --state TX
    """
    try:
        from hermes.db import async_session
        from sqlalchemy import text

        async def _fetch():
            async with async_session() as session:
                where = "WHERE f.state = :state" if state else ""
                params = {"state": state.upper()} if state else {}
                result = await session.execute(
                    text(
                        f"""
                        SELECT
                            c.legal_name,
                            c.naic_code,
                            c.domicile_state,
                            COUNT(f.id) AS filing_count,
                            COUNT(DISTINCT f.state) AS states_count,
                            MAX(f.filed_date) AS last_filing_date
                        FROM hermes_carriers c
                        LEFT JOIN hermes_filings f ON f.carrier_id = c.id
                        {where}
                        GROUP BY c.id, c.legal_name, c.naic_code, c.domicile_state
                        HAVING COUNT(f.id) > 0
                        ORDER BY filing_count DESC
                        LIMIT 100
                        """
                    ),
                    params,
                )
                return result.fetchall()

        rows = _run(_fetch())

        if not rows:
            console.print("[yellow]No carriers with filings found.[/yellow]")
            return

        title = f"Carriers with Filings{f' — {state.upper()}' if state else ''}"
        table = Table(title=title, box=box.ROUNDED)
        table.add_column("Carrier Name", style="cyan")
        table.add_column("NAIC", width=8)
        table.add_column("Domicile", width=8)
        table.add_column("Filings", justify="right", width=8)
        table.add_column("States", justify="right", width=7)
        table.add_column("Last Filing", width=12)

        for row in rows:
            last_filing = str(row.last_filing_date) if row.last_filing_date else "N/A"
            table.add_row(
                row.legal_name[:40],
                row.naic_code or "N/A",
                row.domicile_state or "N/A",
                str(row.filing_count),
                str(row.states_count),
                last_filing,
            )

        console.print(table)

    except Exception as exc:
        err_console.print(f"sources command failed: {exc}")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Command: health
# ---------------------------------------------------------------------------


@app.command("health")
def health() -> None:
    """Run a system health check and display status.

    Verifies database connectivity, parse backlog, stuck scrape jobs, and
    unacknowledged high-severity alerts.
    """
    console.print(Panel("[bold cyan]Hermes System Health Check[/bold cyan]", expand=False))

    try:
        from hermes.tasks import _health_check_async

        with console.status("[bold green]Running health checks...[/bold green]"):
            report = _run(_health_check_async())

        status = report.get("status", "unknown")
        status_colors = {"healthy": "green", "degraded": "yellow", "unhealthy": "red"}
        status_color = status_colors.get(status, "white")

        console.print(f"\nOverall Status: [{status_color}]{status.upper()}[/{status_color}]")

        table = Table(box=box.SIMPLE)
        table.add_column("Check", style="cyan")
        table.add_column("Result", style="bold")

        table.add_row("Database", str(report.get("database", "N/A")))
        table.add_row("Unparsed Documents", str(report.get("unparsed_documents", "N/A")))
        table.add_row("Stuck Scrape Jobs", str(report.get("stuck_scrapes", "N/A")))
        table.add_row("High Severity Alerts", str(report.get("unacknowledged_high_alerts", "N/A")))
        table.add_row("Checked At", str(report.get("timestamp", "N/A")))

        console.print(table)

        issues = report.get("issues", [])
        if issues:
            console.print("\n[bold yellow]Issues:[/bold yellow]")
            for issue in issues:
                console.print(f"  [yellow]- {issue}[/yellow]")
        else:
            console.print("[green]No issues detected.[/green]")

    except Exception as exc:
        err_console.print(f"Health check failed: {exc}")
        logger.exception("CLI health command failed")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Command: scrape-title
# ---------------------------------------------------------------------------


@app.command("scrape-title")
def scrape_title(
    state: str = typer.Option("TX", "--state", "-s", help="State code (default: TX)"),
) -> None:
    """Scrape title insurance rate filings from SERFF or state DOI.

    For promulgated states (TX, NM), loads state-set rates directly.
    For other states, runs SERFF title-specific search.

    Examples:

      hermes scrape-title --state TX

      hermes scrape-title --state NY
    """
    console.print(
        Panel(
            f"[bold cyan]Hermes Title Insurance Scraper[/bold cyan]\n"
            f"State: [yellow]{state.upper()}[/yellow]",
            title="Title Scrape",
            expand=False,
        )
    )

    try:
        from hermes.scraper.title_search import is_promulgated_state

        state_upper = state.upper()

        # ── Dispatch to state-specific loaders ──
        _STATE_LOADERS = {
            "TX": ("hermes.scraper.tdi_scraper", "load_tx_promulgated_rates", "TDI Basic Manual", True),
            "FL": ("hermes.scraper.state_title_rates", "load_fl_promulgated_rates", "Florida OIR", True),
            "NM": ("hermes.scraper.state_title_rates", "load_nm_promulgated_rates", "NM OSI", True),
            "NY": ("hermes.scraper.state_title_rates", "load_ny_filed_rates", "NY DFS (filed)", False),
            "CA": ("hermes.scraper.state_title_rates", "load_ca_filed_rates", "CA CDI (filed)", False),
        }

        if state_upper in _STATE_LOADERS:
            module_path, func_name, source_label, promulgated = _STATE_LOADERS[state_upper]

            import importlib
            mod = importlib.import_module(module_path)
            loader_fn = getattr(mod, func_name)

            rate_type = "promulgated" if promulgated else "filed"
            console.print(
                f"[yellow]{state_upper} has {rate_type} title rates — loading directly[/yellow]"
            )

            with console.status(f"[bold green]Loading {state_upper} {rate_type} rates...[/bold green]"):
                results = _run(loader_fn())

            table = Table(title=f"{state_upper} Title Rates Loaded", box=box.ROUNDED)
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green", justify="right")
            table.add_row("Rate Cards Created", str(len(results)))
            table.add_row("Rate Type", "Promulgated" if promulgated else "Filed (per-carrier)")
            table.add_row("Source", source_label)
            console.print(table)

        elif is_promulgated_state(state_upper):
            console.print(f"[yellow]Promulgated rate loader for {state_upper} not yet implemented[/yellow]")

        else:
            # Non-promulgated, no built-in loader — SERFF search
            from hermes.scraper.title_search import build_title_search_params
            from hermes.tasks import _get_scraper_for_state

            params = build_title_search_params(state_upper)
            if params is None:
                err_console.print(f"No SERFF search available for {state_upper} title filings")
                raise typer.Exit(1)

            scraper = _get_scraper_for_state(state_upper)
            if scraper is None:
                err_console.print(f"No scraper implementation for state '{state_upper}'")
                raise typer.Exit(1)

            with console.status(f"[bold green]Scraping {state_upper} title filings...[/bold green]"):
                result = _run(scraper.scrape(params))

            table = Table(title="Title Scrape Results", box=box.ROUNDED)
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green", justify="right")
            table.add_row("State", state_upper)
            table.add_row("Filings Found", str(result.filings_found))
            table.add_row("Filings New", str(result.filings_new))
            table.add_row("Documents Downloaded", str(result.documents_downloaded))
            table.add_row("Duration", f"{result.duration_seconds:.1f}s")
            console.print(table)

    except typer.Exit:
        raise
    except Exception as exc:
        err_console.print(f"Title scrape failed: {exc}")
        logger.exception("CLI scrape-title command failed")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Command: parse-title
# ---------------------------------------------------------------------------


@app.command("parse-title")
def parse_title(
    filing_id: Optional[str] = typer.Option(
        None, "--filing-id", help="Parse a specific filing by UUID"
    ),
    state: Optional[str] = typer.Option(
        None, "--state", help="Parse all unparsed title filings for a state"
    ),
) -> None:
    """Parse title insurance rate exhibits from SERFF filing PDFs.

    Examples:

      hermes parse-title --filing-id 550e8400-e29b-41d4-a716-446655440000

      hermes parse-title --state NY
    """
    console.print(
        Panel(
            f"[bold cyan]Hermes Title Rate Parser[/bold cyan]\n"
            f"Filing ID: [yellow]{filing_id or 'all'}[/yellow]  "
            f"State: [yellow]{state or 'all'}[/yellow]",
            title="Title Parse",
            expand=False,
        )
    )

    try:
        result = _run(_parse_title_async(filing_id, state))

        table = Table(title="Title Parse Results", box=box.ROUNDED)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        table.add_row("Documents Parsed", str(result["documents_parsed"]))
        table.add_row("Documents Failed", str(result["documents_failed"]))
        table.add_row("Errors", str(len(result.get("errors", []))))
        console.print(table)

        if result.get("errors"):
            console.print("\n[bold red]Errors:[/bold red]")
            for err in result["errors"][:5]:
                console.print(f"  [red]- {err}[/red]")

    except Exception as exc:
        err_console.print(f"Title parse failed: {exc}")
        logger.exception("CLI parse-title command failed")
        raise typer.Exit(1)


async def _parse_title_async(
    filing_id: Optional[str],
    state: Optional[str],
) -> dict:
    """Async backend for the parse-title CLI command."""
    from hermes.parsers.title_parser import TitleParser
    from hermes.db import async_session
    from sqlalchemy import text

    summary = {"documents_parsed": 0, "documents_failed": 0, "errors": []}

    async with async_session() as session:
        conditions = ["fd.parsed_flag = FALSE", "fd.file_path IS NOT NULL"]
        params_dict: dict = {}

        if filing_id:
            conditions.append("fd.filing_id = :filing_id")
            params_dict["filing_id"] = filing_id
        if state:
            conditions.append("f.state = :state")
            params_dict["state"] = state.upper()

        # Filter for title insurance filings
        conditions.append("f.line_of_business ILIKE '%title%'")

        where_clause = "WHERE " + " AND ".join(conditions)

        stmt = text(
            f"""
            SELECT fd.id, fd.file_path, fd.document_type
            FROM hermes_filing_documents fd
            JOIN hermes_filings f ON f.id = fd.filing_id
            {where_clause}
            ORDER BY fd.created_at ASC
            LIMIT 100
            """
        )
        result = await session.execute(stmt, params_dict)
        docs = result.fetchall()

    title_parser = TitleParser()

    for doc in docs:
        try:
            parse_result = await title_parser.parse(doc.id, doc.file_path)
            if parse_result.status in ("completed", "partial"):
                async with async_session() as s:
                    await s.execute(
                        text(
                            "UPDATE hermes_filing_documents SET parsed_flag=TRUE, "
                            "parse_confidence=:conf, updated_at=NOW() WHERE id=:id"
                        ),
                        {"id": str(doc.id), "conf": parse_result.confidence_avg},
                    )
                    await s.commit()
                summary["documents_parsed"] += 1
            else:
                summary["documents_failed"] += 1
        except Exception as exc:
            summary["errors"].append(f"doc={doc.id}: {exc}")
            summary["documents_failed"] += 1

    return summary


# ---------------------------------------------------------------------------
# Command: price-title
# ---------------------------------------------------------------------------


@app.command("price-title")
def price_title(
    purchase_price: float = typer.Option(..., "--purchase-price", "-p", help="Purchase price in USD"),
    loan_amount: float = typer.Option(0, "--loan-amount", "-l", help="Loan amount in USD"),
    state: str = typer.Option("TX", "--state", "-s", help="State code"),
    refinance: bool = typer.Option(False, "--refinance", is_flag=True, help="Refinance transaction"),
    years_since: Optional[float] = typer.Option(None, "--years-since", help="Years since prior policy"),
) -> None:
    """Run a title insurance multi-carrier quote.

    Examples:

      hermes price-title --purchase-price 400000 --loan-amount 380000 --state TX

      hermes price-title -p 250000 -l 200000 -s NY

      hermes price-title -p 500000 -l 475000 -s TX --refinance --years-since 3
    """
    console.print(
        Panel(
            f"[bold cyan]Hermes Title Insurance Pricer[/bold cyan]\n"
            f"Purchase: [yellow]${purchase_price:,.0f}[/yellow]  "
            f"Loan: [yellow]${loan_amount:,.0f}[/yellow]  "
            f"State: [yellow]{state.upper()}[/yellow]",
            title="Title Quote",
            expand=False,
        )
    )

    try:
        from hermes.title.engine import HermesTitleEngine
        from hermes.title.schemas import TitleQuoteRequest

        policy_type = "simultaneous" if loan_amount > 0 else "owner"
        request = TitleQuoteRequest(
            purchase_price=purchase_price,
            loan_amount=loan_amount,
            state=state.upper(),
            policy_type=policy_type,
            is_refinance=refinance,
            years_since_prior_policy=years_since,
        )

        with console.status("[bold green]Running title quote...[/bold green]"):
            response = _run(_price_title_async(request))

        if not response.quotes:
            console.print("[yellow]No carriers returned quotes. Check that rate cards are loaded.[/yellow]")
            console.print("[dim]Hint: Run 'hermes scrape-title --state TX' to load TX promulgated rates.[/dim]")
            return

        table = Table(
            title=f"Title Insurance Quotes — {state.upper()} | ${purchase_price:,.0f} / ${loan_amount:,.0f}",
            box=box.ROUNDED,
        )
        table.add_column("Rank", style="dim", width=5)
        table.add_column("Carrier", style="cyan")
        table.add_column("Owner", justify="right", width=10)
        table.add_column("Lender", justify="right", width=10)
        table.add_column("Simul. Total", justify="right", width=12)
        table.add_column("Savings", justify="right", width=10, style="green")
        table.add_column("Total", justify="right", width=12, style="bold")
        table.add_column("Promulgated", width=11)

        for i, q in enumerate(response.quotes, 1):
            table.add_row(
                str(i),
                q.carrier_name[:30],
                f"${q.owner_premium:,.0f}",
                f"${q.lender_premium:,.0f}",
                f"${q.simultaneous_premium:,.0f}" if q.simultaneous_premium > 0 else "—",
                f"${q.simultaneous_savings:,.0f}" if q.simultaneous_savings > 0 else "—",
                f"${q.total_premium:,.0f}",
                "[green]Yes[/green]" if q.is_promulgated else "No",
            )

        console.print(table)

        # Summary
        console.print(f"\n  Carriers quoted: {response.carriers_quoted}")
        console.print(f"  Processing time: {response.processing_time_ms:.1f}ms")
        if response.best_total:
            console.print(
                f"  [bold green]Best total: ${response.best_total.total_premium:,.0f} "
                f"({response.best_total.carrier_name})[/bold green]"
            )
        if response.best_simultaneous_savings:
            console.print(
                f"  [bold cyan]Max simul. savings: ${response.best_simultaneous_savings.simultaneous_savings:,.0f} "
                f"({response.best_simultaneous_savings.carrier_name})[/bold cyan]"
            )

    except Exception as exc:
        err_console.print(f"Title pricing failed: {exc}")
        logger.exception("CLI price-title command failed")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Command: match-title
# ---------------------------------------------------------------------------


@app.command("match-title")
def match_title(
    purchase_price: float = typer.Option(..., "--purchase-price", "-p", help="Purchase price in USD"),
    loan_amount: float = typer.Option(0, "--loan-amount", "-l", help="Loan amount in USD"),
    state: str = typer.Option(..., "--state", "-s", help="State code (e.g. TX, CA)"),
    policy_type: Optional[str] = typer.Option(None, "--policy-type", help='Policy type: owner/lender/simultaneous'),
    refinance: bool = typer.Option(False, "--refinance", is_flag=True, help="Refinance transaction"),
    years_since: Optional[float] = typer.Option(None, "--years-since", help="Years since prior policy"),
) -> None:
    """Run title carrier matching for a title insurance risk.

    Matches a title insurance risk through the Hermes matching pipeline
    (eligibility → appetite → premium → ranking) and displays ranked results.

    Examples:

      hermes match-title --purchase-price 400000 --loan-amount 380000 --state TX

      hermes match-title -p 250000 -l 200000 -s NY --policy-type simultaneous

      hermes match-title -p 500000 -s TX --refinance --years-since 3
    """
    effective_policy_type = policy_type or ("simultaneous" if loan_amount > 0 else "owner")

    console.print(
        Panel(
            f"[bold cyan]Hermes Title Carrier Matcher[/bold cyan]\n"
            f"Purchase: [yellow]${purchase_price:,.0f}[/yellow]  "
            f"Loan: [yellow]${loan_amount:,.0f}[/yellow]  "
            f"State: [yellow]{state.upper()}[/yellow]  "
            f"Type: [yellow]{effective_policy_type}[/yellow]",
            title="Title Match",
            expand=False,
        )
    )

    try:
        risk_profile = {
            "purchase_price": purchase_price,
            "loan_amount": loan_amount,
            "state": state.upper(),
            "policy_type": effective_policy_type,
            "is_refinance": refinance,
            "years_since_prior_policy": years_since,
            "endorsements": [],
        }

        with console.status("[bold green]Running title carrier matching...[/bold green]"):
            matches = _run(_match_title_async(risk_profile, state.upper()))

        if not matches:
            console.print("[yellow]No title carriers found for this state.[/yellow]")
            console.print("[dim]Hint: Run 'hermes scrape-title --state TX' to load rate cards.[/dim]")
            return

        table = Table(
            title=f"Title Carrier Matches — {state.upper()} | ${purchase_price:,.0f} / ${loan_amount:,.0f}",
            box=box.ROUNDED,
        )
        table.add_column("Rank", style="dim", width=5)
        table.add_column("Carrier", style="cyan")
        table.add_column("Appetite", justify="right", width=9)
        table.add_column("Owner", justify="right", width=10)
        table.add_column("Lender", justify="right", width=10)
        table.add_column("Simul.", justify="right", width=10)
        table.add_column("Savings", justify="right", width=10, style="green")
        table.add_column("Total", justify="right", width=12, style="bold")
        table.add_column("Prob.", justify="right", width=7)

        for m in matches[:20]:
            components = m.premium.components or {}
            owner = float(components.get("owner_premium", 0))
            lender = float(components.get("lender_premium", 0))
            simul = float(components.get("simultaneous_premium", 0))
            savings = float(components.get("simultaneous_savings", 0))
            total = m.premium.final_estimated

            table.add_row(
                str(m.competitiveness_rank),
                m.carrier_name[:30],
                f"{m.appetite.score:.0f}",
                f"${owner:,.0f}" if owner > 0 else "—",
                f"${lender:,.0f}" if lender > 0 else "—",
                f"${simul:,.0f}" if simul > 0 else "—",
                f"${savings:,.0f}" if savings > 0 else "—",
                f"${total:,.0f}" if total > 0 else "—",
                f"{m.placement_probability:.0%}",
            )

        console.print(table)

        # Summary
        eligible_count = sum(1 for m in matches if m.eligibility.status != "fail")
        console.print(f"\n  Carriers evaluated: {len(matches)}")
        console.print(f"  Carriers eligible: {eligible_count}")
        if matches:
            best = min(matches, key=lambda m: m.premium.final_estimated if m.premium.final_estimated > 0 else float("inf"))
            if best.premium.final_estimated > 0:
                console.print(
                    f"  [bold green]Best total: ${best.premium.final_estimated:,.0f} "
                    f"({best.carrier_name})[/bold green]"
                )
            best_savings = max(matches, key=lambda m: float(m.premium.components.get("simultaneous_savings", 0)) if m.premium.components else 0)
            s = float(best_savings.premium.components.get("simultaneous_savings", 0)) if best_savings.premium.components else 0
            if s > 0:
                console.print(
                    f"  [bold cyan]Max simul. savings: ${s:,.0f} "
                    f"({best_savings.carrier_name})[/bold cyan]"
                )

    except Exception as exc:
        err_console.print(f"Title matching failed: {exc}")
        logger.exception("CLI match-title command failed")
        raise typer.Exit(1)


async def _match_title_async(risk_profile: dict, state: str):
    """Async backend for the match-title CLI command."""
    from hermes.matching.engine import MatchingEngine
    from hermes.title.engine import HermesTitleEngine

    title_engine = HermesTitleEngine()
    matching_engine = MatchingEngine()
    matching_engine.set_title_engine(title_engine)
    try:
        return await matching_engine.match_title(risk_profile, state)
    finally:
        await title_engine.close()
        await matching_engine.close()


async def _price_title_async(request):
    """Async backend for the price-title CLI command."""
    from hermes.title.engine import HermesTitleEngine
    engine = HermesTitleEngine()
    try:
        return await engine.price_policy(request)
    finally:
        await engine.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()
