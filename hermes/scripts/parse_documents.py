"""Parse downloaded SERFF filing documents into structured intelligence.

Usage:
    python -m hermes.scripts.parse_documents
    python -m hermes.scripts.parse_documents --limit 50
    python -m hermes.scripts.parse_documents --type rate
    python -m hermes.scripts.parse_documents --state NY --dry-run
    python -m hermes.scripts.parse_documents --reclassify

Queries hermes_filing_documents for unparsed PDFs, classifies them by type,
routes each to the appropriate parser (RateParser, RuleParser, FormParser),
and updates parsed_flag/parse_confidence on completion.
"""

import argparse
import asyncio
import logging
import os
import time
from pathlib import Path
from uuid import UUID

from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import create_async_engine

from hermes.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hermes.parse_documents")

# Map scraper doc types to parser-compatible types
DOC_TYPE_TO_PARSER = {
    "rate": "rate",
    "rate_exhibit": "rate",
    "actuarial_memo": "rate",
    "rule": "rule",
    "rule_manual": "rule",
    "form": "form",
    "policy_form": "form",
    "endorsement": "form",
    "application": "form",
    "schedule": "rate",  # Schedules often contain rate data
}


async def get_engine():
    """Create async engine."""
    return create_async_engine(settings.database_url, echo=False, pool_size=5)


async def get_unparsed_documents(
    engine,
    state: str | None = None,
    doc_type: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    """Query for documents that need parsing."""
    conditions = ["d.parsed_flag = FALSE", "d.file_path IS NOT NULL"]
    params: dict = {}

    if state:
        conditions.append("f.state = :state")
        params["state"] = state

    if doc_type:
        # Map to all matching types
        matching_types = [k for k, v in DOC_TYPE_TO_PARSER.items() if v == doc_type]
        matching_types.append(doc_type)
        conditions.append(
            "d.document_type IN ("
            + ", ".join(f"'{t}'" for t in set(matching_types))
            + ")"
        )

    where = " AND ".join(conditions)
    order = "ORDER BY f.state, d.document_type, d.created_at"
    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
        SELECT
            d.id as doc_id,
            d.filing_id,
            d.document_name,
            d.document_type,
            d.file_path,
            d.file_size_bytes,
            f.state,
            f.serff_tracking_number,
            f.carrier_name_filed,
            f.line_of_business
        FROM hermes_filing_documents d
        JOIN hermes_filings f ON d.filing_id = f.id
        WHERE {where}
        {order}
        {limit_clause}
    """

    async with engine.connect() as conn:
        result = await conn.execute(sa_text(query), params)
        rows = result.fetchall()
        return [dict(row._mapping) for row in rows]


async def reclassify_documents(engine, limit: int | None = None) -> int:
    """Re-classify documents using the 3-tier classifier.

    Updates document_type in hermes_filing_documents for documents
    whose type is 'other' or NULL.
    """
    from hermes.parsers.classifier import DocumentClassifier

    classifier = DocumentClassifier()

    query = """
        SELECT id, file_path, document_name, document_type
        FROM hermes_filing_documents
        WHERE (document_type = 'other' OR document_type IS NULL)
        AND file_path IS NOT NULL
    """
    if limit:
        query += f" LIMIT {limit}"

    async with engine.connect() as conn:
        result = await conn.execute(sa_text(query))
        docs = result.fetchall()

    updated = 0
    for doc in docs:
        file_path = doc.file_path
        if not Path(file_path).exists():
            logger.warning("File not found: %s", file_path)
            continue

        try:
            new_type = await classifier.classify(file_path)
            if new_type and new_type != doc.document_type:
                async with engine.begin() as conn:
                    await conn.execute(
                        sa_text(
                            "UPDATE hermes_filing_documents "
                            "SET document_type = :doc_type, updated_at = NOW() "
                            "WHERE id = CAST(:doc_id AS uuid)"
                        ),
                        {"doc_id": str(doc.id), "doc_type": new_type},
                    )
                updated += 1
                logger.info(
                    "Reclassified %s: %s → %s",
                    doc.document_name[:50], doc.document_type, new_type,
                )
        except Exception as exc:
            logger.warning("Classification failed for %s: %s", doc.document_name[:50], exc)

    return updated


async def parse_document(doc: dict) -> dict:
    """Parse a single document and return results."""
    from hermes.parsers.rate_parser import RateParser
    from hermes.parsers.rule_parser import RuleParser
    from hermes.parsers.form_parser import FormParser

    doc_type = doc["document_type"] or "other"
    parser_type = DOC_TYPE_TO_PARSER.get(doc_type)

    if not parser_type:
        return {
            "doc_id": str(doc["doc_id"]),
            "status": "skipped",
            "reason": f"No parser for type '{doc_type}'",
        }

    file_path = doc["file_path"]
    if not Path(file_path).exists():
        return {
            "doc_id": str(doc["doc_id"]),
            "status": "skipped",
            "reason": f"File not found: {file_path}",
        }

    # Only parse PDFs
    if not file_path.lower().endswith(".pdf"):
        return {
            "doc_id": str(doc["doc_id"]),
            "status": "skipped",
            "reason": f"Not a PDF: {file_path}",
        }

    parser_map = {
        "rate": RateParser,
        "rule": RuleParser,
        "form": FormParser,
    }

    parser_cls = parser_map[parser_type]
    parser = parser_cls()

    doc_id = UUID(str(doc["doc_id"]))
    result = await parser.parse(doc_id, file_path)

    return {
        "doc_id": str(doc["doc_id"]),
        "parser_type": parser_type,
        "status": result.status,
        "confidence_avg": result.confidence_avg,
        "confidence_min": result.confidence_min,
        "tables_extracted": result.tables_extracted,
        "rules_extracted": result.rules_extracted,
        "forms_extracted": result.forms_extracted,
        "factors_extracted": result.factors_extracted,
        "ai_calls": result.ai_calls_made,
        "ai_tokens": result.ai_tokens_used,
        "duration": result.duration_seconds,
        "errors": result.errors,
    }


async def update_document_status(engine, doc_id: str, parse_result: dict) -> None:
    """Update parsed_flag and parse_confidence after parsing."""
    status = parse_result["status"]
    confidence = parse_result.get("confidence_avg", 0.0)

    parsed_flag = status in ("completed", "partial")
    parse_version = "1.0.0"

    async with engine.begin() as conn:
        await conn.execute(
            sa_text("""
                UPDATE hermes_filing_documents SET
                    parsed_flag = :parsed,
                    parse_confidence = :confidence,
                    parse_version = :version,
                    updated_at = NOW()
                WHERE id = CAST(:doc_id AS uuid)
            """),
            {
                "doc_id": doc_id,
                "parsed": parsed_flag,
                "confidence": confidence,
                "version": parse_version,
            },
        )


async def main():
    parser = argparse.ArgumentParser(
        description="Parse downloaded SERFF filing documents"
    )
    parser.add_argument("--state", type=str, help="Filter by state (e.g., NY, CA)")
    parser.add_argument("--type", type=str, choices=["rate", "rule", "form"],
                        help="Filter by document type")
    parser.add_argument("--limit", type=int, help="Max documents to parse")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be parsed without actually parsing")
    parser.add_argument("--reclassify", action="store_true",
                        help="Re-classify 'other' documents before parsing")
    parser.add_argument("--concurrency", type=int, default=1,
                        help="Number of documents to parse concurrently (default: 1)")
    args = parser.parse_args()

    engine = await get_engine()

    # Reclassify first if requested
    if args.reclassify:
        logger.info("Reclassifying documents with type 'other'...")
        updated = await reclassify_documents(engine, limit=args.limit)
        logger.info("Reclassified %d documents", updated)
        if not args.type:
            return  # Just reclassify, don't parse

    # Get unparsed documents
    docs = await get_unparsed_documents(
        engine, state=args.state, doc_type=args.type, limit=args.limit,
    )

    if not docs:
        logger.info("No unparsed documents found matching criteria")
        return

    # Summary
    by_type: dict[str, int] = {}
    by_state: dict[str, int] = {}
    for d in docs:
        by_type[d["document_type"] or "unknown"] = by_type.get(d["document_type"] or "unknown", 0) + 1
        by_state[d["state"]] = by_state.get(d["state"], 0) + 1

    logger.info("=" * 60)
    logger.info("Found %d unparsed documents", len(docs))
    logger.info("  By type: %s", dict(sorted(by_type.items())))
    logger.info("  By state: %s", dict(sorted(by_state.items())))
    logger.info("=" * 60)

    if args.dry_run:
        for d in docs[:20]:
            pt = DOC_TYPE_TO_PARSER.get(d["document_type"] or "", "skip")
            logger.info(
                "  [%s] %s → %s parser | %s | %s",
                d["state"], d["document_type"], pt,
                d["serff_tracking_number"], d["document_name"][:60],
            )
        if len(docs) > 20:
            logger.info("  ... and %d more", len(docs) - 20)
        return

    # Parse documents
    start = time.monotonic()
    stats = {
        "parsed": 0, "failed": 0, "skipped": 0,
        "tables": 0, "rules": 0, "forms": 0, "factors": 0,
        "ai_calls": 0, "ai_tokens": 0,
    }

    sem = asyncio.Semaphore(args.concurrency)

    async def parse_with_semaphore(doc):
        async with sem:
            return await parse_document(doc)

    for i, doc in enumerate(docs):
        try:
            result = await parse_with_semaphore(doc)

            if result["status"] == "skipped":
                stats["skipped"] += 1
                logger.debug("Skipped %s: %s", doc["document_name"][:50], result.get("reason"))
                continue

            await update_document_status(engine, result["doc_id"], result)

            if result["status"] == "failed":
                stats["failed"] += 1
                logger.warning(
                    "[%d/%d] FAILED %s: %s",
                    i + 1, len(docs), doc["document_name"][:50],
                    result.get("errors", []),
                )
            else:
                stats["parsed"] += 1
                stats["tables"] += result.get("tables_extracted", 0)
                stats["rules"] += result.get("rules_extracted", 0)
                stats["forms"] += result.get("forms_extracted", 0)
                stats["factors"] += result.get("factors_extracted", 0)
                stats["ai_calls"] += result.get("ai_calls", 0)
                stats["ai_tokens"] += result.get("ai_tokens", 0)
                logger.info(
                    "[%d/%d] OK %s | %s parser | conf=%.2f | %.1fs | "
                    "tables=%d rules=%d forms=%d factors=%d",
                    i + 1, len(docs),
                    doc["serff_tracking_number"],
                    result.get("parser_type", "?"),
                    result.get("confidence_avg", 0),
                    result.get("duration", 0),
                    result.get("tables_extracted", 0),
                    result.get("rules_extracted", 0),
                    result.get("forms_extracted", 0),
                    result.get("factors_extracted", 0),
                )

        except Exception as exc:
            stats["failed"] += 1
            logger.error(
                "[%d/%d] ERROR %s: %s",
                i + 1, len(docs), doc["document_name"][:50], exc,
            )

        # Progress every 10 docs
        if (i + 1) % 10 == 0:
            elapsed = round(time.monotonic() - start, 1)
            rate = stats["parsed"] / max(elapsed, 1) * 3600
            logger.info(
                "Progress: %d/%d parsed, %d failed, %d skipped (%.0f/hr)",
                stats["parsed"], len(docs), stats["failed"], stats["skipped"], rate,
            )

    elapsed = round(time.monotonic() - start, 1)

    logger.info("=" * 60)
    logger.info("PARSING COMPLETE in %.1fs", elapsed)
    logger.info("  Parsed: %d", stats["parsed"])
    logger.info("  Failed: %d", stats["failed"])
    logger.info("  Skipped: %d", stats["skipped"])
    logger.info("  Tables extracted: %d", stats["tables"])
    logger.info("  Rules extracted: %d", stats["rules"])
    logger.info("  Forms extracted: %d", stats["forms"])
    logger.info("  Factors extracted: %d", stats["factors"])
    logger.info("  AI calls: %d (tokens: %d)", stats["ai_calls"], stats["ai_tokens"])
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
