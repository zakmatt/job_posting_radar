"""Nodes for the ingestion pipeline."""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from job_posting_radar.clients.nofluff import NoFluffJobsClient
from job_posting_radar.clients.justjoin import JustJoinClient
from job_posting_radar.config import AppSettings
from job_posting_radar.metrics import JOBS_INGESTED_TOTAL, INGESTION_ERRORS_TOTAL, push_metrics
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

SOURCE_NOFLUFF = "nofluff"
SOURCE_JUSTJOIN = "justjoin"


def _posted_at(posting: Dict[str, Any]) -> Optional[datetime]:
    """Return posting datetime (UTC) if available."""
    ts = posting.get("posted") or posting.get("renewed") or posting.get("publishedAt")
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        except Exception:  # noqa: BLE001
            return None
    if isinstance(ts, str):
        try:
            # Normalize Z to +00:00
            if ts.endswith("Z"):
                ts = ts.replace("Z", "+00:00")
            return datetime.fromisoformat(ts).astimezone(timezone.utc)
        except Exception:  # noqa: BLE001
            return None
    return None


def ingest_nofluff_node(
    params: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Ingest postings from NoFluffJobs.

    Args:
        params: Ingestion parameters (pages, start_page, limit, since_days).

    Returns:
        Dictionary mapping filename to raw posting content.
    """
    settings = AppSettings()
    client = NoFluffJobsClient(settings=settings)
    
    pages = params.get("pages", 1)
    start_page = params.get("start_page", 1)
    target_count = params.get("limit")
    since_days = params.get("since_days")
    since_cutoff = None
    if since_days:
        since_cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)

    results = {}
    seen_source_ids = set()
    total_written = 0
    stop_pagination = False

    try:
        for page in range(start_page, start_page + pages):
            if stop_pagination:
                break
            logger.info("Fetching NoFluff Jobs page", extra={"page": page})
            search_response = client.fetch_page(page=page)
            postings = search_response.get("postings") or []
            if not postings:
                break

            page_records = []
            for posting in postings:
                posted_at = _posted_at(posting)
                if since_cutoff and posted_at and posted_at < since_cutoff:
                    stop_pagination = True
                    continue
                
                source_id = posting.get("reference") or posting.get("id")
                if not source_id or source_id in seen_source_ids:
                    continue
                seen_source_ids.add(source_id)

                job_slug = posting.get("url")
                record = {
                    "source": SOURCE_NOFLUFF,
                    "source_id": source_id,
                    "job_slug": job_slug,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "payload": {"listing": posting},
                }
                page_records.append(record)

            # Parallel detail fetch
            if page_records:
                with ThreadPoolExecutor(max_workers=8) as executor:
                    future_map = {
                        executor.submit(client.fetch_job_details, rec["job_slug"]): rec
                        for rec in page_records if rec["job_slug"]
                    }
                    for future, rec in future_map.items():
                        try:
                            rec["payload"]["details"] = future.result()
                        except Exception as exc:
                            logger.warning("Failed detail fetch", extra={"error": str(exc)})

            for rec in page_records:
                results[rec["source_id"]] = rec
                total_written += 1
                if target_count and total_written >= target_count:
                    stop_pagination = True
                    break

        JOBS_INGESTED_TOTAL.labels(source=SOURCE_NOFLUFF).inc(len(results))
        push_metrics("ingest", settings=settings)
        return results
    except Exception as exc:
        INGESTION_ERRORS_TOTAL.labels(source=SOURCE_NOFLUFF).inc()
        push_metrics("ingest", settings=settings)
        raise exc


def ingest_justjoin_node(
    params: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Ingest postings from JustJoin.it.

    Args:
        params: Ingestion parameters (pages, start_page, limit, since_days).

    Returns:
        Dictionary mapping filename to raw posting content.
    """
    settings = AppSettings()
    client = JustJoinClient(settings=settings)
    
    pages = params.get("pages", 1)
    start_page = params.get("start_page", 1)
    target_count = params.get("limit")
    since_days = params.get("since_days")
    since_cutoff = None
    if since_days:
        since_cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)

    results = {}
    seen_source_ids = set()
    total_written = 0
    stop_pagination = False

    cursor = (start_page - 1) * settings.justjoin_page_size
    
    try:
        for batch in range(pages):
            if stop_pagination:
                break
            logger.info("Fetching JustJoin page", extra={"batch": batch + 1, "cursor": cursor})
            search_response = client.fetch_page(cursor=cursor)
            postings = search_response.get("postings") or []
            if not postings:
                break

            page_records = []
            for posting in postings:
                posted_at = _posted_at(posting)
                if since_cutoff and posted_at and posted_at < since_cutoff:
                    stop_pagination = True
                    continue
                
                source_id = posting.get("slug") or posting.get("guid")
                if not source_id or source_id in seen_source_ids:
                    continue
                seen_source_ids.add(source_id)

                record = {
                    "source": SOURCE_JUSTJOIN,
                    "source_id": source_id,
                    "job_slug": source_id,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "payload": {"listing": posting},
                }
                page_records.append(record)

            # Parallel detail fetch
            if page_records:
                with ThreadPoolExecutor(max_workers=8) as executor:
                    future_map = {
                        executor.submit(client.fetch_detail, rec["job_slug"]): rec
                        for rec in page_records
                    }
                    for future, rec in future_map.items():
                        try:
                            rec["payload"]["details"] = future.result()
                        except Exception as exc:
                            logger.warning("Failed detail fetch", extra={"error": str(exc)})

            for rec in page_records:
                results[rec["source_id"]] = rec
                total_written += 1
                if target_count and total_written >= target_count:
                    stop_pagination = True
                    break

            if search_response.get("next_cursor") is None:
                cursor += search_response.get("items_count", 100)
            else:
                cursor = search_response.get("next_cursor")

        JOBS_INGESTED_TOTAL.labels(source=SOURCE_JUSTJOIN).inc(len(results))
        push_metrics("ingest", settings=settings)
        return results
    except Exception as exc:
        INGESTION_ERRORS_TOTAL.labels(source=SOURCE_JUSTJOIN).inc()
        push_metrics("ingest", settings=settings)
        raise exc

