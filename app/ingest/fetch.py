"""Orchestration helpers for ingesting raw job postings."""

from __future__ import annotations

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from app.config import AppSettings
from app.ingest.sources.nofluff import NoFluffJobsClient

logger = logging.getLogger(__name__)

SOURCE_NAME = "nofluff"


def ingest_nofluff(
    *,
    pages: int,
    output_dir: Path,
    start_page: int = 1,
    criteria: Optional[Dict[str, Any]] = None,
    fetch_details: bool = True,
    detail_workers: int = 8,
    target_count: Optional[int] = None,
    since: Optional[datetime] = None,
    settings: Optional[AppSettings] = None,
) -> List[Path]:
    """Ingest pages from No Fluff Jobs and persist raw payloads.

    Args:
        pages: Number of pages to fetch.
        output_dir: Directory where raw JSON files will be written.
        start_page: First page to fetch (1-based).
        criteria: Optional search criteria dictionary.
        fetch_details: When True, fetches detail payload per job.
        detail_workers: Maximum parallel workers for detail fetch.
        target_count: Optional hard cap on number of offers to write.
        since: Optional UTC datetime; stop when postings are older than this.
        settings: Optional application settings instance.

    Returns:
        List of paths written.
    """
    settings = settings or AppSettings()
    output_dir.mkdir(parents=True, exist_ok=True)
    client = NoFluffJobsClient(settings=settings)
    written: List[Path] = []
    seen_source_ids: set[str] = set()
    stop_pagination = False
    total_written = 0

    for page in range(start_page, start_page + pages):
        if stop_pagination:
            break
        logger.info("Fetching page", extra={"page": page})
        search_response = client.fetch_page(page=page, criteria=criteria)
        postings = list(_extract_postings(search_response))
        if not postings:
            logger.info("No postings returned; stopping pagination", extra={"page": page})
            break

        page_records: List[Dict[str, Any]] = []
        for posting in postings:
            posted_at = _posted_at(posting)
            if since and posted_at and posted_at < since:
                stop_pagination = True
                continue
            source_id = _extract_source_id(posting)
            if source_id in seen_source_ids:
                continue
            seen_source_ids.add(source_id)

            job_slug = _extract_job_slug(posting)
            record: Dict[str, Any] = {
                "source": SOURCE_NAME,
                "source_id": source_id,
                "job_slug": job_slug,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "page": page,
                "url": posting.get("url") or posting.get("postingUrl"),
                "payload": {"listing": posting},
            }
            path = _target_path(record=record, output_dir=output_dir)
            if path.exists():
                logger.info("Skipping existing payload", extra={"path": str(path)})
                continue
            page_records.append({"record": record, "path": path})

        if not page_records:
            continue

        if fetch_details:
            with ThreadPoolExecutor(max_workers=detail_workers) as executor:
                future_map = {
                    executor.submit(_fetch_details_worker, rec["record"]["job_slug"], settings): rec
                    for rec in page_records
                }
                for future, rec in future_map.items():
                    try:
                        details = future.result()
                        rec["record"]["payload"]["details"] = details
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "Failed to fetch job details",
                            extra={
                                "source_id": rec["record"]["source_id"],
                                "job_slug": rec["record"]["job_slug"],
                                "error": str(exc),
                            },
                        )

        for rec in page_records:
            path = _persist_payload(
                record=rec["record"],
                output_dir=output_dir,
                precomputed_path=rec["path"],
            )
            if path:
                written.append(path)
                total_written += 1

        if target_count and total_written >= target_count:
            stop_pagination = True

    return written


def _extract_postings(response: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """Return iterable of postings from a search response."""
    for key in ("postings", "content", "data"):
        postings = response.get(key)
        if isinstance(postings, list):
            return postings
    raise ValueError("Unexpected No Fluff search response shape; no postings list found")


def _extract_source_id(posting: Dict[str, Any]) -> str:
    """Extract a stable source id for file naming."""
    # Prefer vendor-provided reference if present to dedupe multi-location variants.
    for key in ("reference", "url", "id", "postingId", "uuid", "slug"):
        value = posting.get(key)
        if value:
            return str(value)
    digest = hashlib.sha256(json.dumps(posting, sort_keys=True).encode("utf-8")).hexdigest()
    return digest[:20]


def _extract_job_slug(posting: Dict[str, Any]) -> str:
    """Extract slug for detail fetch; fall back to id/reference if missing."""
    for key in ("reference", "url", "slug", "postingUrl", "id"):
        value = posting.get(key)
        if value:
            return str(value)
    digest = hashlib.sha256(json.dumps(posting, sort_keys=True).encode("utf-8")).hexdigest()
    return digest[:20]


def _posted_at(posting: Dict[str, Any]) -> Optional[datetime]:
    """Return posting datetime (UTC) if available."""
    ts = posting.get("posted") or posting.get("renewed")
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        except Exception:  # noqa: BLE001
            return None
    return None


def _target_path(record: Dict[str, Any], output_dir: Path) -> Path:
    """Compute the target path for a record."""
    source_id = record.get("source_id")
    if not source_id:
        raise ValueError("record missing source_id")
    return output_dir / f"{source_id}.json"


def _persist_payload(
    record: Dict[str, Any], output_dir: Path, precomputed_path: Optional[Path] = None
) -> Optional[Path]:
    """Persist a payload to disk, skipping if it already exists."""
    source_id = record.get("source_id")
    if not source_id:
        raise ValueError("record missing source_id")

    path = precomputed_path or _target_path(record=record, output_dir=output_dir)
    if path.exists():
        logger.info("Skipping existing payload", extra={"path": str(path)})
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Wrote raw payload", extra={"path": str(path)})
    return path


def _fetch_details_worker(job_slug: str, settings: AppSettings) -> Dict[str, Any]:
    """Fetch details using a dedicated client per worker to avoid session sharing."""
    client = NoFluffJobsClient(settings=settings)
    return client.fetch_job_details(job_slug)


