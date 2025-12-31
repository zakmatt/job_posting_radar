"""Orchestration helpers for ingesting raw job postings."""

from __future__ import annotations

import hashlib
import json
import logging
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
    settings: Optional[AppSettings] = None,
) -> List[Path]:
    """Ingest pages from No Fluff Jobs and persist raw payloads.

    Args:
        pages: Number of pages to fetch.
        output_dir: Directory where raw JSON files will be written.
        start_page: First page to fetch (1-based).
        criteria: Optional search criteria dictionary.
        fetch_details: When True, fetches detail payload per job.
        settings: Optional application settings instance.

    Returns:
        List of paths written.
    """
    settings = settings or AppSettings()
    output_dir.mkdir(parents=True, exist_ok=True)
    client = NoFluffJobsClient(settings=settings)
    written: List[Path] = []

    for page in range(start_page, start_page + pages):
        logger.info("Fetching page", extra={"page": page})
        search_response = client.fetch_page(page=page, criteria=criteria)
        postings = list(_extract_postings(search_response))
        if not postings:
            logger.info("No postings returned; stopping pagination", extra={"page": page})
            break

        for posting in postings:
            source_id = _extract_source_id(posting)
            job_slug = _extract_job_slug(posting)
            payload: Dict[str, Any] = {"listing": posting}

            if fetch_details:
                try:
                    payload["details"] = client.fetch_job_details(job_slug)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Failed to fetch job details",
                        extra={"source_id": source_id, "job_slug": job_slug, "error": str(exc)},
                    )

            record = {
                "source": SOURCE_NAME,
                "source_id": source_id,
                "job_slug": job_slug,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "page": page,
                "url": posting.get("url") or posting.get("postingUrl"),
                "payload": payload,
            }
            path = _persist_payload(record=record, output_dir=output_dir)
            if path:
                written.append(path)

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


def _persist_payload(record: Dict[str, Any], output_dir: Path) -> Optional[Path]:
    """Persist a payload to disk, skipping if it already exists."""
    source_id = record.get("source_id")
    if not source_id:
        raise ValueError("record missing source_id")

    path = output_dir / f"{source_id}.json"
    if path.exists():
        logger.info("Skipping existing payload", extra={"path": str(path)})
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Wrote raw payload", extra={"path": str(path)})
    return path


