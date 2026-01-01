"""CLI entrypoint for fetching raw job postings."""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from app.config import AppSettings
from app.ingest.fetch import (
    SOURCE_JUSTJOIN,
    SOURCE_NOFLUFF,
    ingest_justjoin,
    ingest_nofluff,
)


def parse_args() -> argparse.Namespace:
    """Build and parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Ingest raw job postings.")
    parser.add_argument(
        "--source",
        default=SOURCE_NOFLUFF,
        choices=[SOURCE_NOFLUFF, SOURCE_JUSTJOIN],
        help="Source to ingest (NoFluff or JustJoin).",
    )
    parser.add_argument("--pages", type=int, default=1, help="Number of pages to fetch.")
    parser.add_argument(
        "--start-page", type=int, default=1, help="1-based page to start from."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date (YYYY-MM-DD). Defaults to today (UTC).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Override output directory. Defaults to data/raw/<source>/<date>.",
    )
    parser.add_argument(
        "--skip-details",
        action="store_true",
        default=False,
        help="Skip fetching detail payload per job (default: fetch).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after writing this many offers.",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Only include offers posted on/after this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--since-window",
        type=str,
        choices=["last_week", "last_month", "last_quarter", "last_half_year", "last_year"],
        default=None,
        help="Relative time window; mutually exclusive with --since.",
    )
    return parser.parse_args()


def resolve_date(raw_date: Optional[str]) -> date:
    """Resolve ISO date string to date."""
    if not raw_date:
        return datetime.now().astimezone(timezone.utc).date()
    return date.fromisoformat(raw_date)


def resolve_since(raw_since: Optional[str], window: Optional[str]) -> Optional[datetime]:
    """Resolve absolute or relative 'since' cutoff to UTC datetime."""
    if raw_since and window:
        raise ValueError("Use only one of --since or --since-window")
    if raw_since:
        return datetime.fromisoformat(raw_since).astimezone(timezone.utc)
    if window:
        now = datetime.now(timezone.utc)
        if window == "last_week":
            return now - timedelta(days=7)
        if window == "last_month":
            return now - timedelta(days=30)
        if window == "last_quarter":
            return now - timedelta(days=90)
        if window == "last_half_year":
            return now - timedelta(days=182)
        if window == "last_year":
            return now - timedelta(days=365)
    return None


def main() -> None:
    """Run ingestion for the chosen source."""
    args = parse_args()
    settings = AppSettings()
    target_date = resolve_date(args.date)
    since_cutoff = resolve_since(args.since, args.since_window)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.source not in (SOURCE_NOFLUFF, SOURCE_JUSTJOIN):
        raise ValueError(f"Unsupported source {args.source}")

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else settings.source_raw_dir(args.source, target_date)
    )

    if args.source == SOURCE_NOFLUFF:
        written = ingest_nofluff(
            pages=args.pages,
            start_page=args.start_page,
            output_dir=output_dir,
            fetch_details=not args.skip_details,
            target_count=args.limit,
            since=since_cutoff,
            settings=settings,
        )
    else:
        written = ingest_justjoin(
            pages=args.pages,
            start_page=args.start_page,
            output_dir=output_dir,
            fetch_details=False,
            target_count=args.limit,
            since=since_cutoff,
            settings=settings,
        )

    print(f"Wrote {len(written)} files to {output_dir}")


if __name__ == "__main__":
    main()


