"""CLI entrypoint for fetching raw job postings."""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from app.config import AppSettings
from app.ingest.fetch import SOURCE_NAME, ingest_nofluff


def parse_args() -> argparse.Namespace:
    """Build and parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Ingest raw job postings.")
    parser.add_argument(
        "--source",
        default=SOURCE_NAME,
        choices=[SOURCE_NAME],
        help="Source to ingest (currently only NoFluff).",
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
    return parser.parse_args()


def resolve_date(raw_date: Optional[str]) -> date:
    """Resolve ISO date string to date."""
    if not raw_date:
        return datetime.now().astimezone(timezone.utc).date()
    return date.fromisoformat(raw_date)


def main() -> None:
    """Run ingestion for the chosen source."""
    args = parse_args()
    settings = AppSettings()
    target_date = resolve_date(args.date)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.source != SOURCE_NAME:
        raise ValueError(f"Unsupported source {args.source}")

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else settings.source_raw_dir(args.source, target_date)
    )

    written = ingest_nofluff(
        pages=args.pages,
        start_page=args.start_page,
        output_dir=output_dir,
        fetch_details=not args.skip_details,
        settings=settings,
    )

    print(f"Wrote {len(written)} files to {output_dir}")


if __name__ == "__main__":
    main()


