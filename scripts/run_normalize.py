"""CLI entrypoint for normalizing raw job postings."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from app.config import AppSettings
from app.ingest.fetch import SOURCE_JUSTJOIN, SOURCE_NOFLUFF
from app.normalize import normalize_justjoin, normalize_nofluff

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Build and parse CLI arguments.

    Returns:
        argparse.Namespace with normalization options.
    """
    parser = argparse.ArgumentParser(description="Normalize raw job postings.")
    parser.add_argument(
        "--source",
        default=None,
        choices=[SOURCE_NOFLUFF, SOURCE_JUSTJOIN, "all"],
        help="Source to normalize (nofluff, justjoin, or all). Defaults to all.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date (YYYY-MM-DD). Defaults to today (UTC).",
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=None,
        help="Override input directory. Defaults to data/raw/<source>/<date>.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Override output directory. Defaults to data/normalized/<date>.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite existing normalized files.",
    )
    return parser.parse_args()


def resolve_date(raw_date: Optional[str]) -> date:
    """Resolve ISO date string to date.

    Args:
        raw_date: ISO date string (YYYY-MM-DD) or None.

    Returns:
        date object; today (UTC) if None provided.
    """
    if not raw_date:
        return datetime.now().astimezone(timezone.utc).date()
    return date.fromisoformat(raw_date)


def normalize_file(
    input_path: Path,
    output_dir: Path,
    overwrite: bool = False,
) -> Optional[Path]:
    """Normalize a single raw JSON file and persist the result.

    Args:
        input_path: Path to raw JSON file.
        output_dir: Directory for normalized output.
        overwrite: If True, overwrite existing files.

    Returns:
        Path to written file, or None if skipped/failed.
    """
    try:
        raw = json.loads(input_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to read %s: %s", input_path, exc)
        return None

    source = raw.get("source")
    if source == SOURCE_NOFLUFF:
        normalized = normalize_nofluff(raw)
    elif source == SOURCE_JUSTJOIN:
        normalized = normalize_justjoin(raw)
    else:
        logger.warning("Unknown source %s in %s", source, input_path)
        return None

    # Use content_hash as filename for deduplication
    output_path = output_dir / f"{normalized.content_hash}.json"

    if output_path.exists() and not overwrite:
        logger.debug("Skipping existing %s", output_path)
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        normalized.model_dump_json(indent=2),
        encoding="utf-8",
    )
    logger.info("Wrote normalized posting", extra={"path": str(output_path)})
    return output_path


def normalize_directory(
    input_dir: Path,
    output_dir: Path,
    overwrite: bool = False,
) -> int:
    """Normalize all JSON files in a directory.

    Args:
        input_dir: Directory containing raw JSON files.
        output_dir: Directory for normalized output.
        overwrite: If True, overwrite existing files.

    Returns:
        Count of files written.
    """
    if not input_dir.exists():
        logger.warning("Input directory does not exist: %s", input_dir)
        return 0

    written = 0
    for json_file in sorted(input_dir.glob("*.json")):
        result = normalize_file(json_file, output_dir, overwrite=overwrite)
        if result:
            written += 1

    return written


def main() -> None:
    """Run normalization for the chosen source(s)."""
    args = parse_args()
    settings = AppSettings()
    target_date = resolve_date(args.date)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = settings.data_dir / "normalized" / target_date.isoformat()

    # Determine sources to process
    sources = []
    if args.source is None or args.source == "all":
        sources = [SOURCE_NOFLUFF, SOURCE_JUSTJOIN]
    else:
        sources = [args.source]

    total_written = 0

    for source in sources:
        if args.input_dir:
            input_dir = Path(args.input_dir)
        else:
            input_dir = settings.source_raw_dir(source, target_date)

        if not input_dir.exists():
            logger.info("No data for source %s at %s", source, input_dir)
            continue

        logger.info("Normalizing %s from %s", source, input_dir)
        written = normalize_directory(input_dir, output_dir, overwrite=args.overwrite)
        total_written += written
        logger.info("Wrote %d files for %s", written, source)

    print(f"Normalized {total_written} files to {output_dir}")


if __name__ == "__main__":
    main()

