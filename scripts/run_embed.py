"""CLI entrypoint for embedding and upserting job postings to Qdrant."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Optional

from app.config import AppSettings
from app.vector.embeddings import EmbeddingGenerator
from app.vector.store import VectorStore

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Build and parse CLI arguments.

    Returns:
        argparse.Namespace with embedding options.
    """
    parser = argparse.ArgumentParser(description="Embed and upsert normalized job postings.")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date (YYYY-MM-DD) for normalized data. Defaults to today (UTC).",
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=None,
        help="Override input directory. Defaults to data/normalized/<date>.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of postings to process in a single batch.",
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


def main() -> None:
    """Run embedding and upserting process."""
    args = parse_args()
    settings = AppSettings()
    target_date = resolve_date(args.date)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    input_dir = (
        Path(args.input_dir)
        if args.input_dir
        else settings.data_dir / "normalized" / target_date.isoformat()
    )

    if not input_dir.exists():
        logger.error("Input directory does not exist: %s", input_dir)
        return

    # Initialize components
    generator = EmbeddingGenerator(settings=settings)
    store = VectorStore(settings=settings)

    files = sorted(input_dir.glob("*.json"))
    total_files = len(files)
    logger.info("Found normalized files to process", extra={"count": total_files, "dir": str(input_dir)})

    for i in range(0, total_files, args.batch_size):
        batch_files = files[i : i + args.batch_size]
        batch_ids: List[str] = []
        batch_texts: List[str] = []
        batch_payloads: List[dict] = []

        for f in batch_files:
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                # Use point_id for Qdrant (must be UUID or int)
                point_id = data.get("point_id")
                embedding_text = data.get("embedding_text")

                if not point_id or not embedding_text:
                    logger.warning("File missing required fields, skipping", extra={"path": str(f)})
                    continue

                batch_ids.append(point_id)
                batch_texts.append(embedding_text)
                batch_payloads.append(data)
            except Exception as exc:
                logger.warning("Failed to process file", extra={"path": str(f), "error": str(exc)})

        if not batch_texts:
            continue

        # Generate embeddings
        vectors = generator.generate(batch_texts)

        # Upsert to Qdrant
        store.upsert_postings(ids=batch_ids, vectors=vectors, payloads=batch_payloads)
        logger.info(
            "Processed batch",
            extra={
                "batch_start": i,
                "batch_end": min(i + args.batch_size, total_files),
                "total": total_files,
            },
        )

    print(f"Successfully upserted {total_files} postings to Qdrant.")


if __name__ == "__main__":
    main()

