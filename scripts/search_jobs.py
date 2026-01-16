#!/usr/bin/env python
"""Search job postings by semantic query with optional filters."""

import argparse
import sys
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

from job_posting_radar.config import AppSettings
from job_posting_radar.pipelines.embedding.embeddings import EmbeddingGenerator


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Search job postings by semantic query.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/search_jobs.py --q "senior ml engineer"
  python scripts/search_jobs.py --q "python backend" --limit 10 --mode remote
  python scripts/search_jobs.py --q "data scientist" --city Warszawa --mode hybrid
        """,
    )
    parser.add_argument(
        "--q",
        required=True,
        help="Search query (e.g., 'senior ml engineer')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of results (default: 20)",
    )
    parser.add_argument(
        "--mode",
        choices=["remote", "hybrid", "onsite"],
        help="Filter by work mode",
    )
    parser.add_argument(
        "--city",
        help="Filter by city name",
    )
    parser.add_argument(
        "--source",
        choices=["nofluff", "justjoin"],
        help="Filter by source",
    )
    return parser.parse_args()


def build_filter(
    mode: str | None,
    city: str | None,
    source: str | None,
) -> Filter | None:
    """Build Qdrant filter from CLI arguments.

    Args:
        mode: Work mode filter (remote/hybrid/onsite).
        city: City name filter.
        source: Source filter (nofluff/justjoin).

    Returns:
        Qdrant Filter object or None if no filters specified.
    """
    conditions = []

    if mode:
        conditions.append(
            FieldCondition(key="work_mode", match=MatchValue(value=mode))
        )

    if city:
        # Filter by city in the locations array
        conditions.append(
            FieldCondition(key="locations[].city", match=MatchValue(value=city))
        )

    if source:
        conditions.append(
            FieldCondition(key="source", match=MatchValue(value=source))
        )

    if not conditions:
        return None

    return Filter(must=conditions)


def format_salary(salaries: list[dict[str, Any]]) -> str:
    """Format salary information for display.

    Args:
        salaries: List of salary dictionaries.

    Returns:
        Formatted salary string.
    """
    if not salaries:
        return "Not disclosed"

    parts = []
    for sal in salaries:
        from_amt = sal.get("from_amount")
        to_amt = sal.get("to_amount")
        currency = sal.get("currency", "PLN")
        period = sal.get("period", "month")
        emp_type = sal.get("employment_type", "")

        if from_amt and to_amt:
            salary_str = f"{from_amt:,.0f}-{to_amt:,.0f} {currency}/{period}"
        elif from_amt:
            salary_str = f"{from_amt:,.0f}+ {currency}/{period}"
        elif to_amt:
            salary_str = f"up to {to_amt:,.0f} {currency}/{period}"
        else:
            continue

        if emp_type:
            salary_str = f"{salary_str} ({emp_type})"
        parts.append(salary_str)

    return " | ".join(parts) if parts else "Not disclosed"


def format_locations(locations: list[dict[str, Any]]) -> str:
    """Format location information for display.

    Args:
        locations: List of location dictionaries.

    Returns:
        Formatted location string.
    """
    if not locations:
        return "Unknown"

    cities = []
    for loc in locations:
        city = loc.get("city")
        if city and city not in cities:
            cities.append(city)

    return ", ".join(cities[:3]) + ("..." if len(cities) > 3 else "") if cities else "Unknown"


def display_results(results: list[Any], query: str) -> None:
    """Display search results in formatted output.

    Args:
        results: List of Qdrant ScoredPoint objects.
        query: Original search query.
    """
    print(f"\n{'='*80}")
    print(f"Search results for: \"{query}\"")
    print(f"Found {len(results)} matching jobs")
    print(f"{'='*80}\n")

    for i, point in enumerate(results, 1):
        payload = point.payload
        score = point.score

        title = payload.get("title", "Unknown")
        company = payload.get("company", "Unknown")
        locations = format_locations(payload.get("locations", []))
        salary = format_salary(payload.get("salaries", []))
        source = payload.get("source", "unknown")
        work_mode = payload.get("work_mode", "unknown")
        job_url = payload.get("job_url", "N/A")

        print(f"{i:2}. [{score:.3f}] {title}")
        print(f"    Company:  {company}")
        print(f"    Location: {locations} ({work_mode})")
        print(f"    Salary:   {salary}")
        print(f"    Source:   {source}")
        print(f"    URL:      {job_url}")
        print()


def main() -> int:
    """Run the search command.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    args = parse_args()
    settings = AppSettings()

    # Initialize clients
    print(f"Connecting to Qdrant at {settings.qdrant_host}:{settings.qdrant_port}...")
    client = QdrantClient(host=settings.qdrant_host, port=settings.qdrant_port)

    # Check if collection exists
    try:
        collection_info = client.get_collection(settings.qdrant_collection_name)
        print(f"Collection '{settings.qdrant_collection_name}' has {collection_info.points_count} points")
    except Exception as exc:
        print(f"Error: Collection '{settings.qdrant_collection_name}' not found: {exc}")
        return 1

    # Generate embedding for query
    print(f"Generating embedding for query: \"{args.q}\"...")
    generator = EmbeddingGenerator(settings=settings)
    query_vector = generator.generate([args.q])[0]

    # Build filter
    search_filter = build_filter(mode=args.mode, city=args.city, source=args.source)
    if search_filter:
        filter_desc = []
        if args.mode:
            filter_desc.append(f"mode={args.mode}")
        if args.city:
            filter_desc.append(f"city={args.city}")
        if args.source:
            filter_desc.append(f"source={args.source}")
        print(f"Applying filters: {', '.join(filter_desc)}")

    # Search
    print(f"Searching for top {args.limit} results...")
    results = client.search(
        collection_name=settings.qdrant_collection_name,
        query_vector=query_vector,
        query_filter=search_filter,
        limit=args.limit,
        with_payload=True,
    )

    if not results:
        print("\nNo matching jobs found.")
        return 0

    display_results(results, args.q)
    return 0


if __name__ == "__main__":
    sys.exit(main())
