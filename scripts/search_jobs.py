#!/usr/bin/env python
"""Search job postings by semantic query with optional filters."""

import argparse
import sys
from dataclasses import dataclass, field
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

from job_posting_radar.config import AppSettings
from job_posting_radar.pipelines.embedding.embeddings import EmbeddingGenerator


@dataclass
class CollapsedResult:
    """A search result with potential duplicates collapsed."""

    score: float
    payload: dict[str, Any]
    sources: list[str] = field(default_factory=list)
    urls: dict[str, str] = field(default_factory=dict)


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
  python scripts/search_jobs.py --q "devops" --no-collapse  # Show all duplicates
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
    parser.add_argument(
        "--no-collapse",
        action="store_true",
        help="Don't collapse duplicate postings (show all)",
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


def collapse_duplicates(results: list[Any]) -> list[CollapsedResult]:
    """Collapse duplicate postings by content_hash, keeping highest score.

    Args:
        results: List of Qdrant ScoredPoint objects.

    Returns:
        List of CollapsedResult with duplicates merged.
    """
    groups: dict[str, CollapsedResult] = {}

    for point in results:
        payload = point.payload
        score = point.score
        content_hash = payload.get("content_hash", point.id)
        source = payload.get("source", "unknown")
        job_url = payload.get("job_url", "")

        if content_hash not in groups:
            # First occurrence - create new group
            groups[content_hash] = CollapsedResult(
                score=score,
                payload=payload,
                sources=[source],
                urls={source: job_url},
            )
        else:
            # Duplicate found - merge into existing group
            existing = groups[content_hash]
            if source not in existing.sources:
                existing.sources.append(source)
                existing.urls[source] = job_url
            # Keep the higher score
            if score > existing.score:
                existing.score = score
                existing.payload = payload

    # Sort by score descending
    collapsed = sorted(groups.values(), key=lambda x: x.score, reverse=True)
    return collapsed


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
    """Display search results in formatted output (no collapsing).

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


def display_collapsed_results(
    results: list[CollapsedResult],
    query: str,
    total_raw: int,
) -> None:
    """Display collapsed search results with duplicate info.

    Args:
        results: List of CollapsedResult objects.
        query: Original search query.
        total_raw: Total number of raw results before collapsing.
    """
    duplicates_collapsed = total_raw - len(results)

    print(f"\n{'='*80}")
    print(f"Search results for: \"{query}\"")
    print(f"Found {len(results)} unique jobs", end="")
    if duplicates_collapsed > 0:
        print(f" ({duplicates_collapsed} duplicates collapsed)")
    else:
        print()
    print(f"{'='*80}\n")

    for i, result in enumerate(results, 1):
        payload = result.payload
        score = result.score

        title = payload.get("title", "Unknown")
        company = payload.get("company", "Unknown")
        locations = format_locations(payload.get("locations", []))
        salary = format_salary(payload.get("salaries", []))
        work_mode = payload.get("work_mode", "unknown")
        primary_source = result.sources[0]
        primary_url = result.urls.get(primary_source, "N/A")

        print(f"{i:2}. [{score:.3f}] {title}")
        print(f"    Company:  {company}")
        print(f"    Location: {locations} ({work_mode})")
        print(f"    Salary:   {salary}")
        print(f"    Source:   {primary_source}")
        print(f"    URL:      {primary_url}")

        # Show additional sources if duplicates exist
        if len(result.sources) > 1:
            other_sources = [s for s in result.sources if s != primary_source]
            other_urls = [result.urls.get(s, "") for s in other_sources]
            also_on = ", ".join(
                f"{src}" + (f" ({url})" if url else "")
                for src, url in zip(other_sources, other_urls)
            )
            print(f"    Also on:  {also_on}")

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

    # When collapsing, fetch more results to account for duplicates
    fetch_limit = args.limit * 3 if not args.no_collapse else args.limit

    # Search
    print(f"Searching for top {args.limit} results...")
    results = client.search(
        collection_name=settings.qdrant_collection_name,
        query_vector=query_vector,
        query_filter=search_filter,
        limit=fetch_limit,
        with_payload=True,
    )

    if not results:
        print("\nNo matching jobs found.")
        return 0

    if args.no_collapse:
        # Show raw results without collapsing
        display_results(results[:args.limit], args.q)
    else:
        # Collapse duplicates and display
        collapsed = collapse_duplicates(results)
        collapsed = collapsed[:args.limit]  # Trim to requested limit
        display_collapsed_results(collapsed, args.q, len(results))

    return 0


if __name__ == "__main__":
    sys.exit(main())
