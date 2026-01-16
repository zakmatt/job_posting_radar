#!/usr/bin/env python
"""Find similar job postings based on an existing job."""

import argparse
import sys
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

from job_posting_radar.config import AppSettings


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Find similar job postings based on an existing job.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/similar_jobs.py --source nofluff --source-id 3DWAXHWK
  python scripts/similar_jobs.py --source justjoin --source-id senior-python-dev --limit 10
  python scripts/similar_jobs.py --source nofluff --source-id ABC123 --cross-source
        """,
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=["nofluff", "justjoin"],
        help="Source of the reference job (nofluff or justjoin)",
    )
    parser.add_argument(
        "--source-id",
        required=True,
        help="Source ID of the reference job",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of similar jobs to return (default: 20)",
    )
    parser.add_argument(
        "--cross-source",
        action="store_true",
        help="Include jobs from both sources (default: same source only)",
    )
    parser.add_argument(
        "--exclude-same-company",
        action="store_true",
        help="Exclude jobs from the same company",
    )
    return parser.parse_args()


def find_job_by_source_id(
    client: QdrantClient,
    collection_name: str,
    source: str,
    source_id: str,
) -> tuple[Any | None, list[float] | None]:
    """Find a job by its source and source_id.

    Args:
        client: Qdrant client instance.
        collection_name: Name of the collection.
        source: Job source (nofluff/justjoin).
        source_id: Source-specific job ID.

    Returns:
        Tuple of (job payload, job vector) or (None, None) if not found.
    """
    # Search by source and source_id fields
    search_filter = Filter(
        must=[
            FieldCondition(key="source", match=MatchValue(value=source)),
            FieldCondition(key="source_id", match=MatchValue(value=source_id)),
        ]
    )

    # Scroll to find the job (we expect exactly one match)
    results, _ = client.scroll(
        collection_name=collection_name,
        scroll_filter=search_filter,
        limit=1,
        with_payload=True,
        with_vectors=True,
    )

    if not results:
        return None, None

    point = results[0]
    return point.payload, point.vector


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


def display_reference_job(payload: dict[str, Any]) -> None:
    """Display the reference job details.

    Args:
        payload: Job payload dictionary.
    """
    print(f"\n{'='*80}")
    print("REFERENCE JOB")
    print(f"{'='*80}")

    title = payload.get("title", "Unknown")
    company = payload.get("company", "Unknown")
    locations = format_locations(payload.get("locations", []))
    salary = format_salary(payload.get("salaries", []))
    source = payload.get("source", "unknown")
    work_mode = payload.get("work_mode", "unknown")
    job_url = payload.get("job_url", "N/A")

    print(f"Title:    {title}")
    print(f"Company:  {company}")
    print(f"Location: {locations} ({work_mode})")
    print(f"Salary:   {salary}")
    print(f"Source:   {source}")
    print(f"URL:      {job_url}")

    skills = payload.get("skills_required", [])
    if skills:
        print(f"Skills:   {', '.join(skills[:8])}" + ("..." if len(skills) > 8 else ""))


def display_similar_jobs(results: list[Any], reference_source_id: str) -> None:
    """Display similar job results.

    Args:
        results: List of Qdrant ScoredPoint objects.
        reference_source_id: Source ID of the reference job (to exclude from results).
    """
    print(f"\n{'='*80}")
    print("SIMILAR JOBS")
    print(f"{'='*80}\n")

    displayed = 0
    for point in results:
        payload = point.payload
        score = point.score

        # Skip the reference job itself
        if payload.get("source_id") == reference_source_id:
            continue

        displayed += 1
        title = payload.get("title", "Unknown")
        company = payload.get("company", "Unknown")
        locations = format_locations(payload.get("locations", []))
        salary = format_salary(payload.get("salaries", []))
        source = payload.get("source", "unknown")
        work_mode = payload.get("work_mode", "unknown")
        job_url = payload.get("job_url", "N/A")

        print(f"{displayed:2}. [{score:.3f}] {title}")
        print(f"    Company:  {company}")
        print(f"    Location: {locations} ({work_mode})")
        print(f"    Salary:   {salary}")
        print(f"    Source:   {source}")
        print(f"    URL:      {job_url}")
        print()

    if displayed == 0:
        print("No similar jobs found.")


def main() -> int:
    """Run the similar jobs command.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    args = parse_args()
    settings = AppSettings()

    # Initialize client
    print(f"Connecting to Qdrant at {settings.qdrant_host}:{settings.qdrant_port}...")
    client = QdrantClient(host=settings.qdrant_host, port=settings.qdrant_port)

    # Check if collection exists
    try:
        collection_info = client.get_collection(settings.qdrant_collection_name)
        print(f"Collection '{settings.qdrant_collection_name}' has {collection_info.points_count} points")
    except Exception as exc:
        print(f"Error: Collection '{settings.qdrant_collection_name}' not found: {exc}")
        return 1

    # Find the reference job
    print(f"Looking for job: source={args.source}, source_id={args.source_id}...")
    payload, vector = find_job_by_source_id(
        client=client,
        collection_name=settings.qdrant_collection_name,
        source=args.source,
        source_id=args.source_id,
    )

    if payload is None or vector is None:
        print(f"\nError: Job not found with source={args.source}, source_id={args.source_id}")
        print("\nTip: Use the Qdrant dashboard or search to find valid source_ids")
        return 1

    display_reference_job(payload)

    # Build filter for similar jobs search
    conditions = []

    # Filter by source unless cross-source is enabled
    if not args.cross_source:
        conditions.append(
            FieldCondition(key="source", match=MatchValue(value=args.source))
        )

    # Exclude same company if requested
    if args.exclude_same_company and payload.get("company"):
        # Note: Qdrant doesn't have a "not equal" filter directly,
        # so we'd need to use must_not
        pass  # Will handle in filter construction

    search_filter = None
    if conditions:
        search_filter = Filter(must=conditions)

    if args.exclude_same_company and payload.get("company"):
        # Add must_not condition for same company
        must_not = [
            FieldCondition(key="company", match=MatchValue(value=payload["company"]))
        ]
        if search_filter:
            search_filter.must_not = must_not
        else:
            search_filter = Filter(must_not=must_not)

    # Search for similar jobs
    # Request extra results to account for excluding the reference job
    print(f"\nSearching for similar jobs (limit={args.limit})...")
    results = client.search(
        collection_name=settings.qdrant_collection_name,
        query_vector=vector,
        query_filter=search_filter,
        limit=args.limit + 1,  # +1 to account for reference job
        with_payload=True,
    )

    display_similar_jobs(results, args.source_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
