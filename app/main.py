"""FastAPI application for job posting search."""

from contextlib import asynccontextmanager
from enum import Enum
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query

from job_posting_radar.config import AppSettings

from app.models import HealthResponse, SearchResponse, SimilarResponse
from app.services import JobSearchService


class WorkMode(str, Enum):
    """Work mode options."""

    remote = "remote"
    hybrid = "hybrid"
    onsite = "onsite"


class Source(str, Enum):
    """Job source options."""

    nofluff = "nofluff"
    justjoin = "justjoin"


# Global service instance
_service: JobSearchService | None = None


def get_service() -> JobSearchService:
    """Get the search service instance.

    Returns:
        JobSearchService instance.
    """
    global _service
    if _service is None:
        _service = JobSearchService(settings=AppSettings())
    return _service


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler.

    Args:
        app: FastAPI application instance.
    """
    # Startup: initialize service
    get_service()
    yield
    # Shutdown: cleanup if needed


app = FastAPI(
    title="Job Posting Radar API",
    description="Semantic search for tech job postings from Polish markets",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check() -> HealthResponse:
    """Check API health and Qdrant connection."""
    service = get_service()
    result = service.health_check()
    return HealthResponse(**result)


@app.get("/search", response_model=SearchResponse, tags=["search"])
async def search_jobs(
    q: Annotated[str, Query(min_length=1, description="Search query")],
    limit: Annotated[int, Query(ge=1, le=100, description="Max results")] = 20,
    mode: Annotated[WorkMode | None, Query(description="Work mode filter")] = None,
    city: Annotated[str | None, Query(description="City filter")] = None,
    source: Annotated[Source | None, Query(description="Source filter")] = None,
    collapse: Annotated[bool, Query(description="Collapse duplicates")] = True,
) -> SearchResponse:
    """Search for jobs by semantic query.

    - **q**: Natural language search query (e.g., "senior python developer")
    - **limit**: Maximum number of results (1-100, default: 20)
    - **mode**: Filter by work mode (remote, hybrid, onsite)
    - **city**: Filter by city name
    - **source**: Filter by source (nofluff, justjoin)
    - **collapse**: Collapse duplicate postings (default: true)
    """
    service = get_service()

    try:
        result = service.search(
            query=q,
            limit=limit,
            mode=mode.value if mode else None,
            city=city,
            source=source.value if source else None,
            collapse=collapse,
        )
        return SearchResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/similar", response_model=SimilarResponse, tags=["search"])
async def find_similar_jobs(
    source: Annotated[Source, Query(description="Source of reference job")],
    source_id: Annotated[str, Query(min_length=1, description="Source-specific job ID")],
    limit: Annotated[int, Query(ge=1, le=100, description="Max results")] = 20,
    cross_source: Annotated[
        bool, Query(description="Include jobs from both sources")
    ] = False,
    exclude_same_company: Annotated[
        bool, Query(description="Exclude jobs from same company")
    ] = False,
    collapse: Annotated[bool, Query(description="Collapse duplicates")] = True,
) -> SimilarResponse:
    """Find jobs similar to a reference job.

    - **source**: Source of the reference job (nofluff or justjoin)
    - **source_id**: The job's source-specific identifier
    - **limit**: Maximum number of results (1-100, default: 20)
    - **cross_source**: Include jobs from both sources (default: false)
    - **exclude_same_company**: Exclude jobs from the same company (default: false)
    - **collapse**: Collapse duplicate postings (default: true)
    """
    service = get_service()

    try:
        result = service.find_similar(
            source=source.value,
            source_id=source_id,
            limit=limit,
            cross_source=cross_source,
            exclude_same_company=exclude_same_company,
            collapse=collapse,
        )
        return SimilarResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
