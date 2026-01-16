"""Pydantic models for API responses."""

from pydantic import BaseModel, Field


class LocationResponse(BaseModel):
    """Location information."""

    city: str | None = None
    country: str | None = None


class SalaryResponse(BaseModel):
    """Salary information."""

    employment_type: str | None = None
    from_amount: float | None = None
    to_amount: float | None = None
    currency: str | None = None
    period: str | None = None
    gross: bool | None = None


class JobResult(BaseModel):
    """A single job result."""

    score: float = Field(description="Similarity score (0-1)")
    title: str
    company: str | None = None
    locations: list[LocationResponse] = Field(default_factory=list)
    work_mode: str | None = None
    salaries: list[SalaryResponse] = Field(default_factory=list)
    source: str
    source_id: str
    job_url: str | None = None
    skills_required: list[str] = Field(default_factory=list)
    also_on: list[str] = Field(
        default_factory=list,
        description="Other sources where this job appears",
    )


class SearchResponse(BaseModel):
    """Response for search endpoint."""

    query: str
    total: int
    duplicates_collapsed: int = 0
    results: list[JobResult]


class SimilarResponse(BaseModel):
    """Response for similar jobs endpoint."""

    reference_job: JobResult
    total: int
    duplicates_collapsed: int = 0
    results: list[JobResult]


class HealthResponse(BaseModel):
    """Response for health endpoint."""

    status: str
    qdrant_connected: bool
    collection_name: str
    points_count: int | None = None
