"""Pydantic models for normalized job postings."""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import Enum
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, computed_field


class WorkMode(str, Enum):
    """Work arrangement mode."""

    REMOTE = "remote"
    HYBRID = "hybrid"
    ONSITE = "onsite"
    UNKNOWN = "unknown"


class Seniority(str, Enum):
    """Canonical seniority levels."""

    JUNIOR = "junior"
    MID = "mid"
    SENIOR = "senior"
    STAFF = "staff"
    UNKNOWN = "unknown"


class EmploymentType(str, Enum):
    """Contract / employment type."""

    B2B = "b2b"
    PERMANENT = "permanent"
    CONTRACT = "contract"
    MANDATE = "mandate"
    INTERNSHIP = "internship"
    OTHER = "other"


class Location(BaseModel):
    """Geographical location of a job posting."""

    city: Optional[str] = None
    country: Optional[str] = None


class Salary(BaseModel):
    """Salary range for a specific employment type."""

    employment_type: EmploymentType
    from_amount: Optional[float] = Field(default=None, description="Lower bound of salary range.")
    to_amount: Optional[float] = Field(default=None, description="Upper bound of salary range.")
    currency: Optional[str] = Field(default=None, description="ISO 4217 currency code (e.g., PLN, EUR).")
    period: str = Field(default="month", description="Pay period (month, year, hour).")
    gross: Optional[bool] = Field(default=None, description="True if amounts are gross, False if net.")


class NormalizedJobPosting(BaseModel):
    """Normalized job posting ready for embedding and deduplication.

    This model unifies data from multiple sources (NoFluff, JustJoin, etc.)
    into a consistent schema for downstream processing.
    """

    # Identity
    source: Literal["nofluff", "justjoin"]
    source_id: str = Field(..., description="Stable ID per source (guid/reference/id).")
    slug: Optional[str] = Field(default=None, description="Human-readable URL slug.")
    job_url: Optional[str] = Field(default=None, description="Full URL to the offer.")
    ingested_at: datetime = Field(..., description="UTC datetime when the posting was ingested.")

    # Core info
    title: str
    company: Optional[str] = None
    locations: List[Location] = Field(default_factory=list)
    work_mode: WorkMode = WorkMode.UNKNOWN
    seniority: Seniority = Seniority.UNKNOWN

    # Employment & salary
    employment_types: List[EmploymentType] = Field(default_factory=list)
    salaries: List[Salary] = Field(default_factory=list)

    # Skills
    skills_required: List[str] = Field(default_factory=list)
    skills_nice_to_have: List[str] = Field(default_factory=list)

    # Text content (cleaned)
    description: Optional[str] = Field(default=None, description="Company/role description text.")
    requirements_text: Optional[str] = Field(default=None, description="Requirements section text.")
    offer_text: Optional[str] = Field(default=None, description="What the company offers text.")

    # Timestamps (all UTC)
    posted_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    renewed_at: Optional[datetime] = None

    # Dedup helpers
    duplicate_group_id: Optional[str] = Field(default=None, description="Filled during deduplication.")

    @computed_field  # type: ignore[misc]
    @property
    def content_hash(self) -> str:
        """SHA-256 hash of title + company + description for deduplication.

        Returns:
            First 16 hex characters of the hash.
        """
        parts = [
            self.title or "",
            self.company or "",
            self.description or "",
        ]
        combined = "|".join(parts).lower().strip()
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()[:16]

    @computed_field  # type: ignore[misc]
    @property
    def embedding_text(self) -> str:
        """Concatenated text for embedding generation.

        Combines title, company, seniority, work_mode, skills, description,
        and requirements into a single searchable text block.

        Returns:
            Newline-separated text suitable for embedding.
        """
        lines = [
            f"Title: {self.title}",
        ]
        if self.company:
            lines.append(f"Company: {self.company}")
        lines.append(f"Seniority: {self.seniority.value}")
        lines.append(f"Work mode: {self.work_mode.value}")
        if self.skills_required:
            lines.append(f"Required skills: {', '.join(self.skills_required)}")
        if self.description:
            lines.append(f"Description: {self.description}")
        if self.requirements_text:
            lines.append(f"Requirements: {self.requirements_text}")
        return "\n".join(lines)

