"""Central configuration for the Job Posting Radar application."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Application configuration loaded from environment variables or defaults."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    data_dir: Path = Field(
        default_factory=lambda: Path("data"),
        description="Base directory for local artifacts.",
    )
    raw_subdir: str = Field(
        default="raw",
        description="Subdirectory under data_dir for raw payload storage.",
    )
    nofluff_base_url: str = Field(
        default="https://nofluffjobs.com",
        description="Base URL for No Fluff Jobs website.",
    )
    nofluff_timeout_seconds: float = Field(
        default=30.0,
        description="HTTP request timeout for No Fluff Jobs requests.",
    )
    nofluff_rate_limit_seconds: float = Field(
        default=1.0,
        description="Minimum seconds between outbound requests to respect rate limits.",
    )
    request_max_retries: int = Field(
        default=3,
        description="Number of retries for transient HTTP failures.",
    )
    backoff_initial_seconds: float = Field(
        default=1.0,
        description="Base backoff delay in seconds for retry attempts.",
    )
    backoff_multiplier: float = Field(
        default=2.0,
        description="Multiplier applied to backoff after each retry.",
    )
    nofluff_search_path: str = Field(
        default="/api/joboffers/main",
        description="Path for the No Fluff Jobs listing XHR endpoint.",
    )
    nofluff_posting_path: str = Field(
        default="/api/posting",
        description="Path for the No Fluff Jobs posting detail endpoint.",
    )
    nofluff_page_size: int = Field(
        default=20,
        description="Number of offers to request per page from No Fluff Jobs.",
    )
    nofluff_region: str = Field(
        default="pl",
        description="Region parameter sent to No Fluff Jobs search endpoint.",
    )
    nofluff_language: str = Field(
        default="en-GB",
        description="Language parameter sent to No Fluff Jobs search endpoint.",
    )
    nofluff_salary_currency: str = Field(
        default="PLN",
        description="Salary currency filter for No Fluff Jobs search.",
    )
    nofluff_salary_period: str = Field(
        default="month",
        description="Salary period filter for No Fluff Jobs search.",
    )
    user_agent: str = Field(
        default="job-posting-radar/ingest",
        description="User agent sent to upstream sources.",
    )

    def raw_root(self) -> Path:
        """Return the root directory for raw payloads."""
        return self.data_dir / self.raw_subdir

    def source_raw_dir(self, source: str, target_date: date) -> Path:
        """Return the directory where raw payloads for a source and date should be stored."""
        return self.raw_root() / source / target_date.isoformat()


