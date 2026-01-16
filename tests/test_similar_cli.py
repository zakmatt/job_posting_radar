"""Unit tests for scripts/similar_jobs.py - Similar Jobs CLI."""

import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# Import the module functions directly
sys.path.insert(0, "scripts")
from similar_jobs import (
    CollapsedResult,
    collapse_duplicates,
    format_locations,
    format_salary,
    parse_args,
)


class TestParseArgs:
    """Tests for parse_args function."""

    def test_required_source(self) -> None:
        """Test that source is required."""
        with pytest.raises(SystemExit):
            with patch.object(sys, "argv", ["similar_jobs.py", "--source-id", "ABC123"]):
                parse_args()

    def test_required_source_id(self) -> None:
        """Test that source_id is required."""
        with pytest.raises(SystemExit):
            with patch.object(sys, "argv", ["similar_jobs.py", "--source", "nofluff"]):
                parse_args()

    def test_basic_args(self) -> None:
        """Test basic argument parsing."""
        with patch.object(
            sys,
            "argv",
            ["similar_jobs.py", "--source", "nofluff", "--source-id", "ABC123"],
        ):
            args = parse_args()
            assert args.source == "nofluff"
            assert args.source_id == "ABC123"

    def test_default_limit(self) -> None:
        """Test default limit value."""
        with patch.object(
            sys,
            "argv",
            ["similar_jobs.py", "--source", "nofluff", "--source-id", "ABC123"],
        ):
            args = parse_args()
            assert args.limit == 20

    def test_custom_limit(self) -> None:
        """Test custom limit value."""
        with patch.object(
            sys,
            "argv",
            [
                "similar_jobs.py",
                "--source",
                "nofluff",
                "--source-id",
                "ABC123",
                "--limit",
                "30",
            ],
        ):
            args = parse_args()
            assert args.limit == 30

    def test_cross_source_flag(self) -> None:
        """Test cross-source flag."""
        with patch.object(
            sys,
            "argv",
            [
                "similar_jobs.py",
                "--source",
                "nofluff",
                "--source-id",
                "ABC123",
                "--cross-source",
            ],
        ):
            args = parse_args()
            assert args.cross_source is True

    def test_cross_source_default(self) -> None:
        """Test cross-source default is False."""
        with patch.object(
            sys,
            "argv",
            ["similar_jobs.py", "--source", "nofluff", "--source-id", "ABC123"],
        ):
            args = parse_args()
            assert args.cross_source is False

    def test_exclude_same_company_flag(self) -> None:
        """Test exclude-same-company flag."""
        with patch.object(
            sys,
            "argv",
            [
                "similar_jobs.py",
                "--source",
                "nofluff",
                "--source-id",
                "ABC123",
                "--exclude-same-company",
            ],
        ):
            args = parse_args()
            assert args.exclude_same_company is True

    def test_no_collapse_flag(self) -> None:
        """Test no-collapse flag."""
        with patch.object(
            sys,
            "argv",
            [
                "similar_jobs.py",
                "--source",
                "nofluff",
                "--source-id",
                "ABC123",
                "--no-collapse",
            ],
        ):
            args = parse_args()
            assert args.no_collapse is True

    def test_source_choices(self) -> None:
        """Test that source only accepts valid choices."""
        with pytest.raises(SystemExit):
            with patch.object(
                sys,
                "argv",
                ["similar_jobs.py", "--source", "indeed", "--source-id", "ABC123"],
            ):
                parse_args()


class TestCollapseDuplicates:
    """Tests for collapse_duplicates function."""

    def test_empty_results(self) -> None:
        """Test collapsing empty results."""
        result = collapse_duplicates([], exclude_source_id="ref")
        assert result == []

    def test_excludes_reference_job(self) -> None:
        """Test that reference job is excluded."""
        point = MagicMock()
        point.id = "1"
        point.score = 0.9
        point.payload = {
            "content_hash": "hash1",
            "source": "nofluff",
            "source_id": "ABC123",
            "job_url": "",
        }

        result = collapse_duplicates([point], exclude_source_id="ABC123")

        assert len(result) == 0

    def test_keeps_non_reference_jobs(self) -> None:
        """Test that non-reference jobs are kept."""
        point1 = MagicMock()
        point1.id = "1"
        point1.score = 0.9
        point1.payload = {
            "content_hash": "hash1",
            "source": "nofluff",
            "source_id": "ABC123",
            "job_url": "",
        }

        point2 = MagicMock()
        point2.id = "2"
        point2.score = 0.85
        point2.payload = {
            "content_hash": "hash2",
            "source": "nofluff",
            "source_id": "DEF456",
            "job_url": "",
        }

        result = collapse_duplicates([point1, point2], exclude_source_id="ABC123")

        assert len(result) == 1
        assert result[0].payload["source_id"] == "DEF456"

    def test_collapses_duplicates(self) -> None:
        """Test that duplicates are collapsed."""
        point1 = MagicMock()
        point1.id = "1"
        point1.score = 0.9
        point1.payload = {
            "content_hash": "same_hash",
            "source": "nofluff",
            "source_id": "NF123",
            "job_url": "https://nfj.com/1",
        }

        point2 = MagicMock()
        point2.id = "2"
        point2.score = 0.85
        point2.payload = {
            "content_hash": "same_hash",
            "source": "justjoin",
            "source_id": "JJ123",
            "job_url": "https://jj.it/1",
        }

        result = collapse_duplicates([point1, point2], exclude_source_id="other")

        assert len(result) == 1
        assert len(result[0].sources) == 2

    def test_keeps_highest_score(self) -> None:
        """Test that highest score is kept for duplicates."""
        point1 = MagicMock()
        point1.id = "1"
        point1.score = 0.7
        point1.payload = {
            "content_hash": "same",
            "source": "nofluff",
            "source_id": "1",
            "job_url": "",
            "title": "Low",
        }

        point2 = MagicMock()
        point2.id = "2"
        point2.score = 0.95
        point2.payload = {
            "content_hash": "same",
            "source": "justjoin",
            "source_id": "2",
            "job_url": "",
            "title": "High",
        }

        result = collapse_duplicates([point1, point2], exclude_source_id="ref")

        assert result[0].score == 0.95
        assert result[0].payload["title"] == "High"

    def test_sorted_by_score_descending(self) -> None:
        """Test results are sorted by score descending."""
        points = []
        scores = [0.5, 0.9, 0.7, 0.8]
        for i, score in enumerate(scores):
            point = MagicMock()
            point.id = str(i)
            point.score = score
            point.payload = {
                "content_hash": f"hash{i}",
                "source": "nofluff",
                "source_id": f"id{i}",
                "job_url": "",
            }
            points.append(point)

        result = collapse_duplicates(points, exclude_source_id="ref")

        assert len(result) == 4
        assert result[0].score == 0.9
        assert result[1].score == 0.8
        assert result[2].score == 0.7
        assert result[3].score == 0.5


class TestFormatSalary:
    """Tests for format_salary function."""

    def test_empty_salaries(self) -> None:
        """Test formatting empty salaries."""
        result = format_salary([])
        assert result == "Not disclosed"

    def test_full_range_salary(self) -> None:
        """Test formatting salary with full range."""
        salaries = [
            {
                "from_amount": 20000,
                "to_amount": 30000,
                "currency": "PLN",
                "period": "month",
            }
        ]
        result = format_salary(salaries)
        assert "20,000" in result
        assert "30,000" in result
        assert "PLN" in result

    def test_from_only_salary(self) -> None:
        """Test formatting from-only salary."""
        salaries = [
            {
                "from_amount": 15000,
                "to_amount": None,
                "currency": "PLN",
                "period": "month",
            }
        ]
        result = format_salary(salaries)
        assert "15,000+" in result

    def test_to_only_salary(self) -> None:
        """Test formatting to-only salary."""
        salaries = [
            {
                "from_amount": None,
                "to_amount": 40000,
                "currency": "PLN",
                "period": "month",
            }
        ]
        result = format_salary(salaries)
        assert "up to 40,000" in result

    def test_no_amounts(self) -> None:
        """Test salary with no amounts returns Not disclosed."""
        salaries = [
            {
                "from_amount": None,
                "to_amount": None,
                "currency": "PLN",
                "period": "month",
            }
        ]
        result = format_salary(salaries)
        assert result == "Not disclosed"


class TestFormatLocations:
    """Tests for format_locations function."""

    def test_empty_locations(self) -> None:
        """Test formatting empty locations."""
        result = format_locations([])
        assert result == "Unknown"

    def test_single_location(self) -> None:
        """Test formatting single location."""
        locations = [{"city": "Warszawa", "country": "Poland"}]
        result = format_locations(locations)
        assert result == "Warszawa"

    def test_multiple_locations(self) -> None:
        """Test formatting multiple locations."""
        locations = [
            {"city": "Warszawa", "country": "Poland"},
            {"city": "Kraków", "country": "Poland"},
            {"city": "Wrocław", "country": "Poland"},
        ]
        result = format_locations(locations)
        assert "Warszawa" in result
        assert "Kraków" in result
        assert "Wrocław" in result

    def test_truncation_at_three(self) -> None:
        """Test that locations are truncated at 3."""
        locations = [
            {"city": "A", "country": "PL"},
            {"city": "B", "country": "PL"},
            {"city": "C", "country": "PL"},
            {"city": "D", "country": "PL"},
        ]
        result = format_locations(locations)
        assert "..." in result
        assert "D" not in result

    def test_deduplication(self) -> None:
        """Test that duplicate cities are removed."""
        locations = [
            {"city": "Warszawa", "country": "Poland"},
            {"city": "Warszawa", "country": "Poland"},
            {"city": "Kraków", "country": "Poland"},
        ]
        result = format_locations(locations)
        # Should only appear once
        assert result.count("Warszawa") == 1


class TestCollapsedResultDataclass:
    """Tests for CollapsedResult dataclass."""

    def test_creation_with_defaults(self) -> None:
        """Test creating CollapsedResult with defaults."""
        result = CollapsedResult(score=0.8, payload={"title": "Dev"})
        assert result.score == 0.8
        assert result.sources == []
        assert result.urls == {}

    def test_creation_with_all_fields(self) -> None:
        """Test creating CollapsedResult with all fields."""
        result = CollapsedResult(
            score=0.9,
            payload={"title": "Engineer"},
            sources=["nofluff"],
            urls={"nofluff": "https://nfj.com/1"},
        )
        assert result.score == 0.9
        assert len(result.sources) == 1
        assert result.urls["nofluff"] == "https://nfj.com/1"
