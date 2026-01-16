"""Unit tests for scripts/search_jobs.py - Search CLI."""

import sys
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# Import the module functions directly
sys.path.insert(0, "scripts")
from search_jobs import (
    CollapsedResult,
    build_filter,
    collapse_duplicates,
    format_locations,
    format_salary,
    parse_args,
)


class TestParseArgs:
    """Tests for parse_args function."""

    def test_required_query(self) -> None:
        """Test that query is required."""
        with pytest.raises(SystemExit):
            with patch.object(sys, "argv", ["search_jobs.py"]):
                parse_args()

    def test_basic_query(self) -> None:
        """Test basic query parsing."""
        with patch.object(sys, "argv", ["search_jobs.py", "--q", "python developer"]):
            args = parse_args()
            assert args.q == "python developer"

    def test_default_limit(self) -> None:
        """Test default limit value."""
        with patch.object(sys, "argv", ["search_jobs.py", "--q", "test"]):
            args = parse_args()
            assert args.limit == 20

    def test_custom_limit(self) -> None:
        """Test custom limit value."""
        with patch.object(sys, "argv", ["search_jobs.py", "--q", "test", "--limit", "50"]):
            args = parse_args()
            assert args.limit == 50

    def test_mode_filter(self) -> None:
        """Test mode filter parsing."""
        with patch.object(
            sys, "argv", ["search_jobs.py", "--q", "test", "--mode", "remote"]
        ):
            args = parse_args()
            assert args.mode == "remote"

    def test_city_filter(self) -> None:
        """Test city filter parsing."""
        with patch.object(
            sys, "argv", ["search_jobs.py", "--q", "test", "--city", "Warszawa"]
        ):
            args = parse_args()
            assert args.city == "Warszawa"

    def test_source_filter(self) -> None:
        """Test source filter parsing."""
        with patch.object(
            sys, "argv", ["search_jobs.py", "--q", "test", "--source", "nofluff"]
        ):
            args = parse_args()
            assert args.source == "nofluff"

    def test_no_collapse_flag(self) -> None:
        """Test no-collapse flag."""
        with patch.object(sys, "argv", ["search_jobs.py", "--q", "test", "--no-collapse"]):
            args = parse_args()
            assert args.no_collapse is True

    def test_no_collapse_default(self) -> None:
        """Test no-collapse default is False."""
        with patch.object(sys, "argv", ["search_jobs.py", "--q", "test"]):
            args = parse_args()
            assert args.no_collapse is False


class TestBuildFilter:
    """Tests for build_filter function."""

    def test_no_filters_returns_none(self) -> None:
        """Test that no filters returns None."""
        result = build_filter(mode=None, city=None, source=None)
        assert result is None

    def test_mode_filter(self) -> None:
        """Test mode filter creation."""
        result = build_filter(mode="remote", city=None, source=None)
        assert result is not None
        assert len(result.must) == 1
        assert result.must[0].key == "work_mode"

    def test_city_filter(self) -> None:
        """Test city filter creation."""
        result = build_filter(mode=None, city="Kraków", source=None)
        assert result is not None
        assert len(result.must) == 1
        assert result.must[0].key == "locations[].city"

    def test_source_filter(self) -> None:
        """Test source filter creation."""
        result = build_filter(mode=None, city=None, source="justjoin")
        assert result is not None
        assert len(result.must) == 1
        assert result.must[0].key == "source"

    def test_all_filters(self) -> None:
        """Test all filters combined."""
        result = build_filter(mode="hybrid", city="Wrocław", source="nofluff")
        assert result is not None
        assert len(result.must) == 3


class TestCollapseDuplicates:
    """Tests for collapse_duplicates function."""

    def test_empty_results(self) -> None:
        """Test collapsing empty results."""
        result = collapse_duplicates([])
        assert result == []

    def test_single_result(self) -> None:
        """Test collapsing single result."""
        point = MagicMock()
        point.id = "1"
        point.score = 0.9
        point.payload = {
            "content_hash": "hash1",
            "source": "nofluff",
            "job_url": "https://nfj.com/1",
        }

        result = collapse_duplicates([point])

        assert len(result) == 1
        assert result[0].score == 0.9
        assert result[0].sources == ["nofluff"]

    def test_no_duplicates(self) -> None:
        """Test collapsing with no duplicates."""
        points = []
        for i in range(3):
            point = MagicMock()
            point.id = str(i)
            point.score = 0.9 - (i * 0.1)
            point.payload = {
                "content_hash": f"hash{i}",
                "source": "nofluff",
                "job_url": f"https://nfj.com/{i}",
            }
            points.append(point)

        result = collapse_duplicates(points)

        assert len(result) == 3
        # Check sorted by score
        assert result[0].score >= result[1].score >= result[2].score

    def test_with_duplicates(self) -> None:
        """Test collapsing with duplicates."""
        point1 = MagicMock()
        point1.id = "1"
        point1.score = 0.9
        point1.payload = {
            "content_hash": "same_hash",
            "source": "nofluff",
            "job_url": "https://nfj.com/1",
        }

        point2 = MagicMock()
        point2.id = "2"
        point2.score = 0.85
        point2.payload = {
            "content_hash": "same_hash",
            "source": "justjoin",
            "job_url": "https://jj.it/1",
        }

        result = collapse_duplicates([point1, point2])

        assert len(result) == 1
        assert len(result[0].sources) == 2
        assert "nofluff" in result[0].sources
        assert "justjoin" in result[0].sources
        assert result[0].score == 0.9  # Higher score kept

    def test_keeps_highest_score(self) -> None:
        """Test that highest score is kept for duplicates."""
        point1 = MagicMock()
        point1.id = "1"
        point1.score = 0.7
        point1.payload = {
            "content_hash": "same",
            "source": "nofluff",
            "job_url": "",
            "title": "Low score",
        }

        point2 = MagicMock()
        point2.id = "2"
        point2.score = 0.95
        point2.payload = {
            "content_hash": "same",
            "source": "justjoin",
            "job_url": "",
            "title": "High score",
        }

        result = collapse_duplicates([point1, point2])

        assert result[0].score == 0.95
        assert result[0].payload["title"] == "High score"


class TestFormatSalary:
    """Tests for format_salary function."""

    def test_empty_salaries(self) -> None:
        """Test formatting empty salaries list."""
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
        assert "month" in result

    def test_from_only_salary(self) -> None:
        """Test formatting salary with from amount only."""
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
        """Test formatting salary with to amount only."""
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

    def test_with_employment_type(self) -> None:
        """Test formatting salary with employment type."""
        salaries = [
            {
                "from_amount": 25000,
                "to_amount": 35000,
                "currency": "PLN",
                "period": "month",
                "employment_type": "b2b",
            }
        ]
        result = format_salary(salaries)
        assert "(b2b)" in result

    def test_multiple_salaries(self) -> None:
        """Test formatting multiple salaries."""
        salaries = [
            {
                "from_amount": 18000,
                "to_amount": 25000,
                "currency": "PLN",
                "period": "month",
                "employment_type": "uop",
            },
            {
                "from_amount": 22000,
                "to_amount": 32000,
                "currency": "PLN",
                "period": "month",
                "employment_type": "b2b",
            },
        ]
        result = format_salary(salaries)
        assert "(uop)" in result
        assert "(b2b)" in result
        assert "|" in result


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
        ]
        result = format_locations(locations)
        assert "Warszawa" in result
        assert "Kraków" in result

    def test_locations_truncated(self) -> None:
        """Test that more than 3 locations are truncated."""
        locations = [
            {"city": "Warszawa", "country": "Poland"},
            {"city": "Kraków", "country": "Poland"},
            {"city": "Wrocław", "country": "Poland"},
            {"city": "Poznań", "country": "Poland"},
            {"city": "Gdańsk", "country": "Poland"},
        ]
        result = format_locations(locations)
        assert "..." in result
        assert "Poznań" not in result
        assert "Gdańsk" not in result

    def test_locations_deduplicates(self) -> None:
        """Test that duplicate cities are removed."""
        locations = [
            {"city": "Warszawa", "country": "Poland"},
            {"city": "Warszawa", "country": "Poland"},
        ]
        result = format_locations(locations)
        assert result == "Warszawa"

    def test_locations_missing_city(self) -> None:
        """Test handling locations with missing city."""
        locations = [
            {"country": "Poland"},
            {"city": None, "country": "Poland"},
        ]
        result = format_locations(locations)
        assert result == "Unknown"


class TestCollapsedResultDataclass:
    """Tests for CollapsedResult dataclass."""

    def test_default_values(self) -> None:
        """Test CollapsedResult default values."""
        result = CollapsedResult(score=0.8, payload={"title": "Dev"})
        assert result.score == 0.8
        assert result.payload == {"title": "Dev"}
        assert result.sources == []
        assert result.urls == {}

    def test_with_all_fields(self) -> None:
        """Test CollapsedResult with all fields."""
        result = CollapsedResult(
            score=0.9,
            payload={"title": "Engineer"},
            sources=["nofluff", "justjoin"],
            urls={"nofluff": "url1", "justjoin": "url2"},
        )
        assert len(result.sources) == 2
        assert len(result.urls) == 2
