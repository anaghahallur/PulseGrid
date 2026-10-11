"""Shared pytest fixtures."""

from datetime import datetime, timezone
import pytest

from src.schema.events import DataSource, EventType, ICUEvent


@pytest.fixture
def sample_units():
    return {
        "MICU": 24,
        "SICU": 20,
        "CCU": 16,
        "TSICU": 16,
        "CSRU": 18,
    }


@pytest.fixture
def base_timestamp():
    return datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)
