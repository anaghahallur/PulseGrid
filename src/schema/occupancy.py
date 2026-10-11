"""
Hourly occupancy census schemas for ICU units.
Represents aggregated ground-truth state computed without double counting.
"""

from datetime import datetime
from pydantic import BaseModel, Field
from src.schema.events import DataSource


class HourlyOccupancy(BaseModel):
    """
    Ground-truth census record for a specific ICU unit at a specific hour boundary.
    """
    timestamp: datetime = Field(
        ...,
        description="Top-of-the-hour UTC timestamp (e.g. 2026-10-11T12:00:00Z)",
    )
    unit_id: str = Field(
        ...,
        description="ICU unit identifier (e.g. MICU, SICU)",
    )
    occupied_beds: int = Field(
        ...,
        ge=0,
        description="Instantaneous count of patients occupied at timestamp",
    )
    capacity: int = Field(
        ...,
        gt=0,
        description="Total configured operational bed capacity for the unit",
    )
    available_beds: int = Field(
        ...,
        description="Unoccupied beds (capacity - occupied_beds, can be negative if surge overflow)",
    )
    occupancy_rate: float = Field(
        ...,
        ge=0.0,
        description="Ratio of occupied beds to capacity",
    )
    admissions_count: int = Field(
        default=0,
        ge=0,
        description="Admissions occurring in the preceding hour interval [t-1, t)",
    )
    discharges_count: int = Field(
        default=0,
        ge=0,
        description="Discharges occurring in the preceding hour interval [t-1, t)",
    )
    transfers_in_count: int = Field(
        default=0,
        ge=0,
        description="Transfers into unit in preceding hour interval [t-1, t)",
    )
    transfers_out_count: int = Field(
        default=0,
        ge=0,
        description="Transfers out of unit in preceding hour interval [t-1, t)",
    )
    data_source: DataSource = Field(
        default=DataSource.SYNTHETIC,
        description="Provenance of the underlying event records",
    )
