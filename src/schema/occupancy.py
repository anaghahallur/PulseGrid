"""
Hourly occupancy census schemas for ICU units.
Represents aggregated ground-truth state computed without double counting.
"""

from datetime import datetime
from typing import Optional
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
    is_capacity_exceeded: bool = Field(
        default=False,
        description="True if occupied beds strictly exceeds total operational capacity",
    )
    exceedance_beds: int = Field(
        default=0,
        ge=0,
        description="Number of beds above capacity (max(0, occupied - capacity))",
    )
    processing_watermark: Optional[datetime] = Field(
        default=None,
        description="Timestamp when this census record was computed",
    )
    data_source: DataSource = Field(
        default=DataSource.SYNTHETIC,
        description="Provenance of the underlying event records",
    )

    def to_dict(self) -> dict:
        """Serialize occupancy model to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "unit_id": self.unit_id,
            "occupied_beds": self.occupied_beds,
            "capacity": self.capacity,
            "available_beds": self.available_beds,
            "occupancy_rate": self.occupancy_rate,
            "is_capacity_exceeded": self.is_capacity_exceeded,
            "exceedance_beds": self.exceedance_beds,
            "admissions_count": self.admissions_count,
            "discharges_count": self.discharges_count,
            "transfers_in_count": self.transfers_in_count,
            "transfers_out_count": self.transfers_out_count,
            "data_source": self.data_source.value,
            "processing_watermark": self.processing_watermark.isoformat() if self.processing_watermark else None,
        }
