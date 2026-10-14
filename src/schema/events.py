"""
Event schemas and validation rules for ICU Admission, Transfer, and Discharge (ADT) events.
Enforces clinical transition consistency and idempotency requirements.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, field_validator, model_validator


class EventType(str, Enum):
    """Categorical types of patient bed events."""
    ADMISSION = "ADMISSION"
    TRANSFER = "TRANSFER"
    DISCHARGE = "DISCHARGE"


class DataSource(str, Enum):
    """Provenance tracking for events."""
    SYNTHETIC = "SYNTHETIC"
    MIMIC_DEMO = "MIMIC_DEMO"
    MIMIC_CLINICAL = "MIMIC_CLINICAL"


class ICUEvent(BaseModel):
    """
    Validated representation of an ICU Patient Transition Event.
    Serves as the canonical contract across Kafka, HDFS, and downstream computation.
    """
    event_id: UUID = Field(
        default_factory=uuid4,
        description="Unique event UUID serving as the primary idempotency key",
    )
    patient_id: str = Field(
        ...,
        min_length=1,
        description="Anonymized patient identifier (e.g. P-100234)",
    )
    encounter_id: str = Field(
        ...,
        min_length=1,
        description="Hospital encounter / stay identifier (e.g. ENC-50012)",
    )
    event_type: EventType = Field(
        ...,
        description="Type of clinical transition: ADMISSION, TRANSFER, or DISCHARGE",
    )
    event_timestamp: datetime = Field(
        ...,
        description="UTC timestamp of the transition event",
    )
    from_unit: Optional[str] = Field(
        default=None,
        description="Originating unit identifier (required for TRANSFER and DISCHARGE)",
    )
    to_unit: Optional[str] = Field(
        default=None,
        description="Destination unit identifier (required for ADMISSION and TRANSFER)",
    )
    bed_id: Optional[str] = Field(
        default=None,
        description="Specific bed identifier within the unit",
    )
    version: int = Field(
        default=1,
        ge=1,
        description="Record version number for state amendments and corrections",
    )
    is_correction: bool = Field(
        default=False,
        description="Indicates whether this event supersedes an earlier erroneous record",
    )
    original_event_id: Optional[UUID] = Field(
        default=None,
        description="Reference to superseded event UUID when is_correction is True",
    )
    data_source: DataSource = Field(
        default=DataSource.SYNTHETIC,
        description="Data provenance label (SYNTHETIC, MIMIC_DEMO, MIMIC_CLINICAL)",
    )
    metadata: Optional[dict] = Field(
        default=None,
        description="Optional auxiliary attributes (e.g. admission_type, severity score)",
    )

    @field_validator("event_timestamp")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        """Ensure timestamp has explicit timezone awareness in UTC."""
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_transition_integrity(self) -> "ICUEvent":
        """
        Validates clinical unit routing consistency:
        - ADMISSION must specify to_unit and have from_unit is None.
        - DISCHARGE must specify from_unit and have to_unit is None.
        - TRANSFER must specify both from_unit and to_unit, and from_unit != to_unit.
        - If is_correction is True, original_event_id must be provided.
        """
        if self.event_type == EventType.ADMISSION:
            if not self.to_unit:
                raise ValueError("ADMISSION event must specify a destination 'to_unit'.")
            if self.from_unit is not None:
                raise ValueError("ADMISSION event must not specify 'from_unit' (patient is admitted from outside).")

        elif self.event_type == EventType.DISCHARGE:
            if not self.from_unit:
                raise ValueError("DISCHARGE event must specify an originating 'from_unit'.")
            if self.to_unit is not None:
                raise ValueError("DISCHARGE event must not specify 'to_unit' (patient is leaving the unit).")

        elif self.event_type == EventType.TRANSFER:
            if not self.from_unit:
                raise ValueError("TRANSFER event must specify 'from_unit'.")
            if not self.to_unit:
                raise ValueError("TRANSFER event must specify 'to_unit'.")
            if self.from_unit == self.to_unit:
                raise ValueError(
                    f"TRANSFER 'from_unit' and 'to_unit' cannot be identical ('{self.from_unit}')."
                )

        if self.is_correction and self.original_event_id is None:
            raise ValueError(
                "When 'is_correction' is True, 'original_event_id' must reference the superseded event."
            )

        return self

    def to_dict(self) -> dict:
        """Convert event to serialized dictionary with ISO string timestamps and UUID strings."""
        return {
            "event_id": str(self.event_id),
            "patient_id": self.patient_id,
            "encounter_id": self.encounter_id,
            "event_type": self.event_type.value,
            "event_timestamp": self.event_timestamp.isoformat(),
            "from_unit": self.from_unit,
            "to_unit": self.to_unit,
            "bed_id": self.bed_id,
            "version": self.version,
            "is_correction": self.is_correction,
            "original_event_id": str(self.original_event_id) if self.original_event_id else None,
            "data_source": self.data_source.value,
            "metadata": self.metadata,
        }


class VitalSignEvent(BaseModel):
    """
    Validated representation of an ICU patient vital-sign observation event.
    Standardized payload for Kafka streaming and raw HDFS lake storage.

    Field Optionality Rationale:
    In clinical ICU telemetry, continuous bedside monitors stream multi-parameter data.
    Different modalities update at varying frequencies (e.g., continuous pulse oximetry
    and ECG streaming every few seconds, vs. episodic NIBP cuff inflations every 15-30 minutes).
    Furthermore, individual sensor leads may momentarily detach or suffer artifact.
    Therefore, individual measurement channels are optional, but every valid VitalSignEvent
    must contain at least one valid measurement channel to prevent empty telemetry frames.
    """

    event_id: UUID = Field(
        default_factory=uuid4,
        description="Unique event UUID serving as idempotency key",
    )
    patient_id: str = Field(
        ...,
        min_length=1,
        description="Anonymized patient identifier (e.g. SYN-P-000101)",
    )
    encounter_id: str = Field(
        ...,
        min_length=1,
        description="Hospital stay / encounter identifier (e.g. SYN-ENC-000101)",
    )
    unit_id: str = Field(
        ...,
        min_length=1,
        description="ICU unit where telemetry observation occurred (e.g. MICU, SICU, CCU, TSICU, CSRU)",
    )
    event_timestamp: datetime = Field(
        ...,
        description="Observation timestamp in UTC",
    )
    heart_rate: Optional[float] = Field(
        default=None,
        ge=20.0,
        le=300.0,
        description="Heart rate in beats per minute (plausible clinical range: 20-300 bpm)",
    )
    temperature_c: Optional[float] = Field(
        default=None,
        ge=25.0,
        le=45.0,
        description="Body temperature in degrees Celsius (plausible clinical range: 25.0-45.0 °C)",
    )
    systolic_bp: Optional[float] = Field(
        default=None,
        ge=40.0,
        le=300.0,
        description="Systolic blood pressure in mmHg (plausible clinical range: 40-300 mmHg)",
    )
    diastolic_bp: Optional[float] = Field(
        default=None,
        ge=20.0,
        le=200.0,
        description="Diastolic blood pressure in mmHg (plausible clinical range: 20-200 mmHg)",
    )
    respiratory_rate: Optional[float] = Field(
        default=None,
        ge=4.0,
        le=80.0,
        description="Respiratory rate in breaths per minute (plausible clinical range: 4-80 bpm)",
    )
    spo2: Optional[float] = Field(
        default=None,
        ge=50.0,
        le=100.0,
        description="Pulse oximetry oxygen saturation percentage (plausible clinical range: 50.0-100.0 %)",
    )
    data_source: DataSource = Field(
        default=DataSource.SYNTHETIC,
        description="Data provenance label (SYNTHETIC, MIMIC_DEMO, MIMIC_CLINICAL)",
    )
    metadata: Optional[dict] = Field(
        default=None,
        description="Auxiliary telemetry attributes (e.g. device_id, sensor_status, is_synthetic)",
    )

    @field_validator("event_timestamp")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        """Ensure timestamp has explicit timezone awareness in UTC."""
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)

    @field_validator("unit_id")
    @classmethod
    def normalize_unit_id(cls, v: str) -> str:
        return v.strip().upper()

    @model_validator(mode="after")
    def validate_vitals_payload(self) -> "VitalSignEvent":
        """
        Validates:
        1. At least one vital sign measurement must be present.
        2. If both systolic and diastolic blood pressure are provided, systolic >= diastolic.
        """
        vital_measurements = [
            self.heart_rate,
            self.temperature_c,
            self.systolic_bp,
            self.diastolic_bp,
            self.respiratory_rate,
            self.spo2,
        ]
        if all(v is None for v in vital_measurements):
            raise ValueError(
                "VitalSignEvent must contain at least one valid measurement "
                "(heart_rate, temperature_c, systolic_bp, diastolic_bp, respiratory_rate, or spo2)."
            )

        if self.systolic_bp is not None and self.diastolic_bp is not None:
            if self.systolic_bp < self.diastolic_bp:
                raise ValueError(
                    f"Systolic BP ({self.systolic_bp} mmHg) cannot be lower than "
                    f"diastolic BP ({self.diastolic_bp} mmHg)."
                )

        return self

    def to_dict(self) -> dict:
        """Convert vital sign event to serialized dictionary with ISO string timestamps and UUID strings."""
        return {
            "event_id": str(self.event_id),
            "patient_id": self.patient_id,
            "encounter_id": self.encounter_id,
            "unit_id": self.unit_id,
            "event_timestamp": self.event_timestamp.isoformat(),
            "heart_rate": self.heart_rate,
            "temperature_c": self.temperature_c,
            "systolic_bp": self.systolic_bp,
            "diastolic_bp": self.diastolic_bp,
            "respiratory_rate": self.respiratory_rate,
            "spo2": self.spo2,
            "data_source": self.data_source.value,
            "metadata": self.metadata,
        }
