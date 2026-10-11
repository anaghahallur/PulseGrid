"""
FastAPI Application Entrypoint for ICU Bed Demand Forecasting.
Provides health, readiness, model metadata, units inspection, and multi-horizon predictions.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
from typing import List
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.api.schemas import (
    HealthResponse,
    ModelInfoResponse,
    PredictionRequest,
    PredictionResponse,
    ReadinessResponse,
    UnitInfo,
)
from src.api.service import ForecastingService, get_forecasting_service

logger = logging.getLogger("icu_forecast_api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan context manager.
    Loads and validates the trained model artifact ONCE at startup to avoid
    costly per-request deserialization overhead.
    """
    logger.info("Initializing ICU Forecasting API service...")
    service = get_forecasting_service()
    loaded = service.load_model()
    if loaded:
        logger.info("Model loaded and validated successfully during startup.")
    else:
        logger.warning("Service started but model failed to load: %s", service.error_detail)
    yield
    logger.info("Shutting down ICU Forecasting API service.")


def create_app() -> FastAPI:
    """Factory function to build and configure the FastAPI application."""
    app = FastAPI(
        title="ICU Bed Demand Forecasting API",
        description=(
            "REST API providing 1-to-24 hour ICU occupancy forecasts using "
            "streaming ADT data features, multi-horizon Ridge regression, "
            "and persistence operational baselines."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )

    # Restrict CORS to authorized frontend and API origin locations
    cors_origins = [
        "http://localhost:8501",
        "http://127.0.0.1:8501",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError):
        """Catches domain validation errors and formats as HTTP 422 Unprocessable Entity."""
        logger.warning("Validation error on %s: %s", request.url.path, str(exc))
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": str(exc), "error_type": "ValidationError"},
        )

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(request: Request, exc: RuntimeError):
        """Catches runtime / service readiness errors and formats as HTTP 503."""
        logger.error("Runtime error on %s: %s", request.url.path, str(exc))
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": str(exc), "error_type": "ServiceUnavailable"},
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        """
        Catches unexpected internal exceptions and returns sanitized HTTP 500.
        Does NOT expose internal stack traces or internal exception details to client.
        """
        logger.exception("Internal server error processing %s: %s", request.url.path, str(exc))
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "detail": "An internal server error occurred while processing the request.",
                "error_type": "InternalServerError",
            },
        )

    @app.get("/health", response_model=HealthResponse, tags=["Health"])
    async def health_check() -> HealthResponse:
        """Service liveness probe returning current operational status."""
        return HealthResponse(
            status="ok",
            service="icu-bed-demand-forecasting",
            version="0.1.0",
            timestamp=datetime.now(timezone.utc),
        )

    @app.get("/ready", response_model=ReadinessResponse, tags=["Health"])
    async def readiness_check() -> ReadinessResponse:
        """
        Service readiness probe.
        Verifies that the trained model and metadata are successfully loaded and validated.
        Returns 503 if the model is not ready.
        """
        service = get_forecasting_service()
        if not service.is_ready:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Forecasting model is not ready: {service.error_detail}",
            )

        meta = service.metadata or {}
        return ReadinessResponse(
            ready=True,
            model_loaded=True,
            model_name=meta.get("model_name"),
            horizons=meta.get("horizons"),
            features_count=meta.get("feature_count", len(meta.get("feature_names", []))),
            timestamp=datetime.now(timezone.utc),
            details="Model and metadata verified. Ready for multi-horizon inference.",
        )

    @app.get("/units", response_model=List[UnitInfo], tags=["Metadata"])
    async def list_units() -> List[UnitInfo]:
        """Returns the configured monitored ICU units and their staffed capacities."""
        service = get_forecasting_service()
        return service.get_configured_units()

    @app.get("/model/info", response_model=ModelInfoResponse, tags=["Metadata"])
    async def get_model_info() -> ModelInfoResponse:
        """Returns metadata regarding active models, horizons, features, and benchmarks."""
        service = get_forecasting_service()
        try:
            return service.get_model_info()
        except RuntimeError as re:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(re))

    @app.post("/predict", response_model=PredictionResponse, tags=["Inference"])
    async def predict_occupancy(request: PredictionRequest) -> PredictionResponse:
        """
        Generates 1-to-24 hour ICU occupancy forecasts.
        - Validates unit ID, history continuity, non-negative bed counts, and positive capacity.
        - NEVER silently treats missing historical observations as zero.
        - Executes Direct Ridge Regression or Persistence Baseline.
        - Identifies capacity exceedances and surge warnings.
        """
        service = get_forecasting_service()
        try:
            return service.predict(request)
        except ValueError as ve:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(ve))
        except RuntimeError as re:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(re))
        except Exception as e:
            logger.exception("Unexpected error during prediction: %s", str(e))
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="An internal server error occurred while processing the prediction.",
            )

    return app


app = create_app()
