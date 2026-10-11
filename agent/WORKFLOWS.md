# Agent Workflows & Execution Runbook

This runbook lists the exact terminal commands required to run, test, reproduce, and serve the ICU Bed Demand Forecasting system.

---

## 1. Environment Setup

```powershell
# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Install all optional dependencies (testing, ML, and serving)
uv pip install -e ".[test,ml,serving]"
# Or via standard pip:
pip install -e ".[test,ml,serving]"
```

---

## 2. Infrastructure Operations (Docker Compose)

```powershell
# Validate docker-compose configuration
docker compose config

# Launch Kafka, ZooKeeper, NameNode, and DataNode
docker compose up -d

# Check container status and health
docker compose ps

# View container logs if troubleshooting
docker compose logs kafka --tail 50
docker compose logs namenode --tail 50

# Stop infrastructure
docker compose down
```

---

## 3. Synthetic Data Generation

```powershell
# Generate 90-day canonical dataset with TSICU surge scenario
python scripts/generate_synthetic_data.py --days 90 --seed 42 --surge
```
- Outputs events: `data/synthetic/simulated_icu_events.jsonl`
- Outputs hourly census: `data/synthetic/simulated_hourly_occupancy.parquet`

---

## 4. Model Training & Evaluation

```powershell
# Run chronological training, validation selection, test evaluation, and artifact serialization
python scripts/train_eval_forecaster.py `
    --data-path data/synthetic/simulated_hourly_occupancy.parquet `
    --output-dir models `
    --seed 42
```
- Artifacts written to `models/`:
  - `icu_occupancy_forecaster.joblib`
  - `feature_metadata.json`
  - `evaluation_report.json`
  - `EVALUATION_REPORT.md`

---

## 5. Running the Backend API (FastAPI)

```powershell
# Run FastAPI server locally with Uvicorn (reload mode for development)
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Endpoints available:
- Swagger Documentation: `http://localhost:8000/docs`
- Health: `http://localhost:8000/health`
- Readiness: `http://localhost:8000/ready`
- Units: `http://localhost:8000/units`
- Model Info: `http://localhost:8000/model/info`
- Prediction: `POST http://localhost:8000/predict`

---

## 6. Running the Dashboard (Streamlit)

```powershell
# Run Streamlit dashboard
streamlit run src/dashboard/app.py --server.port 8501
```
- Dashboard URL: `http://localhost:8501`

---

## 7. Running the Automated Test Suite

```powershell
# Run entire test suite (unit + integration)
pytest -v

# Run only unit tests
pytest tests/unit/ -v

# Run only integration tests (requires Docker containers running)
pytest tests/integration/ -v
```

---

## 8. Knowledge Graph Maintenance (Graphify)

Whenever source files or configurations are meaningfully modified:
```powershell
graphify update .
```
This updates the AST knowledge graph in `graphify-out/` to keep structural context synchronized.
