# Agent Workflows & Execution Runbook

This runbook lists the exact terminal commands required to run, test, reproduce, and serve the ICU Bed Demand Forecasting system.

---

## 1. Environment Setup

```powershell
# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Option A: Install all optional dependencies (testing, ML, and serving)
uv pip install -e ".[test,ml,serving]"
# Or via standard pip:
pip install -e ".[test,ml,serving]"

# Option B: Serving-only installation (now self-contained for inference)
pip install -e ".[serving]"
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

# Audit port assignments and verify container socket connectivity
python scripts/check_ports.py

# View container logs if troubleshooting
docker compose logs kafka --tail 50
docker compose logs namenode --tail 50

# Stop infrastructure
docker compose down
```

---

## 3. Streaming Ingestion to HDFS Runbook

```powershell
# 1. Produce streaming synthetic events into Kafka topic 'icu-events'
python -c "
from datetime import datetime, timezone
from src.producer.simulator import ICUSimulator
from src.producer.kafka_producer import ICUKafkaProducer
sim = ICUSimulator(start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), days=7, random_seed=42)
events = sim.generate_events()
prod = ICUKafkaProducer()
prod.publish_stream(events[:100], raise_on_error=True)
print(f'Published {len(events[:100])} events to Kafka')
"

# 2. Sink events from Kafka micro-batch into HDFS lake (/raw/icu_events/)
python -c "
from src.storage.kafka_sink import KafkaHDFSSink
sink = KafkaHDFSSink()
stats = sink.poll_and_sink(max_messages=100, poll_timeout=2.0, idle_timeout=5.0)
print('Sink results:', stats)
sink.close()
"

# 3. Reconstruct hourly census from HDFS raw lake and persist to /processed/icu_occupancy_hourly/
python -c "
from src.storage.hdfs_client import HDFSClient
from src.processing.occupancy_calculator import OccupancyCalculator
from src.schema.events import ICUEvent
from src.config import get_settings
settings = get_settings()
hdfs = HDFSClient()
raw_records = hdfs.read_all_jsonl(settings.hdfs_raw_path)
events = [ICUEvent.model_validate(r) for r in raw_records]
calc = OccupancyCalculator(unit_capacities=settings.icu_unit_capacities)
df = calc.reconstruct(events)
saved_path = calc.persist_occupancy_to_hdfs(df, hdfs, hdfs_target_dir=settings.hdfs_processed_path)
print(f'Reconstructed and persisted census to: {saved_path}')
"
```

---

## 4. Synthetic Data Generation

```powershell
# Generate 90-day canonical dataset with TSICU surge scenario
python scripts/generate_synthetic_data.py --days 90 --seed 42 --surge
```
- Outputs events: `data/synthetic/simulated_icu_events.jsonl`
- Outputs hourly census: `data/synthetic/simulated_hourly_occupancy.parquet`

---

## 5. Model Training & Evaluation

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

## 6. Running the Backend API (FastAPI)

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

## 7. Running the Dashboard (Streamlit)

```powershell
# Run Streamlit dashboard
streamlit run src/dashboard/app.py --server.port 8501
```
- Dashboard URL: `http://localhost:8501`

---

## 8. Running the Automated Test Suite

```powershell
# Run entire test suite (79 unit + integration tests)
pytest -v

# Run only unit tests
pytest tests/unit/ -v

# Run only integration tests (requires Docker containers running)
pytest tests/integration/ -v
```

---

## 9. Knowledge Graph Maintenance (Graphify)

Whenever source files or configurations are meaningfully modified:
```powershell
graphify update .
```
This updates the AST knowledge graph in `graphify-out/` to keep structural context synchronized.
