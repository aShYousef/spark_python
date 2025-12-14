# Spark Data Processor

Cloud-based distributed data processing platform using PySpark.

## Features
- Upload datasets
- Run Spark jobs
- Distributed ML (1–8 workers)
- Performance benchmarking

## Run
```bash
pip install -r requirements.txt
uvicorn backend.app.main:app

---

# ⚙️ .replit
```ini
run = "uvicorn backend.app.main:app --host=0.0.0.0 --port=8000"
