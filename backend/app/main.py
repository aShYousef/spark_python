from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from .storage import save_upload
from .job_manager import run_jobs

app = FastAPI(title="Spark Data Processor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

LAST_RESULTS = []

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    path = save_upload(file)
    return {"message": "File uploaded", "path": path}


@app.post("/run")
async def run_processing(
    file_path: str,
    tasks: list[str],
    workers: list[int]
):
    global LAST_RESULTS
    LAST_RESULTS = run_jobs(file_path, tasks, workers)
    return {"status": "completed", "results": LAST_RESULTS}


@app.get("/results")
def get_results():
    return LAST_RESULTS

