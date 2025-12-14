from pydantic import BaseModel
from typing import List, Dict

class JobRequest(BaseModel):
    tasks: List[str]
    workers: List[int]

class JobResult(BaseModel):
    task: str
    workers: int
    execution_time: float
    metrics: Dict
