import os
import uuid
import json

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv


load_dotenv()
REDIS_IP_ADDRESS = os.getenv("REDIS_IP_ADDRESS")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
FASTAPI_PORT = os.getenv("FASTAPI_PORT")

HIGH_PRIORITY_QUEUE_NAME = "high_priority_queue"
LOW_PRIORITY_QUEUE_NAME = "low_priority_queue"

app = FastAPI()


class ProcessingData(BaseModel):
    text: str


class ProcessingRequest(BaseModel):
    data: list[ProcessingData]


class JobStatusResponse(BaseModel):
    job_id: str


class JobResultResponse(BaseModel):
    job_id: str
    result: list[float]


r = redis.Redis(
    host=REDIS_IP_ADDRESS, port=int(REDIS_PORT), db=0, password=REDIS_PASSWORD
)


def get_result_cache_name(job_id: str) -> str:
    return f"result:{job_id}"


def add_task(queue_name: str, text: str) -> str:
    job_id = str(uuid.uuid4())
    job = {"job_id": job_id, "texts": text}
    r.rpush(queue_name, json.dumps(job))
    return job_id


@app.post("/high_priority", response_model=list[JobStatusResponse])
def add_task_to_high_priority(request: ProcessingRequest):
    jobs_response = []
    for data in request.data:
        job_id = add_task(queue_name=HIGH_PRIORITY_QUEUE_NAME, text=data.text)
        jobs_response.append({"job_id": job_id})
    return jobs_response


@app.post("/low_priority", response_model=list[JobStatusResponse])
def add_task_to_low_priority(request: ProcessingRequest):
    jobs_response = []
    for data in request.data:
        job_id = add_task(queue_name=LOW_PRIORITY_QUEUE_NAME, text=data.text)
        jobs_response.append({"job_id": job_id})
    return jobs_response


@app.get("/status/{job_id}", response_model=JobResultResponse)
def get_job_status(job_id: str):
    result_key = get_result_cache_name(job_id=job_id)
    result = r.get(result_key)
    if result:
        r.delete(result_key)
        return {"job_id": job_id, "result": json.loads(result)}
    else:
        raise HTTPException(status_code=404, detail="Job not found or still processing")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(FASTAPI_PORT))
