import os
import uuid
import pickle
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import redis

from scheme import AddJobRequest, AddJobResponse, GetResultResponse


load_dotenv()
REDIS_IP_ADDRESS = os.environ["REDIS_IP_ADDRESS"]
REDIS_PORT = os.environ["REDIS_PORT"]
REDIS_PASSWORD = os.environ["REDIS_PASSWORD"]

HIGH_PRIORITY_QUEUE_NAME = "high_priority_queue"
LOW_PRIORITY_QUEUE_NAME = "low_priority_queue"
PRE_PROGRESS_SET_NAME = "pre_progress_jobs"


def get_job_data_key(job_id: str) -> str:
    return f"job_data:{job_id}"


def get_result_data_key(job_id: str) -> str:
    return f"result_data:{job_id}"


r = redis.Redis(
    host=REDIS_IP_ADDRESS, port=int(REDIS_PORT), db=0, password=REDIS_PASSWORD
)


def add_pre_progress_to_redis(job_id: str) -> None:
    r.sadd(PRE_PROGRESS_SET_NAME, job_id)


def add_job_to_redis(queue_name: str, text: str) -> str:
    job_id = str(uuid.uuid4())

    job_data = {
        "text": text,
    }
    job_data_key = get_job_data_key(job_id=job_id)
    r.set(job_data_key, pickle.dumps(job_data))

    add_pre_progress_to_redis(job_id=job_id)
    
    r.rpush(queue_name, job_id)
    return job_id


def get_result_data_from_redis(job_id: str) -> None:
    result_data_key = get_result_data_key(job_id=job_id)
    raw_result_data: Any | None = r.get(result_data_key)

    if raw_result_data is None:
        return None

    r.delete(result_data_key)
    return pickle.loads(raw_result_data)


app = FastAPI()


def add_job_process(queue_name: str, request: AddJobRequest):
    response_data = []
    for request_data in request.data:
        job_id = add_job_to_redis(queue_name=queue_name, text=request_data.text)
        response_data.append({"job_id": job_id})
    return {"data": response_data}


def get_result_process(job_id: str):
    h_idx: int | None = r.lpos(HIGH_PRIORITY_QUEUE_NAME, job_id)
    if h_idx is not None:
        return JSONResponse(
            status_code=202,
            content={
                "data": {"n_wait": h_idx + 1},
            },
        )

    l_idx: int | None = r.lpos(LOW_PRIORITY_QUEUE_NAME, job_id)
    if l_idx is not None:
        h_length: int = r.llen(HIGH_PRIORITY_QUEUE_NAME)
        return JSONResponse(
            status_code=202,
            content={
                "data": {"n_wait": h_length + l_idx + 1},
            },
        )

    if r.sismember(PRE_PROGRESS_SET_NAME, job_id):
        return JSONResponse(
            status_code=202,
            content={
                "data": {"n_wait": 0},
            },
        )

    result = get_result_data_from_redis(job_id=job_id)
    if result:
        return {
            "data": {"embedding": result["embedding"]},
        }

    raise HTTPException(status_code=404, detail="Job not found")


@app.post("/add-job/high-priority", response_model=AddJobResponse)
def add_job_as_high_priority(request: AddJobRequest):
    return add_job_process(queue_name=HIGH_PRIORITY_QUEUE_NAME, request=request)


@app.post("/add-job/low-priority", response_model=AddJobResponse)
def add_job_as_low_priority(request: AddJobRequest):
    return add_job_process(queue_name=LOW_PRIORITY_QUEUE_NAME, request=request)


@app.get("/get-result/{job_id}", response_model=GetResultResponse)
def get_result(job_id: str):
    return get_result_process(job_id=job_id)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
