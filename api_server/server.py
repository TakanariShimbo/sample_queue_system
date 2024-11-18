import os
import uuid
import pickle
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import redis

from scheme import AddJobRequest, AddJobResponse, GetResultResponse, AddJobRequestData


# load environment variables
CACHE_SERVER_ADDRESS = os.environ["CACHE_SERVER_ADDRESS"]
CACHE_SERVER_PORT = os.environ["CACHE_SERVER_PORT"]
CACHE_SERVER_PASSWORD = os.environ["CACHE_SERVER_PASSWORD"]


# redis keys
HIGH_PRIORITY_JOB_LIST_NAME = "high_priority_job_list"
LOW_PRIORITY_JOB_LIST_NAME = "low_priority_job_list"
PRE_PROCESS_JOB_SET_NAME = "pre_process_job_set"


def get_job_data_key(job_id: str) -> str:
    return f"job_data:{job_id}"


def get_result_data_key(job_id: str) -> str:
    return f"result_data:{job_id}"


r = redis.Redis(host=CACHE_SERVER_ADDRESS, port=int(CACHE_SERVER_PORT), db=0, password=CACHE_SERVER_PASSWORD)


def add_job_data_to_pool(job_id: str, job_data_dict: dict[str, Any]) -> None:
    job_data_key = get_job_data_key(job_id=job_id)
    r.set(job_data_key, pickle.dumps(job_data_dict))


def add_job_id_to_pre_process_job_set(job_id: str) -> None:
    r.sadd(PRE_PROCESS_JOB_SET_NAME, job_id)


def add_job_id_to_job_list(job_list_name: str, job_id: str) -> None:
    r.rpush(job_list_name, job_id)


def get_result_data_from_redis(job_id: str) -> dict[str, Any] | None:
    result_data_key = get_result_data_key(job_id=job_id)
    raw_result_data: Any | None = r.get(result_data_key)

    if raw_result_data is None:
        return None

    return pickle.loads(raw_result_data)


def remove_result_data_from_pool(job_id: str) -> None:
    result_data_key = get_result_data_key(job_id=job_id)
    r.delete(result_data_key)


def check_n_wait(job_id: str) -> int:
    if not r.sismember(PRE_PROCESS_JOB_SET_NAME, job_id):
        # already processed or not found
        return -1

    high_priority_idx: int | None = r.lpos(HIGH_PRIORITY_JOB_LIST_NAME, job_id)
    if high_priority_idx is not None:
        # exist at high priority job list
        return high_priority_idx + 1

    low_priority_idx: int | None = r.lpos(LOW_PRIORITY_JOB_LIST_NAME, job_id)
    if low_priority_idx is not None:
        # exist at low priority job list
        high_priority_length: int = r.llen(HIGH_PRIORITY_JOB_LIST_NAME)
        return high_priority_length + low_priority_idx + 1

    # processing now
    return 0


def add_job_to_redis(job_list_name: str, job_data: AddJobRequestData) -> str:
    job_id = str(uuid.uuid4())

    job_data_dict = job_data.model_dump()
    add_job_data_to_pool(job_id=job_id, job_data_dict=job_data_dict)

    add_job_id_to_pre_process_job_set(job_id=job_id)

    add_job_id_to_job_list(job_list_name=job_list_name, job_id=job_id)
    return job_id


app = FastAPI()


def add_job_process(job_list_name: str, request: AddJobRequest):
    response_data = []
    for job_data in request.data:
        job_id = add_job_to_redis(job_list_name=job_list_name, job_data=job_data)
        response_data.append({"job_id": job_id})
    return {"data": response_data}


def get_result_process(job_id: str):
    n_wait = check_n_wait(job_id=job_id)
    if n_wait >= 0:
        return JSONResponse(
            status_code=202,
            content={
                "data": {"n_wait": n_wait},
            },
        )

    result = get_result_data_from_redis(job_id=job_id)
    if result:
        remove_result_data_from_pool(job_id=job_id)

        return {
            "data": {"embedding": result["embedding"]},
        }

    raise HTTPException(status_code=404, detail="Job not found")


@app.post("/add-job/high-priority", response_model=AddJobResponse)
def add_job_as_high_priority(request: AddJobRequest):
    return add_job_process(job_list_name=HIGH_PRIORITY_JOB_LIST_NAME, request=request)


@app.post("/add-job/low-priority", response_model=AddJobResponse)
def add_job_as_low_priority(request: AddJobRequest):
    return add_job_process(job_list_name=LOW_PRIORITY_JOB_LIST_NAME, request=request)


@app.get("/get-result/{job_id}", response_model=GetResultResponse)
def get_result(job_id: str):
    return get_result_process(job_id=job_id)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
