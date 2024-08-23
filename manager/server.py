import os
import uuid
import pickle

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
import redis

from scheme import AddJobRequest, AddJobResponse, GetResultResponse


load_dotenv()
REDIS_IP_ADDRESS = os.environ["REDIS_IP_ADDRESS"]
REDIS_PORT = os.environ["REDIS_PORT"]
REDIS_PASSWORD = os.environ["REDIS_PASSWORD"]
FASTAPI_PORT = os.environ["FASTAPI_PORT"]

HIGH_PRIORITY_QUEUE_NAME = "high_priority_queue"
LOW_PRIORITY_QUEUE_NAME = "low_priority_queue"

app = FastAPI()


r = redis.Redis(host=REDIS_IP_ADDRESS, port=int(REDIS_PORT), db=0, password=REDIS_PASSWORD)


def get_result_key(job_id: str) -> str:
    return f"result:{job_id}"


def add_job(queue_name: str, text: str) -> str:
    job_id = str(uuid.uuid4())
    job = {"job_id": job_id, "texts": text}
    r.rpush(queue_name, pickle.dumps(job))
    return job_id


@app.post("/add-job/high-priority", response_model=AddJobResponse)
def add_job_as_high_priority(request: AddJobRequest):
    response_data = []
    for request_data in request.data:
        job_id = add_job(queue_name=HIGH_PRIORITY_QUEUE_NAME, text=request_data.text)
        response_data.append({"job_id": job_id})
    return {"data": response_data}


@app.post("/add-job/low-priority", response_model=AddJobResponse)
def add_job_as_low_priority(request: AddJobRequest):
    response_data = []
    for request_data in request.data:
        job_id = add_job(queue_name=LOW_PRIORITY_QUEUE_NAME, text=request_data.text)
        response_data.append({"job_id": job_id})
    return {"data": response_data}


@app.get("/get-result/{job_id}", response_model=GetResultResponse)
def get_job_status(job_id: str):
    result_key = get_result_key(job_id=job_id)
    result = r.get(result_key)
    if result:
        r.delete(result_key)
        return {"job_id": job_id, "result": pickle.loads(result)}
    else:
        raise HTTPException(status_code=404, detail="Job not found or still processing")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(FASTAPI_PORT))
