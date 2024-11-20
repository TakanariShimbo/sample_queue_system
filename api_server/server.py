from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from scheme import AddJobRequest, AddJobResponse, GetResultResponse
from cache_clinet import CacheClient


cache_client = CacheClient()
app = FastAPI()


def add_job_process(priority: Literal["high", "low"], request: AddJobRequest) -> dict[str, Any]:
    response_data = []
    for job_data in request.data:
        job_data_dict = job_data.model_dump()

        if priority == "high":
            job_id = cache_client.add_high_priority_job(job_data_dict=job_data_dict)
        elif priority == "low":
            job_id = cache_client.add_low_priority_job(job_data_dict=job_data_dict)

        response_data.append({"job_id": job_id})
    return {"data": response_data}


def get_result_process(job_id: str) -> JSONResponse | dict[str, Any]:
    n_wait = cache_client.check_n_wait(job_id=job_id)
    if n_wait >= 0:
        return JSONResponse(
            status_code=202,
            content={
                "data": {"n_wait": n_wait},
            },
        )

    result = cache_client.get_result_data(job_id=job_id)
    if result:
        return {
            "data": {"embedding": result["embedding"]},
        }

    raise HTTPException(status_code=404, detail="Job not found")


@app.post(path="/add-job/high-priority", response_model=AddJobResponse)
def add_job_as_high_priority(request: AddJobRequest):
    return add_job_process(priority="high", request=request)


@app.post(path="/add-job/low-priority", response_model=AddJobResponse)
def add_job_as_low_priority(request: AddJobRequest):
    return add_job_process(priority="low", request=request)


@app.get(path="/get-result/{job_id}", response_model=GetResultResponse)
def get_result(job_id: str):
    return get_result_process(job_id=job_id)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app=app, host="0.0.0.0", port=8000)
