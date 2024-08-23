from pydantic import BaseModel


class AddJobRequestData(BaseModel):
    text: str


class AddJobRequest(BaseModel):
    data: list[AddJobRequestData]


class AddJobResponseData(BaseModel):
    job_id: str


class AddJobResponse(BaseModel):
    data: list[AddJobResponseData]


class GetResultResponse(BaseModel):
    result: list[float]
