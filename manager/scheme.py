from pydantic import BaseModel


class AddJobRequestData(BaseModel):
    text: str


class AddJobRequest(BaseModel):
    data: list[AddJobRequestData]


class AddJobResponseData(BaseModel):
    job_id: str


class AddJobResponse(BaseModel):
    data: list[AddJobResponseData]


class GetResultResponseSuccessData(BaseModel):
    embedding: list[float]


class GetResultResponseProcessingData(BaseModel):
    n_wait: int


class GetResultResponse(BaseModel):
    data: GetResultResponseSuccessData | GetResultResponseProcessingData
