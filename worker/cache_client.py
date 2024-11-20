import os
import pickle
from typing import Any

import redis


CACHE_SERVER_ADDRESS = os.environ["CACHE_SERVER_ADDRESS"]
CACHE_SERVER_PORT = os.environ["CACHE_SERVER_PORT"]
CACHE_SERVER_PASSWORD = os.environ["CACHE_SERVER_PASSWORD"]


HIGH_PRIORITY_JOB_LIST_NAME = "high_priority_job_list"
LOW_PRIORITY_JOB_LIST_NAME = "low_priority_job_list"
PRE_PROCESS_JOB_SET_NAME = "pre_process_job_set"


def get_job_data_key(job_id: str) -> str:
    return f"job_data:{job_id}"


def get_result_data_key(job_id: str) -> str:
    return f"result_data:{job_id}"


class CacheClient:
    def __init__(self) -> None:
        self._client = redis.Redis(host=CACHE_SERVER_ADDRESS, port=int(CACHE_SERVER_PORT), db=0, password=CACHE_SERVER_PASSWORD)

    def _get_job_id_from_job_list(self, job_list_name: str) -> str | None:
        raw_job_id: bytes | None = self._client.lpop(job_list_name)
        if raw_job_id is None:
            return None
        job_id = raw_job_id.decode(encoding="utf-8")
        return job_id

    def _get_job_data_from_pool(self, job_id: str) -> dict[str, Any]:
        job_data_key = get_job_data_key(job_id=job_id)
        raw_job_data = self._client.get(job_data_key)
        job_data: dict[str, Any] = pickle.loads(raw_job_data)
        return job_data

    def _get_job(self, job_list_name: str) -> tuple[str, dict[str, Any]] | None:
        job_id = self._get_job_id_from_job_list(job_list_name=job_list_name)
        if job_id is None:
            return None

        job_data = self._get_job_data_from_pool(job_id=job_id)
        return job_id, job_data

    def _add_result_data_to_pool(self, job_id: str, result_data: dict[str, Any], expiration_sec: int) -> None:
        result_data_key = get_result_data_key(job_id=job_id)
        self._client.set(result_data_key, pickle.dumps(result_data), ex=expiration_sec)

    def _delete_job_data_from_pool(self, job_id: str) -> None:
        job_data_key = get_job_data_key(job_id=job_id)
        self._client.delete(job_data_key)

    def _delete_job_id_from_pre_process_job_set(self, job_id: str) -> None:
        self._client.srem(PRE_PROCESS_JOB_SET_NAME, job_id)

    def search_job(self) -> tuple[str, dict[str, Any]] | None:
        for job_list_name in [HIGH_PRIORITY_JOB_LIST_NAME, LOW_PRIORITY_JOB_LIST_NAME]:
            job = self._get_job(job_list_name=job_list_name)
            if job:
                job_id, job_data = job
                return job_id, job_data
        return None

    def post_process_job(self, job_id: str, job_data: dict[str, Any], result_data: dict[str, Any]) -> None:
        expiration_sec: int = job_data["expiration_sec"]
        self._add_result_data_to_pool(job_id=job_id, result_data=result_data, expiration_sec=expiration_sec)
        self._delete_job_data_from_pool(job_id=job_id)
        self._delete_job_id_from_pre_process_job_set(job_id=job_id)
