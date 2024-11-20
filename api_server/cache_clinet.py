import os
import uuid
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

    def _add_job_data_to_pool(self, job_id: str, job_data_dict: dict[str, Any]) -> None:
        job_data_key = get_job_data_key(job_id=job_id)
        self._client.set(job_data_key, pickle.dumps(job_data_dict))

    def _add_job_id_to_pre_process_job_set(self, job_id: str) -> None:
        self._client.sadd(PRE_PROCESS_JOB_SET_NAME, job_id)

    def _add_job_id_to_job_list(self, job_list_name: str, job_id: str) -> None:
        self._client.rpush(job_list_name, job_id)

    def _get_result_data_from_pool(self, job_id: str) -> dict[str, Any] | None:
        result_data_key = get_result_data_key(job_id=job_id)
        raw_result_data: Any | None = self._client.get(result_data_key)

        if raw_result_data is None:
            return None

        return pickle.loads(raw_result_data)

    def _remove_result_data_from_pool(self, job_id: str) -> None:
        result_data_key = get_result_data_key(job_id=job_id)
        self._client.delete(result_data_key)

    def _check_idx_in_job_list(self, job_list_name: str, job_id: str) -> int | None:
        idx: int | None = self._client.lpos(job_list_name, job_id)
        if idx is None:
            return None
        return idx

    def _check_length_of_job_list(self, job_list_name: str) -> int:
        length: int | None = self._client.llen(job_list_name)
        if length is None:
            return 0
        return length

    def _add_job(self, job_list_name: str, job_data_dict: dict[str, Any]) -> str:
        job_id = str(uuid.uuid4())

        self._add_job_data_to_pool(job_id=job_id, job_data_dict=job_data_dict)

        self._add_job_id_to_pre_process_job_set(job_id=job_id)

        self._add_job_id_to_job_list(job_list_name=job_list_name, job_id=job_id)
        return job_id

    def add_high_priority_job(self, job_data_dict: dict[str, Any]) -> str:
        return self._add_job(job_list_name=HIGH_PRIORITY_JOB_LIST_NAME, job_data_dict=job_data_dict)

    def add_low_priority_job(self, job_data_dict: dict[str, Any]) -> str:
        return self._add_job(job_list_name=LOW_PRIORITY_JOB_LIST_NAME, job_data_dict=job_data_dict)

    def check_n_wait(self, job_id: str) -> int:
        if not self._client.sismember(PRE_PROCESS_JOB_SET_NAME, job_id):
            # already processed or not found
            return -1

        high_priority_idx = self._check_idx_in_job_list(job_list_name=HIGH_PRIORITY_JOB_LIST_NAME, job_id=job_id)
        if high_priority_idx is not None:
            # exist at high priority job list
            return high_priority_idx + 1

        low_priority_idx: int | None = self._check_idx_in_job_list(job_list_name=LOW_PRIORITY_JOB_LIST_NAME, job_id=job_id)
        if low_priority_idx is not None:
            # exist at low priority job list
            high_priority_length = self._check_length_of_job_list(job_list_name=HIGH_PRIORITY_JOB_LIST_NAME)
            return high_priority_length + low_priority_idx + 1

        # processing now
        return 0

    def get_result_data(self, job_id: str) -> dict[str, Any] | None:
        result_data = self._get_result_data_from_pool(job_id=job_id)
        if result_data is None:
            return None

        self._remove_result_data_from_pool(job_id=job_id)
        return result_data
