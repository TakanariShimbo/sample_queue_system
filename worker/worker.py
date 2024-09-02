import os
import pickle
import random
import time
from typing import Any

from dotenv import load_dotenv
import redis


load_dotenv()
REDIS_IP_ADDRESS = os.environ["REDIS_IP_ADDRESS"]
REDIS_PORT = os.environ["REDIS_PORT"]
REDIS_PASSWORD = os.environ["REDIS_PASSWORD"]

HIGH_PRIORITY_JOB_LIST_NAME = "high_priority_job_list"
LOW_PRIORITY_JOB_LIST_NAME = "low_priority_job_list"
PRE_PROCESS_JOB_SET_NAME = "pre_process_job_set"


def get_job_data_key(job_id: str) -> str:
    return f"job_data:{job_id}"


def get_result_data_key(job_id: str) -> str:
    return f"result_data:{job_id}"


r = redis.Redis(host=REDIS_IP_ADDRESS, port=int(REDIS_PORT), db=0, password=REDIS_PASSWORD)


def _word_to_vector(job_data: dict[str, Any]) -> Any:
    embedding = [random.uniform(0, 1) for _ in range(1024)]
    time.sleep(5)
    return embedding


def _get_job_from_redis(queue_list_name: str) -> tuple[str, dict[str, Any]] | None:
    raw_job_id: bytes | None = r.lpop(queue_list_name)

    if raw_job_id is None:
        return None

    job_id = raw_job_id.decode("utf-8")
    job_data_key = get_job_data_key(job_id=job_id)
    raw_job_data = r.get(job_data_key)
    job_data: dict[str, Any] = pickle.loads(raw_job_data)

    return job_id, job_data


def add_result_data_to_redis(job_id: str, result_data: dict[str, Any]) -> None:
    result_data_key = get_result_data_key(job_id=job_id)
    r.set(result_data_key, pickle.dumps(result_data))


def delete_job_data_from_redis(job_id: str) -> None:
    job_data_key = get_job_data_key(job_id=job_id)
    r.delete(job_data_key)


def delete_pre_process_job_from_redis(job_id: str) -> None:
    r.srem(PRE_PROCESS_JOB_SET_NAME, job_id)


def search_job_from_redis() -> tuple[str, dict[str, Any]] | None:
    for queue_list_name in [HIGH_PRIORITY_JOB_LIST_NAME, LOW_PRIORITY_JOB_LIST_NAME]:
        job = _get_job_from_redis(queue_list_name=queue_list_name)
        if job:
            return job
    return None


def process_job(job_id: str, job_data: dict[str, Any]) -> None:
    embedding = _word_to_vector(job_data=job_data)
    print(f"Processing completed: {job_id}")

    result_data = {
        "embedding": embedding,
    }

    add_result_data_to_redis(job_id=job_id, result_data=result_data)

    delete_job_data_from_redis(job_id=job_id)
    delete_pre_process_job_from_redis(job_id=job_id)

    return result_data


if __name__ == "__main__":
    while True:
        job = search_job_from_redis()
        if job is None:
            continue

        job_id, job_data = job
        process_job(job_id=job_id, job_data=job_data)

        time.sleep(1)
