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

HIGH_PRIORITY_QUEUE_NAME = "high_priority_queue"
LOW_PRIORITY_QUEUE_NAME = "low_priority_queue"
IN_PROGRESS_SET_NAME = "in_progress_jobs"


def get_job_data_key(job_id: str) -> str:
    return f"job_data:{job_id}"


def get_result_data_key(job_id: str) -> str:
    return f"result_data:{job_id}"


r = redis.Redis(host=REDIS_IP_ADDRESS, port=int(REDIS_PORT), db=0, password=REDIS_PASSWORD)


def _word_to_vector(job_data: dict[str, Any]) -> Any:
    embedding = [random.uniform(0, 1) for _ in range(1024)]
    time.sleep(5)
    return embedding


def process_job(job_id: str, job_data: dict[str, Any]) -> dict[str, Any]:
    embedding = _word_to_vector(job_data=job_data)
    print(f"Processing completed: {job_id}")

    result_data = {
        "embedding": embedding,
    }
    return result_data


def _get_job_from_redis(queue_name: str) -> tuple[str, dict[str, Any]] | None:
    raw_job_id: bytes | None = r.lpop(queue_name)

    if raw_job_id is None:
        return None

    job_id = raw_job_id.decode("utf-8")
    job_data_key = get_job_data_key(job_id=job_id)
    raw_job_data = r.get(job_data_key)
    job_data: dict[str, Any] = pickle.loads(raw_job_data)

    return job_id, job_data


def add_result_to_redis(job_id: str, result_data: dict[str, Any]) -> None:
    result_data_key = get_result_data_key(job_id=job_id)
    r.set(result_data_key, pickle.dumps(result_data))


def delete_job_from_redis(job_id: str) -> None:
    job_data_key = get_job_data_key(job_id=job_id)
    r.delete(job_data_key)


def search_job_from_redis() -> tuple[str, dict[str, Any]] | None:
    for queue_name in [HIGH_PRIORITY_QUEUE_NAME, LOW_PRIORITY_QUEUE_NAME]:
        job = _get_job_from_redis(queue_name=queue_name)
        if job:
            return job
    return None


def add_in_progress_to_redis(job_id: str) -> None:
    r.sadd(IN_PROGRESS_SET_NAME, job_id)


def delete_in_progress_from_redis(job_id: str) -> None:
    r.srem(IN_PROGRESS_SET_NAME, job_id)


if __name__ == "__main__":
    while True:
        job = search_job_from_redis()
        if job is None:
            continue

        job_id, job_data = job

        add_in_progress_to_redis(job_id)

        result_data = process_job(job_id=job_id, job_data=job_data)

        add_result_to_redis(job_id=job_id, result_data=result_data)
        delete_job_from_redis(job_id=job_id)

        delete_in_progress_from_redis(job_id)

        time.sleep(1)
