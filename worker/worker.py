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


def get_result_key(job_id: str) -> str:
    return f"result:{job_id}"


r = redis.Redis(host=REDIS_IP_ADDRESS, port=int(REDIS_PORT), db=0, password=REDIS_PASSWORD)


def estimate_result(job_data: dict[str, Any]) -> tuple[str, Any]:
    result = [random.uniform(0, 1) for _ in range(1024)]
    time.sleep(5)
    return job_data["job_id"], result


def get_job_from_redis(queue_name: str) -> dict[str, Any] | None:
    raw_job_data: Any | None = r.lpop(queue_name)
    if raw_job_data is None:
        return None
    job_data = pickle.loads(raw_job_data)
    return job_data


def add_result_to_redis(job_id: str, result: Any) -> None:
    result_key = get_result_key(job_id=job_id)
    r.set(result_key, pickle.dumps(result))


def search_job_from_redis() -> dict[str, Any] | None:
    for queue_name in [HIGH_PRIORITY_QUEUE_NAME, LOW_PRIORITY_QUEUE_NAME]:
        job_data = get_job_from_redis(queue_name=queue_name)
        if job_data:
            return job_data
    return None


if __name__ == "__main__":
    while True:
        job_data = search_job_from_redis()
        if job_data is None:
            continue

        job_id, result = estimate_result(job_data=job_data)
        print(f"Processing completed: {job_id}")

        add_result_to_redis(job_id=job_id, result=result)

        time.sleep(0.1)
