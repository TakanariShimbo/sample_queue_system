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

r = redis.Redis(host=REDIS_IP_ADDRESS, port=int(REDIS_PORT), db=0, password=REDIS_PASSWORD)


def get_result_key(job_id: str) -> str:
    return f"result:{job_id}"


def estimate_result(job_data: Any) -> Any:
    result_key = get_result_key(job_id=job_data["job_id"])
    time.sleep(5)
    result = [random.uniform(0, 1) for _ in range(1024)]
    return result_key, result


def get_job() -> Any | None:
    high_priority_job = r.lpop(HIGH_PRIORITY_QUEUE_NAME)
    if high_priority_job:
        return high_priority_job

    low_priority_job = r.lpop(LOW_PRIORITY_QUEUE_NAME)
    if low_priority_job:
        return low_priority_job

    return None


def process_job(job: Any) -> None:
    job_data = pickle.loads(job)
    result_key, result = estimate_result(job_data=job_data)
    r.set(result_key, pickle.dumps(result))
    print(f"Processing completed: {job_data['job_id']}")


if __name__ == "__main__":
    while True:
        job = get_job()
        if job is None:
            continue

        process_job(job=job)
        time.sleep(0.1)
