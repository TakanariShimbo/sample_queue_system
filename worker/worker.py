import time
import os
from typing import Awaitable, Any
import json
import random

import redis
from dotenv import load_dotenv


load_dotenv()
REDIS_IP_ADDRESS = os.getenv("REDIS_IP_ADDRESS")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

HIGH_PRIORITY_QUEUE_NAME = "high_priority_queue"
LOW_PRIORITY_QUEUE_NAME = "low_priority_queue"

r = redis.Redis(
    host=REDIS_IP_ADDRESS, port=int(REDIS_PORT), db=0, password=REDIS_PASSWORD
)


def get_result_cache_name(job_id: str) -> str:
    return f"result:{job_id}"


def get_job():
    high_priority_job = r.lpop(HIGH_PRIORITY_QUEUE_NAME)
    if high_priority_job:
        return high_priority_job

    low_priority_job = r.lpop(LOW_PRIORITY_QUEUE_NAME)
    if low_priority_job:
        return low_priority_job

    return None


def process_job(job: Awaitable) -> dict[str, Any]:
    job_data = json.loads(job.decode("utf-8"))
    result = [random.uniform(0, 1) for _ in range(1024)]
    time.sleep(5)
    result_key = get_result_cache_name(job_id=job_data["job_id"])
    r.set(result_key, json.dumps(result))
    print(f"Processing completed: {job_data}")
    return {"job_id": job_data["job_id"], "result": result}


if __name__ == "__main__":
    while True:
        job = get_job()
        if not job:
            continue

        process_job(job=job)
        time.sleep(0.1)
