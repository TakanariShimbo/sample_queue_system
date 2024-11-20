import random
import time
from typing import Any

from cache_client import CacheClient


def _embedding_process(job_data: dict[str, Any]) -> dict[str, Any]:
    embedding = [random.uniform(0, 1) for _ in range(5)]

    result_data = {
        "embedding": embedding,
    }

    time.sleep(5)
    return result_data


def process_job(job_id: str, job_data: dict[str, Any]) -> dict[str, Any]:
    result_data = _embedding_process(job_data=job_data)
    print(f"Processing completed: {job_id}")

    return result_data


if __name__ == "__main__":
    cache_client = CacheClient()

    while True:
        job = cache_client.search_job()
        if job is None:
            continue

        job_id, job_data = job
        result_data = process_job(job_id=job_id, job_data=job_data)

        cache_client.post_process_job(job_id=job_id, job_data=job_data, result_data=result_data)

        time.sleep(1)
